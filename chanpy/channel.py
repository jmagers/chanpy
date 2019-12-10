import asyncio
import contextlib
import random
import threading
from collections import deque
from . import handlers as hd
from . import xf


@contextlib.contextmanager
def acquire_handlers(*handlers):
    # Consistent lock acquisition order
    for h in sorted(handlers, key=lambda h: h.lock_id):
        is_acquired = h.acquire()
        assert is_acquired

    try:
        yield True
    finally:
        for h in handlers:
            h.release()


MAX_QUEUE_SIZE = 1024


class MaxQueueSize(Exception):
    """Maximum pending operations exceeded"""


def nop_ex_handler(e):
    raise e


class Chan:
    def __init__(self, buf=None, xform=None, ex_handler=None):
        xform = xf.identity if xform is None else xform
        ex_handler = nop_ex_handler if ex_handler is None else ex_handler
        self._buf = buf
        self._takes = deque()
        self._puts = deque()
        self._is_closed = False
        self._xform_is_completed = False
        self._lock = threading.Lock()

        @xform
        @xf.completing
        def xrf(_, val):
            if val is None:
                raise TypeError('xform cannot produce None')
            self._buf.put(val)

        def ex_handler_rf(*args):
            try:
                return xrf(*args)
            except Exception as e:
                val = ex_handler(e)
                if val is not None:
                    self._buf.put(val)

        self._buf_rf = ex_handler_rf

    def put(self, val, *, wait=True):
        flag = hd.create_flag()
        future = hd.FlagFuture(flag)
        handler = hd.FlagHandler(flag, hd.future_deliver_fn(future), wait)
        ret = self._p_put(handler, val)
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
        return future

    def get(self, *, wait=True):
        flag = hd.create_flag()
        future = hd.FlagFuture(flag)
        handler = hd.FlagHandler(flag, hd.future_deliver_fn(future), wait)
        ret = self._p_get(handler)
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
        return future

    def b_put(self, val, *, wait=True):
        prom = hd.Promise()
        ret = self._p_put(hd.FnHandler(prom.deliver, wait), val)
        if ret is not None:
            return ret[0]
        return prom.deref()

    def b_get(self, *, wait=True):
        prom = hd.Promise()
        ret = self._p_get(hd.FnHandler(prom.deliver, wait))
        if ret is not None:
            return ret[0]
        return prom.deref()

    def f_put(self, val, f=lambda _: None):
        """Asynchronously puts val onto channel and calls f when complete.

        Args:
            val: A value to put onto channel.
            f: An optional non-blocking function accepting a bool. Will be
                passed False if channel is already closed or True if not.

        Returns: False if channel is already closed or True if not.
        """
        ret = self._p_put(hd.FnHandler(f), val)
        if ret is None:
            return True
        f(ret[0])
        return ret[0]

    def f_get(self, f):
        """Asynchronously gets a value from channel and calls f with it.

        Args:
            f: A non-blocking function accepting a single argument. Will be
                passed the value taken from the channel or None if channel is
                exhausted.
        """
        ret = self._p_get(hd.FnHandler(f))
        if ret is None:
            return
        f(ret[0])

    def offer(self, val):
        return self.b_put(val, wait=False)

    def poll(self):
        return self.b_get(wait=False)

    def close(self):
        with self._lock:
            self._cleanup()
            self._close()

    def _p_put(self, handler, val):
        if val is None:
            raise TypeError('item cannot be None')
        with self._lock:
            self._cleanup()

            if self._is_closed:
                return self._fail_op(handler, False)

            # Attempt to transfer val onto buf
            if self._buf is not None and not self._buf.is_full():
                with handler:
                    if not handler.is_active:
                        return False
                    handler.commit()

                self._buf_put(val)
                self._distribute_buf_vals()
                return True,

            # Attempt to transfer val to a taker
            if self._buf is None:
                while len(self._takes) > 0:
                    taker = self._takes.popleft()
                    with acquire_handlers(handler, taker):
                        if handler.is_active and taker.is_active:
                            handler.commit()
                            taker.commit()(val)
                            return True,

            if not handler.is_blockable:
                return self._fail_op(handler, False)

            # Enqueue
            if len(self._puts) >= MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._puts.append((handler, val))

    def _p_get(self, handler):
        with self._lock:
            self._cleanup()

            # Attempt to take val from buf
            if self._buf is not None and len(self._buf) > 0:
                with handler:
                    if not handler.is_active:
                        return None,
                    handler.commit()

                ret = self._buf.get()

                # Transfer vals from putters onto buf
                while len(self._puts) > 0 and not self._buf.is_full():
                    putter, val = self._puts.popleft()
                    with putter:
                        if putter.is_active:
                            putter.commit()(True)
                            self._buf_put(val)

                self._complete_xform_if_ready()
                return ret,

            # Attempt to take val from a putter
            if self._buf is None:
                while len(self._puts) > 0:
                    putter, val = self._puts.popleft()
                    with acquire_handlers(handler, putter):
                        if handler.is_active and putter.is_active:
                            handler.commit()
                            putter.commit()(True)
                            return val,

            if self._is_closed or not handler.is_blockable:
                return self._fail_op(handler, None)

            # Enqueue
            if len(self._takes) >= MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._takes.append(handler)

    def _cleanup(self):
        self._takes = deque(h for h in self._takes if h.is_active)
        self._puts = deque((h, v) for h, v in self._puts if h.is_active)

    @staticmethod
    def _fail_op(handler, val):
        with handler:
            if handler.is_active:
                handler.commit()
                return val,

    def _buf_put(self, val):
        if xf.is_reduced(self._buf_rf(None, val)):
            # If reduced value is returned then no more input is allowed onto
            # buf. To ensure this, remove all pending puts and close ch.
            for putter, _ in self._puts:
                with putter:
                    if putter.is_active:
                        putter.commit()(False)
            self._puts.clear()
            self._close()

    def _distribute_buf_vals(self):
        while len(self._takes) > 0 and len(self._buf) > 0:
            taker = self._takes.popleft()
            with taker:
                if taker.is_active:
                    taker.commit()(self._buf.get())

    def _complete_xform_if_ready(self):
        """Calls the xform completion arity exactly once iff all input has been
        placed onto buf"""
        if (self._is_closed and
                len(self._puts) == 0 and
                not self._xform_is_completed):
            self._xform_is_completed = True
            self._buf_rf(None)

    def _close(self):
        self._is_closed = True

        if self._buf is not None:
            self._complete_xform_if_ready()
            self._distribute_buf_vals()

        # Remove pending takes
        # No-op if there are pending puts or buffer isn't empty
        for taker in self._takes:
            with taker:
                if taker.is_active:
                    taker.commit()(None)
        self._takes.clear()

    async def __aiter__(self):
        while True:
            value = await self.get()
            if value is None:
                break
            yield value


class _Undefined:
    """A default parameter value that a user could never pass in."""


def _alts(flag, deliver_fn, ports, priority, default):
    ports = list(ports)
    if len(ports) == 0:
        raise ValueError('alts must have at least one channel operation')
    if not priority:
        random.shuffle(ports)

    ops = {}

    # Parse ports into ops
    for p in ports:
        try:
            ch, val = p
            op = {'type': 'put', 'value': val}
        except TypeError:
            ch = p
            op = {'type': 'get'}
        if ops.get(ch, op)['type'] != op['type']:
            raise ValueError('cannot get and put to same channel')
        ops[ch] = op

    def create_handler(ch):
        return hd.FlagHandler(flag, lambda val: deliver_fn((val, ch)))

    # Start ops
    for ch, op in ops.items():
        if op['type'] == 'get':
            ret = ch._p_get(create_handler(ch))
        elif op['type'] == 'put':
            ret = ch._p_put(create_handler(ch), op['value'])
        if ret is not None:
            return ret[0], ch

    if default is not _Undefined:
        with flag['lock']:
            if flag['is_active']:
                flag['is_active'] = False
                return default, 'default'


def alts(ports, *, priority=False, default=_Undefined):
    """Returns an awaitable representing the first ports operation to complete.

    If no default is provided then only the first ports operation to complete
    will be committed. If default is provided and none of the ports operations
    complete immediately then none of the ports operations will be committed
    and default will be used to complete the returned awaitable instead.

    Args:
        ports: An iterable of get and put operations to attempt.
            A get operation is represented as simply the channel to get from.
            A put operations is represented as an iterable of the form
            [channel, val] where val is the item to put onto the channel.
        priority: An optional bool. If true, operations will be tried in order.
            If false, operations will be tried in random order.
        default: An optional value to use in case none of the operations in
            ports complete immediately.

    Returns: An awaitable that evaluates to a tuple of the form (val, ch).
        If default is not provided then val will be what the first successful
        ports operation returned and ch will be the channel used in that
        operation. If default is provided and none of the ports operations
        complete immediately then the awaitable will evaluate to
        (default, 'default').

    Raises:
        ValueError: If ports is empty or contains both a get and put operation
            to the same channel.
    """
    flag = hd.create_flag()
    future = hd.FlagFuture(flag)
    ret = _alts(flag, hd.future_deliver_fn(future), ports,
                priority, default)
    if ret is not None:
        asyncio.Future.set_result(future, ret)
    return future


def b_alts(ports, *, priority=False, default=_Undefined):
    """Same as alts() except it blocks instead of returning an awaitable."""
    prom = hd.Promise()
    ret = _alts(hd.create_flag(), prom.deliver, ports, priority, default)
    return prom.deref() if ret is None else ret


def alt(*ports, priority=False, default=_Undefined):
    """A variadic version of alts()."""
    return alts(ports, priority=priority, default=default)


def b_alt(*ports, priority=False, default=_Undefined):
    """A variadic version of b_alts()."""
    return b_alts(ports, priority=priority, default=default)
