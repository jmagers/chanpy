import asyncio
import contextlib
import random
import threading
from collections import deque
from numbers import Number
from . import buffers as bufs
from . import handlers as hd
from . import xf


MAX_QUEUE_SIZE = 1024


class QueueSizeExceeded(Exception):
    """Maximum pending channel operations exceeded.

    Raised when too many channel operations have been enqueued on the channel.
    Consider using a windowing buffer to prevent enqueuing too many puts or
    altering your design to have less asynchronous processes access the channel
    at once.
    """


@contextlib.contextmanager
def acquire_handlers(*handlers):
    """Returns a context manager for acquiring handlers without deadlock."""

    # Acquire locks in consistent order
    for h in sorted(handlers, key=lambda h: h.lock_id):
        is_acquired = h.acquire()
        assert is_acquired

    try:
        yield True
    finally:
        for h in handlers:
            h.release()


def nop_ex_handler(e):
    raise e


class chan:
    """A CSP channel with optional buffer, transducer, and exception handler.

    Channels support multiple producers and consumers and may be buffered or
    unbuffered. Additionally, buffered channels can optionally have a
    transformation applied to the values put to them through the use of a
    transducer.

    Channels may be used by threads with or without a running asyncio event
    loop. The get and put methods provide direct support for asyncio by
    returning awaitables whereas b_get and b_put provide blocking alternatives.
    Producers and consumers need not be of the same type. For example, a value
    placed onto the channel with put can be taken by a call to b_get from a
    separate thread.

    A select/alt feature is also available via the alt and b_alt functions.
    See alts() for more information.

    Once closed, future puts will be unsuccessful but any pending puts will
    remain until consumed or until a reduced value is returned from the
    transformation. Once exhausted, all future gets will complete with
    the value None. Because of this, None cannot be put onto a channel either
    directly or indirectly through a transformation.

    Args:
        buf_or_n: An optional buffer that may be expressed as a positive number.
            If it's an int, a fixed buffer of that capacity will be used.
            If None, the channel will be unbuffered.
        xform: An optional transducer for transforming elements put onto the
            channel. buf_or_n must not be None if transducer is provided.
        ex_handler: An optional function to handle exceptions raised during
            transformation. Must accept the raised exception as a parameter.
            Any non-None return value will be put onto the buffer.

    Raises:
        TypeError: If xform or ex_handler is provided without a buffer.

    """
    def __init__(self, buf_or_n=None, xform=None, ex_handler=None):
        if buf_or_n is None:
            if xform is not None:
                raise TypeError('unbuffered channels cannot have an xform')
            if ex_handler is not None:
                raise TypeError('unbuffered channels cannot have an ex_handler')

        self._buf = (bufs.FixedBuffer(buf_or_n)
                     if isinstance(buf_or_n, Number)
                     else buf_or_n)
        xform = xf.identity if xform is None else xform
        ex_handler = nop_ex_handler if ex_handler is None else ex_handler
        self._takes = deque()
        self._puts = deque()
        self._is_closed = False
        self._buf_rf_is_completed = False
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
        """Attempts to put val onto the channel.

        Puts will fail in the following cases:

        - channel is already closed
        - wait is false and val cannot be immediately put onto channel
        - a reduced value is returned during transformation before the
          operation completes

        Args:
            val: A non-None value to put onto the channel.
            wait: An optional bool that if false, fails the put operation when
                it cannot complete immediately.

        Returns: An awaitable that will evaluate to True if val is accepted
            onto the channel or False if it is not.

        Raises:
            TypeError: If val is None.
            RuntimeError: If the calling thread has no running event loop.
            QueueSizeExceeded: If the channel has too many pending put
                operations.
        """
        flag = hd.create_flag()
        future = hd.FlagFuture(flag)
        handler = hd.FlagHandler(flag, hd.future_deliver_fn(future), wait)
        ret = self._p_put(handler, val)
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
        return future

    def get(self, *, wait=True):
        """Attempts to take a value from the channel.

        Gets will fail if the channel is exhausted or if wait is false and a
        value is not immediately available.

        Args:
            wait: An optional bool that if false, fails the get operation when
                a value is not immediately available.

        Returns: An awaitable that evaluates to a value taken from the
            channel or None if the operation fails.

        Raises:
            RuntimeError: If the calling thread has no running event loop.
            QueueSizeExceeded: If the channel has too many pending get
                operations.
        """
        flag = hd.create_flag()
        future = hd.FlagFuture(flag)
        handler = hd.FlagHandler(flag, hd.future_deliver_fn(future), wait)
        ret = self._p_get(handler)
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
        return future

    def b_put(self, val, *, wait=True):
        """Same as put() except it blocks instead of returning an awaitable."""
        prom = hd.Promise()
        ret = self._p_put(hd.FnHandler(prom.deliver, wait), val)
        if ret is not None:
            return ret[0]
        return prom.deref()

    def b_get(self, *, wait=True):
        """Same as get() except it blocks instead of returning an awaitable."""
        prom = hd.Promise()
        ret = self._p_get(hd.FnHandler(prom.deliver, wait))
        if ret is not None:
            return ret[0]
        return prom.deref()

    def f_put(self, val, f=lambda _: None):
        """Asynchronously puts val onto the channel and calls f when complete.

        Args:
            val: A value to put onto the channel.
            f: An optional non-blocking function accepting the completion
                status of the put operation.

        Returns: False if channel is already closed or True if not.

        Raises:
            TypeError: If val is None.
            QueueSizeExceeded: If the channel has too many pending put
                operations.
        """
        ret = self._p_put(hd.FnHandler(f), val)
        if ret is None:
            return True
        f(ret[0])
        return ret[0]

    def f_get(self, f):
        """Asynchronously takes a value from the channel and calls f with it.

        Args:
            f: A non-blocking function accepting a single argument. Will be
                passed the value taken from the channel or None if channel is
                exhausted.

        Raises:
            QueueSizeExceeded: If the channel has too many pending get
                operations.
        """
        ret = self._p_get(hd.FnHandler(f))
        if ret is None:
            return
        f(ret[0])

    def offer(self, val):
        """Same as b_put(val, wait=False)."""
        return self.b_put(val, wait=False)

    def poll(self):
        """Same as b_get(wait=False)."""
        return self.b_get(wait=False)

    def close(self):
        """Closes the channel."""
        with self._lock:
            self._cleanup()
            self._close()

    def _p_put(self, handler, val):
        """Commits or enqueues a put operation to the channel.

        If the put operation completes immediately then the handler will be
        committed but its callback will not be invoked. The completion status
        of the operation will be wrapped in a tuple and returned. The status
        will be True if val was accepted onto the channel or False otherwise.

        If the operation is unable to complete immediately then handler and val
        will be enqueued and None will be returned. When the operation
        eventually completes, the handler will be committed and its callback
        will be invoked with the completion status.

        Args:
            handler: A handler that will be committed upon completion. Its
                callback will only be invoked if the operation is enqueued.
            val: A value to put onto the channel.

        Returns: A tuple containing the completion status if the operation
            completes immediately. None if the operation is enqueued.

        Raises:
            TypeError: If val is None.
            QueueSizeExceeded: If the channel has too many pending put
                operations.
        """
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
                self._transfer_buf_vals_to_takers()
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

            # Attempt to enqueue the operation
            if len(self._puts) >= MAX_QUEUE_SIZE:
                raise QueueSizeExceeded('channel has too many pending puts')
            self._puts.append((handler, val))

    def _p_get(self, handler):
        """Commits or enqueues a get operation to the channel.

        If the get operation completes immediately then the handler will be
        committed but its callback will not be invoked. If the channel is not
        already exhausted then the value taken from the channel will be wrapped
        in a tuple and returned. If the channel is already exhausted then the
        tuple, (None,), will be returned.

        If the operation is unable to complete immediately then handler and val
        will be enqueued and None will be returned. When the operation
        eventually completes, the handler will be committed and its callback
        will be invoked with the value taken from the channel or None if it is
        exhausted.

        Args:
            handler: A handler that will be committed upon completion. Its
                callback will only be invoked if the operation is enqueued.

        Returns: A tuple containing the result of the get operation if it
            completes immediately. None if the operation is enqueued.

        Raises:
            QueueSizeExceeded: If the channel has too many pending get
                operations.
        """
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

                self._complete_buf_rf_if_ready()
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

            # Attempt to enqueue the operation
            if len(self._takes) >= MAX_QUEUE_SIZE:
                raise QueueSizeExceeded('channel has too many pending gets')
            self._takes.append(handler)

    def _cleanup(self):
        """Removes enqueued operations that are no longer active."""
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

    def _transfer_buf_vals_to_takers(self):
        while len(self._takes) > 0 and len(self._buf) > 0:
            taker = self._takes.popleft()
            with taker:
                if taker.is_active:
                    taker.commit()(self._buf.get())

    def _complete_buf_rf_if_ready(self):
        """Calls buf_rf completion arity once if all input has been put to buf.

        Guarantees that the buf_rf completion arity will be invoked only after
        all input has been placed onto the buffer and that it will never be
        called more than once. Invoking the completion arity will flush any
        remaining values from the transformed reducing function onto buf.
        """
        if (self._is_closed and
                len(self._puts) == 0 and
                not self._buf_rf_is_completed):
            self._buf_rf_is_completed = True
            self._buf_rf(None)

    def _close(self):
        self._is_closed = True

        if self._buf is not None:
            self._complete_buf_rf_if_ready()
            self._transfer_buf_vals_to_takers()

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
    """Returns an awaitable representing the first channel operation to complete.

    Accepts an iterable of operations that either get from or put to a channel
    and commits only one of them. If no default is provided then only the first
    ports operation to complete will be committed. If default is provided and
    none of the ports operations complete immediately then none of the ports
    operations will be committed and default will instead be used to complete
    the returned awaitable.

    Args:
        ports: An iterable of operations that either get from or put to a
            channel. A get operation is represented as simply the channel to
            get from. A put operations is represented as an iterable of the form
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
        RuntimeError: If the calling thread has no running event loop.
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
