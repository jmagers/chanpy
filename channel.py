import asyncio
import random
import threading
from collections import deque
from genericfuncs import multiArity, isReduced
from toolz import identity


class FixedBuffer:
    def __init__(self, maxsize):
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise ValueError('maxsize must be a positive int')
        self._maxsize = maxsize
        self._deque = deque()

    def get(self):
        return self._deque.popleft()

    def put(self, item):
        self._deque.append(item)

    def is_full(self):
        return len(self._deque) >= self._maxsize

    def __len__(self):
        return len(self._deque)


class DroppingBuffer(FixedBuffer):
    def put(self, item):
        if len(self._deque) < self._maxsize:
            self._deque.append(item)

    def is_full(self):
        return False


class SlidingBuffer(FixedBuffer):
    def put(self, item):
        self._deque.append(item)
        if len(self._deque) > self._maxsize:
            self._deque.popleft()

    def is_full(self):
        return False


class Promise:
    def __init__(self):
        self._lock = threading.Lock()
        self._value = None
        self._is_realized = False
        self._realized = threading.Condition(self._lock)

    def deliver(self, value):
        with self._lock:
            if self._is_realized:
                return False
            self._value = value
            self._is_realized = True
            self._realized.notify_all()
            return True

    def deref(self):
        with self._lock:
            self._realized.wait_for(lambda: self._is_realized)
            return self._value


class FnHandler:
    def __init__(self, f, is_blockable=True):
        self._f = f
        self.is_blockable = is_blockable
        self.lock_id = 0
        self.is_active = True

    def acquire(self):
        pass

    def release(self):
        pass

    def commit(self):
        return self._f


_MAX_QUEUE_SIZE = 1024


class MaxQueueSize(Exception):
    """Maximum pending operations exceeded"""


class Chan:
    def __init__(self, buf=None, xform=identity):
        self._buf = buf
        self._takes = deque()
        self._puts = deque()
        self._is_closed = False
        self._xform_is_completed = False
        self._lock = threading.Lock()

        def step(_, val):
            if val is None:
                raise TypeError('xform cannot produce None')
            self._buf.put(val)

        self._buf_rf = xform(multiArity(lambda: None, lambda _: None, step))

    def a_put(self, val, block=True):
        return self._a_op(lambda h: self._put(h, val), block)

    def a_get(self, block=True):
        return self._a_op(self._get, block)

    def t_put(self, val, block=True):
        prom = Promise()
        ret = self._put(FnHandler(prom.deliver, block), val)
        if ret is not None:
            return ret[0]
        return prom.deref()

    def t_get(self, block=True):
        prom = Promise()
        ret = self._get(FnHandler(prom.deliver, block))
        if ret is not None:
            return ret[0]
        return prom.deref()

    def close(self):
        with self._lock:
            self._close()

    @staticmethod
    def _a_op(op, block):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def deliver(result):
            loop.call_soon_threadsafe(lambda: future.set_result(result))

        ret = op(FnHandler(deliver, block))
        if ret is not None:
            future.set_result(ret[0])
        return future

    def _put(self, handler, val):
        if val is None:
            raise TypeError('item cannot be None')
        with self._lock:
            self._cleanup()

            if self._is_closed:
                return self._fail_op(handler, False)

            # Attempt to transfer val onto buf
            if self._buf is not None and not self._buf.is_full():
                try:
                    handler.acquire()
                    if not handler.is_active:
                        return False,
                    handler.commit()
                finally:
                    handler.release()

                self._buf_put(val)
                self._distribute_buf_vals()
                return True,

            # Attempt to transfer val to a taker
            if self._buf is None:
                while len(self._takes) > 0:
                    taker = self._takes.popleft()
                    if handler.lock_id < taker.lock_id:
                        handler.acquire()
                        taker.acquire()
                    else:
                        taker.acquire()
                        handler.acquire()
                    taker_cb = None
                    if handler.is_active and taker.is_active:
                        handler.commit()
                        taker_cb = taker.commit()
                    handler.release()
                    taker.release()
                    if taker_cb is not None:
                        taker_cb(val)
                        return True,

            if not handler.is_blockable:
                return self._fail_op(handler, False)

            # Enqueue
            if len(self._puts) >= _MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._puts.append((handler, val))

    def _get(self, handler):
        with self._lock:
            self._cleanup()

            # Attempt to take val from buf
            if self._buf is not None and len(self._buf) > 0:
                try:
                    handler.acquire()
                    if not handler.is_active:
                        return None,
                    handler.commit()
                finally:
                    handler.release()

                ret = self._buf.get()

                # Transfer vals from putters onto buf
                while len(self._puts) > 0 and not self._buf.is_full():
                    putter, val = self._puts.popleft()
                    putter.acquire()
                    putter_cb = None
                    if putter.is_active:
                        putter_cb = putter.commit()
                    putter.release()
                    if putter_cb is not None:
                        self._buf_put(val)
                        putter_cb(True)

                self._complete_xform_if_ready()
                return ret,

            # Attempt to take val from a putter
            if self._buf is None:
                while len(self._puts) > 0:
                    putter, val = self._puts.popleft()
                    if handler.lock_id < putter.lock_id:
                        handler.acquire()
                        putter.acquire()
                    else:
                        putter.acquire()
                        handler.acquire()
                    putter_cb = None
                    if handler.is_active and putter.is_active:
                        handler.commit()
                        putter_cb = putter.commit()
                    handler.release()
                    putter.release()
                    if putter_cb is not None:
                        putter_cb(True)
                        return val,

            if self._is_closed or not handler.is_blockable:
                return self._fail_op(handler, None)

            # Enqueue
            if len(self._takes) >= _MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._takes.append(handler)

    def _cleanup(self):
        self._takes = deque(h for h in self._takes if h.is_active)
        self._puts = deque((h, v) for h, v in self._puts if h.is_active)

    @staticmethod
    def _fail_op(handler, val):
        handler.acquire()
        try:
            if handler.is_active:
                handler.commit()
                return val,
            return
        finally:
            handler.release()

    def _buf_put(self, val):
        if isReduced(self._buf_rf(None, val)):
            # If reduced value is returned then no more input is allowed onto
            # buf. To ensure this, remove all pending puts and close ch.
            for putter, _ in self._puts:
                putter.acquire()
                put_cb = None
                if putter.is_active:
                    put_cb = putter.commit()
                putter.release()
                if put_cb is not None:
                    put_cb(False)
            self._puts.clear()
            self._close()

    def _distribute_buf_vals(self):
        while len(self._takes) > 0 and len(self._buf) > 0:
            taker = self._takes.popleft()
            taker.acquire()
            taker_cb = None
            if taker.is_active:
                taker_cb = taker.commit()
            taker.release()
            if taker_cb is not None:
                taker_cb(self._buf.get())

    def _complete_xform_if_ready(self):
        """Calls the xform completion arity exactly once iff all input has been
        placed onto buf"""
        if (self._is_closed and
                len(self._puts) == 0 and
                not self._xform_is_completed):
            self._xform_is_completed = True
            self._buf_rf(None)

    def _close(self):
        self._cleanup()
        self._is_closed = True

        if self._buf is not None:
            self._complete_xform_if_ready()
            self._distribute_buf_vals()

        # Remove pending takes
        # No-op if there are pending puts or buffer isn't empty
        for taker in self._takes:
            taker.acquire()
            take_cb = None
            if taker.is_active:
                take_cb = taker.commit()
            taker.release()
            if take_cb is not None:
                take_cb(None)
        self._takes.clear()

    def __iter__(self):
        while True:
            value = self.t_get()
            if value is None:
                break
            yield value

    async def __aiter__(self):
        while True:
            value = await self.a_get()
            if value is None:
                break
            yield value


def is_chan(ch):
    return isinstance(ch, Chan)


def chan(buf=None, xform=None):
    if buf is None:
        if xform is not None:
            raise TypeError('unbuffered channels cannot have an xform')
        return Chan()
    new_buf = FixedBuffer(buf) if isinstance(buf, int) else buf
    return Chan(new_buf, identity if xform is None else xform)


class _AltHandler:
    def __init__(self, flag, cb):
        self._flag = flag
        self._cb = cb
        self.lock_id = id(flag)
        self.is_blockable = True

    @property
    def is_active(self):
        return self._flag['is_active']

    def acquire(self):
        self._flag['lock'].acquire()

    def release(self):
        self._flag['lock'].release()

    def commit(self):
        self._flag['is_active'] = False
        return self._cb


def _alts(deliver, ports, priority):
    ports = list(ports)
    if len(ports) == 0:
        raise ValueError('alts must have at least one channel operation')
    if not priority:
        random.shuffle(ports)

    ops = {}

    # Parse ports into ops
    for p in ports:
        if type(p) in [list, tuple]:
            ch, val = p
            op = {'type': 'put', 'value': val}
        else:
            ch = p
            op = {'type': 'get'}
        if ops.get(ch, op)['type'] != op['type']:
            raise ValueError('cannot get and put to same channel')
        ops[ch] = op

    flag = {'lock': threading.Lock(), 'is_active': True}

    def create_handler(ch):
        return _AltHandler(flag, lambda val: deliver((val, ch)))

    # Start ops
    for ch, op in ops.items():
        if op['type'] == 'get':
            ret = ch._get(create_handler(ch))
        elif op['type'] == 'put':
            ret = ch._put(create_handler(ch), op['value'])
        if ret is not None:
            return ret[0], ch


def a_alts(ports, priority=False):
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def deliver(result):
        loop.call_soon_threadsafe(lambda: future.set_result(result))

    ret = _alts(deliver, ports, priority)
    if ret is not None:
        future.set_result(ret)
    return future


def t_alts(ports, priority=False):
    prom = Promise()
    ret = _alts(prom.deliver, ports, priority)
    return prom.deref() if ret is None else ret


def async_put(port, val, f=lambda _: None, on_caller=True):
    ret = port._put(FnHandler(f), val)
    if ret is None:
        return True
    elif on_caller:
        f(ret[0])
    else:
        threading.Thread(target=f, args=[ret[0]]).start()
    return ret[0]


def async_get(port, f, on_caller=True):
    ret = port._get(FnHandler(f))
    if ret is None:
        return None
    elif on_caller:
        f(ret[0])
    else:
        threading.Thread(target=f, args=[ret[0]]).start()


def reduce(go, f, init, ch):
    result_ch = chan(1)

    async def proc():
        result = init
        async for val in ch:
            result = f(result, val)
        await result_ch.a_put(result)
        result_ch.close()

    go(proc())
    return result_ch


def thread_call(f):
    ch = chan(1)

    def wrapper():
        ch.t_put(f())
        ch.close()

    threading.Thread(target=wrapper).start()
    return ch


async def a_list(ch):
    return [x async for x in ch]


class Go:
    def __init__(self):
        self._loop = asyncio.get_running_loop()

    @property
    def loop(self):
        return self._loop

    def in_loop(self):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        return loop is self._loop

    def __call__(self, coro, daemon=False):
        if self.in_loop():
            asyncio.create_task(coro)
        else:
            asyncio.run_coroutine_threadsafe(coro, self._loop)

        # TODO: Add daemon feature

    def get(self, coro):
        ch = chan(1)

        async def wrapper():
            await ch.a_put(await coro)
            ch.close()

        self(wrapper())
        return ch

    def schedule_callback(self, cb, eager=False):
        """Schedules cb to run in event loop.
        Returns a ch that closes when finished."""
        ch = chan()

        def wrapper():
            cb()
            ch.close()

        if self.in_loop():
            if eager:
                wrapper()
            else:
                self._loop.call_soon(wrapper)
        else:
            self._loop.call_soon_threadsafe(wrapper)

        return ch

    def run_callback(self, cb):
        self.schedule_callback(cb, eager=True).t_get()


def timeout(go, msecs):
    ch = chan()
    go.loop.call_later(msecs / 1000, ch.close)
    return ch


def onto_chan(go, ch, coll, close=True):
    close_ch = chan()

    async def proc():
        for x in coll:
            await ch.a_put(x)
        close_ch.close()
        if close:
            ch.close()

    go(proc())
    return close_ch


def to_chan(go, coll):
    ch = chan()
    onto_chan(go, ch, coll)
    return ch


def pipe(go, from_ch, to_ch, close=True):
    complete_ch = chan()

    async def proc():
        async for val in from_ch:
            if not await to_ch.a_put(val):
                break
        complete_ch.close()
        if close:
            to_ch.close()

    go(proc())
    return complete_ch


def merge(go, chs, buf=None):
    to_ch = chan(buf)

    async def proc():
        ports = set(chs)
        while len(ports) > 0:
            val, ch = await a_alts(ports)
            if val is None:
                ports.remove(ch)
            else:
                await to_ch.a_put(val)
        to_ch.close()

    go(proc())
    return to_ch


class Mult:
    def __init__(self, go, ch):
        self._go = go
        self._src_ch = ch
        self._consumers = {}
        self._is_closed = False
        go(self._proc())

    def tap(self, ch, close=True):
        def _tap():
            if self._is_closed and close:
                ch.close()
            self._consumers[ch] = close
        self._go.run_callback(_tap)

    def untap(self, ch):
        self._go.run_callback(lambda: self._consumers.pop(ch, None))

    def untap_all(self):
        self._go.run_callback(self._consumers.clear)

    async def _proc(self):
        while True:
            # Get next item to distribute. Close consumers when src_ch closes.
            item = await self._src_ch.a_get()
            if item is None:
                self._is_closed = True
                for consumer, close in self._consumers.items():
                    if close:
                        consumer.close()
                break

            # Distribute item to consumers
            remaining_chs = {ch: item for ch in self._consumers.keys()}
            while len(remaining_chs) > 0:
                is_open, ch = await a_alts(remaining_chs.items())
                if not is_open:
                    self._consumers.pop(ch, None)
                remaining_chs.pop(ch)


def mult(go, ch):
    return Mult(go, ch)


class Mix:
    def __init__(self, go, ch):
        self._go = go
        self._state_ch = chan(SlidingBuffer(1))
        self._state_map = {}
        self._solo_mode = 'mute'
        go(self._proc(ch))

    def toggle(self, state_map):
        for ch, state in state_map.items():
            if not is_chan(ch):
                raise ValueError(f'state_map key is not a channel: '
                                 f'{state}')
            if not set(state.keys()).issubset({'solo', 'pause', 'mute'}):
                raise ValueError(f'state contains invalid options: '
                                 f'{state}')
            if not set(state.values()).issubset({True, False}):
                raise ValueError(f'state contains non-boolean values: '
                                 f'{state}')

            def _toggle():
                for ch, new_state in state_map.items():
                    original_state = self._state_map.get(ch, {'solo': False,
                                                              'pause': False,
                                                              'mute': False})
                    self._state_map[ch] = {**original_state, **new_state}
                self._sync_state()

            self._go.run_callback(_toggle)

    def admix(self, ch):
        self.toggle({ch: {}})

    def unmix(self, ch):
        def _unmix():
            self._state_map.pop(ch, None)
            self._sync_state()
        self._go.run_callback(_unmix)

    def unmix_all(self):
        def _unmix_all():
            self._state_map.clear()
            self._sync_state()
        self._go.run_callback(_unmix_all)

    def solo_mode(self, mode):
        if mode not in ['pause', 'mute']:
            raise ValueError(f'solo-mode is invalid: {mode}')

        def _solo_mode():
            self._solo_mode = mode
            self._sync_state()

        self._go.run_callback(_solo_mode)

    def _sync_state(self):
        soloed_chs, muted_chs, live_chs = set(), set(), set()

        for ch, state in self._state_map.items():
            if state['solo']:
                soloed_chs.add(ch)
            elif state['pause']:
                continue
            elif state['mute']:
                muted_chs.add(ch)
            else:
                live_chs.add(ch)

        if len(soloed_chs) == 0:
            self._state_ch.t_put({'live_chs': live_chs,
                                  'muted_chs': muted_chs})
        elif self._solo_mode == 'pause':
            self._state_ch.t_put({'live_chs': soloed_chs, 'muted_chs': set()})
        elif self._solo_mode == 'mute':
            self._state_ch.t_put({'live_chs': soloed_chs,
                                  'muted_chs': muted_chs.union(live_chs)})

    async def _proc(self, to_ch):
        live_chs, muted_chs = set(), set()
        while True:
            data_chs = list(live_chs.union(muted_chs))
            random.shuffle(data_chs)
            val, ch = await a_alts([self._state_ch, *data_chs], priority=True)
            if ch is self._state_ch:
                live_chs, muted_chs = val['live_chs'], val['muted_chs']
            elif val is None:
                self._state_map.pop(ch, None)
                live_chs.discard(ch)
                muted_chs.discard(ch)
            elif ch in muted_chs:
                pass
            elif not await to_ch.a_put(val):
                break


def mix(go, ch):
    return Mix(go, ch)
