import asyncio as _asyncio
import contextlib as _contextlib
import random as _random
import threading as _threading
from numbers import Number as _Number
from . import buffers as _bufs
from .channel import Chan as _Chan
from . import handlers as _hd
from . import xf


class _UNDEFINED:
    pass


def buffer(n):
    return _bufs.FixedBuffer(n)


def dropping_buffer(n):
    return _bufs.DroppingBuffer(n)


def sliding_buffer(n):
    return _bufs.SlidingBuffer(n)


def is_unblocking_buffer(buf):
    return isinstance(buf, _bufs.UnblockingBufferMixin)


def chan(buf_or_n=None, xform=None, ex_handler=None):
    if buf_or_n is None:
        if xform is not None:
            raise TypeError('unbuffered channels cannot have an xform')
        if ex_handler is not None:
            raise TypeError('unbuffered channels cannot have an ex_handler')
        return _Chan()
    buf = buffer(buf_or_n) if isinstance(buf_or_n, _Number) else buf_or_n
    return _Chan(buf, xform, ex_handler)


def promise_chan(xform=xf.identity):
    return chan(_bufs.PromiseBuffer(), xform)


def is_chan(ch):
    return isinstance(ch, _Chan)


def _alts(flag, deliver_fn, ports, priority, default):
    ports = list(ports)
    if len(ports) == 0:
        raise ValueError('alts must have at least one channel operation')
    if not priority:
        _random.shuffle(ports)

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
        return _hd.FlagHandler(flag, lambda val: deliver_fn((val, ch)))

    # Start ops
    for ch, op in ops.items():
        if op['type'] == 'get':
            ret = ch._get(create_handler(ch))
        elif op['type'] == 'put':
            ret = ch._put(create_handler(ch), op['value'])
        if ret is not None:
            return ret[0], ch

    if default is not _UNDEFINED:
        with flag['lock']:
            if flag['is_active']:
                flag['is_active'] = False
                return default, 'default'


def a_alts(ports, *, priority=False, default=_UNDEFINED):
    flag = _hd.create_flag()
    future = _hd.FlagFuture(flag)
    ret = _alts(flag, _hd.future_deliver_fn(future), ports,
                priority, default)
    if ret is not None:
        _asyncio.Future.set_result(future, ret)
    return future


def t_alts(ports, *, priority=False, default=_UNDEFINED):
    prom = _hd.Promise()
    ret = _alts(_hd.create_flag(), prom.deliver, ports, priority, default)
    return prom.deref() if ret is None else ret


_local_data = _threading.local()


def get_loop():
    loop = getattr(_local_data, 'loop', None)
    return _asyncio.get_running_loop() if loop is None else loop


def ensure_loop(loop):
    return get_loop() if loop is None else loop


def set_loop(loop):
    _local_data.loop = loop


def in_loop(loop):
    try:
        return _asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


@_contextlib.contextmanager
def loop_manager(loop):
    prev_loop = getattr(_local_data, 'loop', None)
    set_loop(loop)
    try:
        yield
    finally:
        set_loop(prev_loop)


def thread_call(f, executor=None):
    loop = get_loop()
    ch = chan(1)

    def wrapper():
        with loop_manager(loop):
            ret = f()
            if ret is not None:
                ch.t_put(ret)
            ch.close()

    if executor is None:
        _threading.Thread(target=wrapper).start()
    else:
        executor.submit(wrapper)
    return ch


def async_put(port, val, f=lambda _: None, *, on_caller=True, executor=None):
    ret = port._put(_hd.FnHandler(f), val)
    if ret is None:
        return True
    elif on_caller:
        f(ret[0])
    else:
        thread_call(lambda: f(ret[0]), executor)
    return ret[0]


def async_get(port, f, *, on_caller=True, executor=None):
    ret = port._get(_hd.FnHandler(f))
    if ret is None:
        return
    elif on_caller:
        f(ret[0])
    else:
        thread_call(lambda: f(ret[0]), executor)


def to_iter(ch):
    while True:
        val = ch.t_get()
        if val is None:
            break
        yield val


def t_list(ch):
    return list(to_iter(ch))


def t_tuple(ch):
    return tuple(to_iter(ch))


async def a_list(ch):
    return [x async for x in ch]


async def a_tuple(ch):
    return tuple(await a_list(ch))


def go(coro, loop=None):
    loop = ensure_loop(loop)
    ch = chan(1)

    def create_tasks():
        # coro and put_result_to_ch coroutines need to be added separately or
        # else a 'coroutine never awaited' RuntimeWarning could get raised when
        # call_soon_threadsafe is used

        coro_task = _asyncio.create_task(coro)

        async def put_result_to_ch():
            ret = await coro_task
            if ret is not None:
                await ch.a_put(ret)
            ch.close()

        loop.create_task(put_result_to_ch())

    if in_loop(loop):
        create_tasks()
    else:
        loop.call_soon_threadsafe(create_tasks)

    return ch


def call_soon(cb, loop=None, *, eager=False):
    """Schedules cb to run in event loop.
    Returns a ch that contains the return value."""
    loop = ensure_loop(loop)
    ch = chan(1)

    def wrapper():
        ret = cb()
        if ret is not None:
            ch.t_put(ret)
        ch.close()

    if in_loop(loop):
        if eager:
            wrapper()
        else:
            loop.call_soon(wrapper)
    else:
        loop.call_soon_threadsafe(wrapper)

    return ch


def timeout(msecs):
    ch = chan()
    get_loop().call_later(msecs / 1000, ch.close)
    return ch


def _reduce(rf, init, ch):
    async def proc():
        result = init
        async for val in ch:
            result = rf(result, val)
            if xf.is_reduced(result):
                break
        return xf.unreduced(result)

    return go(proc())


def reduce(rf, init, ch=_UNDEFINED):
    if ch is _UNDEFINED:
        return _reduce(rf, rf(), init)
    return _reduce(rf, init, ch)


def _transduce(xform, rf, init, ch):
    async def proc():
        xrf = xform(rf)
        ret = await reduce(xrf, init, ch).a_get()
        return xrf(ret)

    return go(proc())


def transduce(xform, rf, init, ch=_UNDEFINED):
    if ch is _UNDEFINED:
        return _transduce(xform, rf, rf(), init)
    return _transduce(xform, rf, init, ch)


def onto_chan(ch, coll, *, close=True):
    async def proc():
        for x in coll:
            await ch.a_put(x)
        if close:
            ch.close()

    return go(proc())


def to_chan(coll):
    ch = chan()
    onto_chan(ch, coll)
    return ch


def pipe(from_ch, to_ch, *, close=True):
    async def proc():
        async for val in from_ch:
            if not await to_ch.a_put(val):
                break
        if close:
            to_ch.close()

    go(proc())
    return to_ch


def merge(chs, buf=None):
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
    def __init__(self, ch):
        self._lock = _threading.Lock()
        self._from_ch = ch
        self._taps = {}  # ch->close
        self._is_closed = False
        go(self._proc())

    @property
    def muxch(self):
        return self._from_ch

    def tap(self, ch, *, close=True):
        with self._lock:
            if self._is_closed and close:
                ch.close()
            self._taps[ch] = close

    def untap(self, ch):
        with self._lock:
            self._taps.pop(ch, None)

    def untap_all(self):
        with self._lock:
            self._taps.clear()

    async def _proc(self):
        async for item in self._from_ch:
            with self._lock:
                chs = tuple(self._taps)
            results = await _asyncio.gather(*(ch.a_put(item) for ch in chs))
            with self._lock:
                for ch, is_open in zip(chs, results):
                    if not is_open:
                        self._taps.pop(ch, None)

        with self._lock:
            self._is_closed = True
            for ch, close in self._taps.items():
                if close:
                    ch.close()


def mult(ch):
    return Mult(ch)


class Pub:
    def __init__(self, ch, topic_fn, buf_fn=lambda _: None):
        self._lock = _threading.Lock()
        self._from_ch = ch
        self._topic_fn = topic_fn
        self._buf_fn = buf_fn
        self._mults = {}  # topic->mult
        go(self._proc())

    def sub(self, topic, ch, *, close=True):
        with self._lock:
            if topic not in self._mults:
                self._mults[topic] = mult(chan(self._buf_fn(topic)))
            self._mults[topic].tap(ch, close=close)

    def unsub(self, topic, ch):
        with self._lock:
            m = self._mults.get(topic, None)
            if m is not None:
                m.untap(ch)
                if len(m._taps) == 0:
                    m.muxch.close()
                    self._mults.pop(topic)

    def unsub_all(self, topic=_UNDEFINED):
        with self._lock:
            topics = tuple(self._mults) if topic is _UNDEFINED else [topic]
            for t in topics:
                m = self._mults.get(t, None)
                if m is not None:
                    m.untap_all()
                    m.muxch.close()
                    self._mults.pop(t)

    async def _proc(self):
        async for item in self._from_ch:
            with self._lock:
                m = self._mults.get(self._topic_fn(item), None)
            if m is not None:
                await m.muxch.a_put(item)

        with self._lock:
            for m in self._mults.values():
                m.muxch.close()
            self._mults.clear()


def pub(ch, topic_fn, buf_fn=lambda _: None):
    return Pub(ch, topic_fn, buf_fn)


class Mix:
    def __init__(self, ch):
        self._lock = _threading.Lock()
        self._to_ch = ch
        self._state_ch = chan(sliding_buffer(1))
        self._state_map = {}  # ch->state
        self._solo_mode = 'mute'
        go(self._proc())

    @property
    def muxch(self):
        return self._to_ch

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

        with self._lock:
            for ch, new_state in state_map.items():
                original_state = self._state_map.get(ch, {'solo': False,
                                                          'pause': False,
                                                          'mute': False})
                self._state_map[ch] = {**original_state, **new_state}
            self._sync_state()

    def admix(self, ch):
        self.toggle({ch: {}})

    def unmix(self, ch):
        with self._lock:
            self._state_map.pop(ch, None)
            self._sync_state()

    def unmix_all(self):
        with self._lock:
            self._state_map.clear()
            self._sync_state()

    def solo_mode(self, mode):
        if mode not in ['pause', 'mute']:
            raise ValueError(f'solo-mode is invalid: {mode}')

        with self._lock:
            self._solo_mode = mode
            self._sync_state()

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

    async def _proc(self):
        live_chs, muted_chs = set(), set()
        while True:
            data_chs = list(live_chs.union(muted_chs))
            _random.shuffle(data_chs)
            val, ch = await a_alts([self._state_ch, *data_chs], priority=True)
            if ch is self._state_ch:
                live_chs, muted_chs = val['live_chs'], val['muted_chs']
            elif val is None:
                with self._lock:
                    self._state_map.pop(ch, None)
                live_chs.discard(ch)
                muted_chs.discard(ch)
            elif ch in muted_chs:
                pass
            elif not await self._to_ch.a_put(val):
                break


def mix(ch):
    return Mix(ch)


def split(pred, ch, t_buf=None, f_buf=None):
    true_ch, false_ch = chan(t_buf), chan(f_buf)

    async def proc():
        async for x in ch:
            if pred(x):
                await true_ch.a_put(x)
            else:
                await false_ch.a_put(x)
        true_ch.close()
        false_ch.close()

    go(proc())
    return true_ch, false_ch
