import asyncio
import random
import threading
from collections import deque
from genericfuncs import multiArity, isReduced
from toolz import identity


class _UNDEFINED:
    pass


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


class UnblockingBufferMixin:
    def is_full(self):
        return False


class DroppingBuffer(UnblockingBufferMixin, FixedBuffer):
    def put(self, item):
        if len(self._deque) < self._maxsize:
            self._deque.append(item)


class SlidingBuffer(UnblockingBufferMixin, FixedBuffer):
    def put(self, item):
        self._deque.append(item)
        if len(self._deque) > self._maxsize:
            self._deque.popleft()


class PromiseBuffer(UnblockingBufferMixin):
    def __init__(self):
        self._value = None

    def get(self):
        return self._value

    def put(self, item):
        if self._value is None:
            self._value = item

    def __len__(self):
        return 0 if self._value is None else 1


def is_unblocking_buffer(buf):
    return isinstance(buf, UnblockingBufferMixin)


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


def create_flag():
    return {'lock': threading.Lock(), 'is_active': True}


class FnHandler:
    def __init__(self, cb, is_blockable=True):
        self._cb = cb
        self.is_blockable = is_blockable
        self.lock_id = 0
        self.is_active = True

    def acquire(self):
        pass

    def release(self):
        pass

    def commit(self):
        return self._cb


class FlagHandler:
    def __init__(self, flag, cb, is_blockable=True):
        self._flag = flag
        self._cb = cb
        self.is_blockable = is_blockable
        self.lock_id = id(flag)

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


class FlagFuture(asyncio.Future):
    def __init__(self, flag):
        self.__flag = flag
        self.__result = None
        super().__init__(loop=asyncio.get_running_loop())

    def set_result(self, result):
        assert False

    def set_exception(self, exception):
        assert False

    def cancel(self):
        with self.__flag['lock']:
            if self.__flag['is_active']:
                self.__flag['is_active'] = False
            elif not super().done():
                # This case is when value has been committed but
                # future hasn't been set because call_soon_threadsafe()
                # callback hasn't been invoked yet
                super().set_result(self.__result)
        return super().cancel()


def _create_future_deliver_fn(future):
    def set_result(result):
        try:
            asyncio.Future.set_result(future, result)
        except asyncio.InvalidStateError:
            assert future.result() is result

    def deliver(result):
        future._FlagFuture__result = result
        future.get_loop().call_soon_threadsafe(lambda: set_result(result))

    return deliver


_MAX_QUEUE_SIZE = 1024


class MaxQueueSize(Exception):
    """Maximum pending operations exceeded"""


def nop_ex_handler(e):
    raise e


class Chan:
    def __init__(self, buf=None, xform=identity, ex_handler=nop_ex_handler):
        self._buf = buf
        self._takes = deque()
        self._puts = deque()
        self._is_closed = False
        self._xform_is_completed = False
        self._lock = threading.Lock()

        def ex_handler_xform(rf):
            def wrapper(*args, **kwargs):
                try:
                    return rf(*args, **kwargs)
                except Exception as e:
                    val = ex_handler(e)
                    if val is not None:
                        self._buf.put(val)
            return wrapper

        def step(_, val):
            if val is None:
                raise TypeError('xform cannot produce None')
            self._buf.put(val)

        rf = multiArity(lambda: None, lambda _: None, step)
        self._buf_rf = ex_handler_xform(xform(rf))

    def a_put(self, val, wait=True):
        return self._a_op(lambda h: self._put(h, val), wait)

    def a_get(self, wait=True):
        return self._a_op(self._get, wait)

    def t_put(self, val, wait=True):
        prom = Promise()
        ret = self._put(FnHandler(prom.deliver, wait), val)
        if ret is not None:
            return ret[0]
        return prom.deref()

    def t_get(self, wait=True):
        prom = Promise()
        ret = self._get(FnHandler(prom.deliver, wait))
        if ret is not None:
            return ret[0]
        return prom.deref()

    def offer(self, val):
        return self.t_put(val, wait=False)

    def poll(self):
        return self.t_get(wait=False)

    def close(self):
        with self._lock:
            self._close()

    @staticmethod
    def _a_op(op, wait):
        flag = create_flag()
        future = FlagFuture(flag)
        ret = op(FlagHandler(flag, _create_future_deliver_fn(future), wait))
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
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
                        taker_cb(val)
                    handler.release()
                    taker.release()
                    if taker_cb is not None:
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
                    if putter.is_active:
                        putter.commit()(True)
                        self._buf_put(val)
                    putter.release()

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
                        putter_cb(True)
                    handler.release()
                    putter.release()
                    if putter_cb is not None:
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
                if putter.is_active:
                    putter.commit()(False)
                putter.release()
            self._puts.clear()
            self._close()

    def _distribute_buf_vals(self):
        while len(self._takes) > 0 and len(self._buf) > 0:
            taker = self._takes.popleft()
            taker.acquire()
            if taker.is_active:
                taker.commit()(self._buf.get())
            taker.release()

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
            if taker.is_active:
                taker.commit()(None)
            taker.release()
        self._takes.clear()

    async def __aiter__(self):
        while True:
            value = await self.a_get()
            if value is None:
                break
            yield value


def is_chan(ch):
    return isinstance(ch, Chan)


def chan(buf=None, xform=identity, ex_handler=nop_ex_handler):
    if buf is None:
        if xform is not identity:
            raise TypeError('unbuffered channels cannot have an xform')
        if ex_handler is not nop_ex_handler:
            raise TypeError('unbuffered channels cannot have an ex_handler')
        return Chan()
    new_buf = FixedBuffer(buf) if isinstance(buf, int) else buf
    return Chan(new_buf, xform, ex_handler)


def promise_chan(xform=identity):
    return chan(PromiseBuffer(), xform)


def _alts(flag, deliver_fn, ports, priority):
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
        return FlagHandler(flag, lambda val: deliver_fn((val, ch)))

    # Start ops
    for ch, op in ops.items():
        if op['type'] == 'get':
            ret = ch._get(create_handler(ch))
        elif op['type'] == 'put':
            ret = ch._put(create_handler(ch), op['value'])
        if ret is not None:
            return ret[0], ch


def a_alts(ports, priority=False):
    flag = create_flag()
    future = FlagFuture(flag)
    ret = _alts(flag, _create_future_deliver_fn(future), ports, priority)
    if ret is not None:
        asyncio.Future.set_result(future, ret)
    return future


def t_alts(ports, priority=False):
    prom = Promise()
    ret = _alts(create_flag(), prom.deliver, ports, priority)
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

    def __call__(self, coro):
        if self.in_loop():
            asyncio.create_task(coro)
        else:
            asyncio.run_coroutine_threadsafe(coro, self._loop)

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

    @property
    def muxch(self):
        return self._src_ch

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
        async for item in self._src_ch:
            pending_puts = [(ch, ch.a_put(item)) for ch in self._consumers]
            for ch, pending_put in pending_puts:
                if not await pending_put:
                    self._consumers.pop(ch, None)

        self._is_closed = True
        for consumer, close in self._consumers.items():
            if close:
                consumer.close()


def mult(go, ch):
    return Mult(go, ch)


class Pub:
    def __init__(self, go, ch, topic_fn, buf_fn=lambda _: None):
        self._go = go
        self._src_ch = ch
        self._topic_fn = topic_fn
        self._buf_fn = buf_fn
        self._mults = {}
        go(self._proc())

    def sub(self, topic, ch, close=True):
        def _sub():
            if topic not in self._mults:
                self._mults[topic] = mult(self._go, chan(self._buf_fn(topic)))
            self._mults[topic].tap(ch, close)
        self._go.run_callback(_sub)

    def unsub(self, topic, ch):
        def _unsub():
            m = self._mults.get(topic, None)
            if m is not None:
                m.untap(ch)
                if len(m._consumers) == 0:
                    m.muxch.close()
                    self._mults.pop(topic)
        self._go.run_callback(_unsub)

    def unsub_all(self, topic=_UNDEFINED):
        def _unsub_all():
            topics = tuple(self._mults) if topic is _UNDEFINED else [topic]
            for t in topics:
                m = self._mults.get(t, None)
                if m is not None:
                    m.untap_all()
                    m.muxch.close()
                    self._mults.pop(t)
        self._go.run_callback(_unsub_all)

    async def _proc(self):
        async for item in self._src_ch:
            m = self._mults.get(self._topic_fn(item), None)
            if m is not None:
                await m.muxch.a_put(item)

        for m in self._mults.values():
            m.muxch.close()
        self._mults.clear()


def pub(go, ch, topic_fn, buf_fn=lambda _: None):
    return Pub(go, ch, topic_fn, buf_fn)


class Mix:
    def __init__(self, go, ch):
        self._go = go
        self._to_ch = ch
        self._state_ch = chan(SlidingBuffer(1))
        self._state_map = {}
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

    async def _proc(self):
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
            elif not await self._to_ch.a_put(val):
                break


def mix(go, ch):
    return Mix(go, ch)


def split(go, pred, ch, t_buf=None, f_buf=None):
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
