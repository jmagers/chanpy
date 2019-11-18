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
            assert val is not None
            self._buf.put(val)

        self._bufRf = xform(multiArity(lambda: None, lambda _: None, step))

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

    def _a_op(self, op, block):
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
        if isReduced(self._bufRf(None, val)):
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
            self._bufRf(None)

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


def isChan(ch):
    return isinstance(ch, Chan)


# Event loop globals
_event_loop = None
_loop_lock = threading.Lock()


def set_event_loop(event_loop):
    global _loop_lock, _event_loop
    with _loop_lock:
        assert _event_loop is None or _event_loop.is_closed()
        _event_loop = event_loop


def get_event_loop():
    global _loop_lock, _event_loop
    with _loop_lock:
        if _event_loop is None or _event_loop.is_closed():
            _event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_event_loop)
        return _event_loop


def go(coro):
    return asyncio.run_coroutine_threadsafe(coro, _event_loop)


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


def alts(ports, priority=False):
    prom = Promise()
    ret = _alts(prom.deliver, ports, priority)
    return prom.deref() if ret is None else ret


def chan(buf=None, xform=None):
    if buf is None:
        if xform is not None:
            raise TypeError('unbuffered channels cannot have an xform')
        return Chan()
    newBuf = FixedBuffer(buf) if isinstance(buf, int) else buf
    return Chan(newBuf, identity if xform is None else xform)


def reduce(f, init, ch):
    result = init
    while True:
        value = ch.t_get()
        if value is None:
            return result
        result = f(result, value)


def ontoChan(ch, coll, close=True):
    newCh = chan()

    def thread():
        for x in coll:
            ch.t_put(x)
        newCh.close()
        if close:
            ch.close()

    threading.Thread(target=thread, daemon=True).start()
    return newCh


def toChan(coll):
    ch = chan()
    ontoChan(ch, coll)
    return ch


def timeout(msecs):
    ch = chan()
    timer = threading.Timer(msecs / 1000, ch.close)
    timer.daemon = True
    timer.start()
    return ch


def pipe(fromCh, toCh, close=True):
    completeCh = chan()

    def thread():
        while True:
            val = fromCh.t_get()
            if val is None or not toCh.t_put(val):
                completeCh.close()
                if close:
                    toCh.close()
                return
    threading.Thread(target=thread, daemon=True).start()
    return completeCh


def merge(chs, buf=None):
    toCh = chan(buf)

    def thread():
        ports = set(chs)
        while len(ports) > 0:
            val, ch = alts(ports)
            if val is None:
                ports.remove(ch)
            else:
                toCh.t_put(val)
        toCh.close()

    threading.Thread(target=thread, daemon=True).start()
    return toCh


class Mult:
    def __init__(self, ch):
        self._srcCh = ch
        self._consumers = {}
        self._isClosed = False
        self._lock = threading.Lock()
        threading.Thread(target=self._proc, daemon=True).start()

    def tap(self, ch, close=True):
        with self._lock:
            if self._isClosed and close:
                ch.close()
            self._consumers[ch] = close

    def untap(self, ch):
        with self._lock:
            self._consumers.pop(ch, None)

    def untapAll(self):
        with self._lock:
            self._consumers.clear()

    def _proc(self):
        while True:
            # Get next item to distribute. Close consumers when srcCh closes.
            item = self._srcCh.t_get()
            if item is None:
                with self._lock:
                    self._isClosed = True
                    for consumer, close in self._consumers.items():
                        if close:
                            consumer.close()
                break

            # Distribute item to consumers
            with self._lock:
                remainingConsumers = set(self._consumers.keys())
            while len(remainingConsumers) > 0:
                stillOpen, ch = alts([ch, item] for ch in remainingConsumers)
                if not stillOpen:
                    with self._lock:
                        self._consumers.pop(ch, None)
                remainingConsumers.remove(ch)


def mult(ch):
    return Mult(ch)


class Mix:
    def __init__(self, toCh):
        self._stateCh = chan(SlidingBuffer(1))
        self._stateMap = {}
        self._soloMode = 'mute'
        self._lock = threading.Lock()
        threading.Thread(target=self._proc, args=[toCh], daemon=True).start()

    def toggle(self, stateMap):
        with self._lock:
            for ch, state in stateMap.items():
                if not isChan(ch):
                    raise ValueError(f'stateMap key is not a channel: {state}')
                if not set(state.keys()).issubset({'solo', 'pause', 'mute'}):
                    raise ValueError(f'state contains invalid options: '
                                     f'{state}')
                if not set(state.values()).issubset({True, False}):
                    raise ValueError(f'state contains non-boolean values: '
                                     f'{state}')
            for fromCh, newState in stateMap.items():
                originalState = self._stateMap.get(ch, {'solo': False,
                                                        'pause': False,
                                                        'mute': False})
                self._stateMap[fromCh] = {**originalState, **newState}
            self._syncState()

    def admix(self, ch):
        self.toggle({ch: {}})

    def unmix(self, ch):
        with self._lock:
            self._stateMap.pop(ch, None)
            self._syncState()

    def unmixAll(self):
        with self._lock:
            self._stateMap.clear()
            self._syncState()

    def soloMode(self, mode):
        with self._lock:
            if mode not in ['pause', 'mute']:
                raise ValueError(f'solo-mode is invalid: {mode}')
            self._soloMode = mode
            self._syncState()

    def _syncState(self):
        soloedChs, mutedChs, liveChs = set(), set(), set()

        for ch, state in self._stateMap.items():
            if state['solo']:
                soloedChs.add(ch)
            elif state['pause']:
                continue
            elif state['mute']:
                mutedChs.add(ch)
            else:
                liveChs.add(ch)

        if len(soloedChs) == 0:
            self._stateCh.t_put({'liveChs': liveChs, 'mutedChs': mutedChs})
        elif self._soloMode == 'pause':
            self._stateCh.t_put({'liveChs': soloedChs, 'mutedChs': set()})
        elif self._soloMode == 'mute':
            self._stateCh.t_put({'liveChs': soloedChs,
                                 'mutedChs': mutedChs.union(liveChs)})

    def _proc(self, toCh):
        liveChs, mutedChs = set(), set()
        while True:
            dataChs = list(liveChs.union(mutedChs))
            random.shuffle(dataChs)
            val, ch = alts([self._stateCh, *dataChs], priority=True)
            if ch is self._stateCh:
                liveChs, mutedChs = val['liveChs'], val['mutedChs']
            elif val is None:
                with self._lock:
                    self._stateMap.pop(ch, None)
                liveChs.discard(ch)
                mutedChs.discard(ch)
            elif ch in mutedChs:
                pass
            elif not toCh.t_put(val):
                break


def mix(ch):
    return Mix(ch)
