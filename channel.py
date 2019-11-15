import random
import threading
from collections import deque, OrderedDict
from genericfuncs import multiArity, isReduced, Reduced, unreduced
from toolz import identity


class Promise:
    def __init__(self):
        self._lock = threading.Lock()
        self._value = None
        self._completeStatus = None
        self._valueSet = threading.Condition(self._lock)
        self._complete = threading.Condition(self._lock)

    def put(self, item):
        if item is None:
            raise TypeError('item cannot be None')
        with self._lock:
            if self._value is not None:
                return False
            self._value = item
            self._valueSet.notify_all()
            self._complete.wait_for(lambda: self._completeStatus is not None)
            return self._completeStatus == 'consumed'

    def get(self):
        with self._lock:
            self._valueSet.wait_for(lambda: (self._value is not None or
                                             self._completeStatus is not None))
            if self._completeStatus == 'canceled':
                return None
            self._completeStatus = 'consumed'
            self._complete.notify()
            return self._value

    def close(self):
        with self._lock:
            self._completeStatus = self._completeStatus or 'canceled'
            self._valueSet.notify_all()
            self._complete.notify_all()


class FixedBuffer:
    def __init__(self, maxsize):
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise ValueError('maxsize must be a positive int')
        self._maxsize = maxsize
        self._deque = deque()

    def peek(self):
        return self._deque[0]

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


class UnbufferedDeliverer:
    def put(self, deliver, getWaiters, item):
        while len(getWaiters) > 0:
            prom, _ = getWaiters.popitem(last=False)
            if deliver(prom, item):
                return True
        return False

    def get(self, deliver, putWaiters):
        while len(putWaiters) > 0:
            prom, item = putWaiters.popitem(last=False)
            if deliver(prom, True):
                return item
        return None

    def close(self, deliver, getWaiters):
        return True


class BufferedDeliverer:
    def __init__(self, buf, xform=identity):
        self._buffer = buf

        def step(_, val):
            assert val is not None
            self._buffer.put(val)

        self._bufferRf = xform(multiArity(lambda: None, lambda _: None, step))

    def put(self, deliver, getWaiters, item):
        if self._buffer.is_full():
            return False
        reduced = isReduced(self._bufferRf(None, item))
        self._deliverBufferItems(deliver, getWaiters)
        return Reduced(True) if reduced else True

    def get(self, deliver, putWaiters):
        if len(self._buffer) == 0:
            return None
        getItem = self._buffer.get()

        # Transfer pending put items into buffer
        while len(putWaiters) > 0 and not self._buffer.is_full():
            prom, putItem = putWaiters.popitem(last=False)
            if deliver(prom, True):
                if isReduced(self._bufferRf(None, putItem)):
                    return Reduced(getItem)

        return getItem

    def close(self, deliver, getWaiters):
        self._bufferRf(None)
        self._deliverBufferItems(deliver, getWaiters)
        return len(self._buffer) == 0

    def _deliverBufferItems(self, deliver, getWaiters):
        while len(getWaiters) > 0 and len(self._buffer) > 0:
            prom, _ = getWaiters.popitem(last=False)
            if deliver(prom, self._buffer.peek()):
                self._buffer.get()


class PENDING:
    pass


_MAX_QUEUE_SIZE = 1024


class MaxQueueSize(Exception):
    """Maximum pending operations exceeded"""


class Channel:

    def __init__(self, deliverer):
        self._deliverer = deliverer
        self._lock = threading.Lock()
        self._getWaiters = OrderedDict()
        self._putWaiters = OrderedDict()
        self._deliver = lambda prom, val: prom.put({'ch': self, 'value': val})
        self._isClosed = False

    def maybePut(self, promise, item, block=True):
        if item is None:
            raise TypeError('item cannot be None')
        with self._lock:
            if self._isClosed:
                return False
            done = self._deliverer.put(self._deliver, self._getWaiters, item)
            if isReduced(done):
                self._close()
            if unreduced(done) or not block:
                return unreduced(done)
            if len(self._putWaiters) >= _MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._putWaiters[promise] = item
            return PENDING

    def maybeGet(self, promise, block=True):
        with self._lock:
            item = self._deliverer.get(self._deliver, self._putWaiters)
            if isReduced(item):
                self._close()
            if unreduced(item) is not None:
                return unreduced(item)
            if self._isClosed:
                self._cancelGets()
            if not block or self._isClosed:
                return None
            if len(self._getWaiters) >= _MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._getWaiters[promise] = True
            return PENDING

    def cancelRequest(self, promise):
        with self._lock:
            self._getWaiters.pop(promise, None)
            self._putWaiters.pop(promise, None)

    def get(self, block=True):
        return self._commitReq('get', block)

    def put(self, item, block=True):
        return self._commitReq('put', block, item)

    def close(self):
        with self._lock:
            self._close()

    def _cancelGets(self):
        for prom in self._getWaiters.keys():
            prom.put({'ch': self, 'value': None})
        self._getWaiters.clear()

    def _close(self):
        if not self._isClosed:
            if (self._deliverer.close(self._deliver, self._getWaiters) and
                    len(self._putWaiters) == 0):
                self._cancelGets()
            self._isClosed = True

    def _commitReq(self, reqType, block, item=None):
        p = Promise()
        response = (self.maybeGet(p, block)
                    if reqType == 'get'
                    else self.maybePut(p, item, block))

        if response is not PENDING:
            return response

        secondResponse = p.get()
        return secondResponse['value']

    def __iter__(self):
        while True:
            value = self.get()
            if value is None:
                break
            yield value


class _Promise:
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
        self.is_blockable = is_blockable
        self._f = f
        self.lock_id = 0
        self.is_active = True

    def acquire(self):
        pass

    def release(self):
        pass

    def commit(self):
        return self._f


class Chan:
    def __init__(self, buf=None):
        self._buf = buf
        self._takes = deque()
        self._puts = deque()
        self._is_closed = False
        self._lock = threading.Lock()

    def put(self, val):
        prom = _Promise()
        ret = self._put(FnHandler(prom.deliver, True), val)
        if ret is not None:
            return ret[0]
        return prom.deref()

    def get(self):
        prom = _Promise()
        ret = self._get(FnHandler(prom.deliver, True))
        if ret is not None:
            return ret[0]
        return prom.deref()

    def close(self):
        with self._lock:
            self._close()

    def _put(self, handler, val):
        if val is None:
            raise TypeError('item cannot be None')
        with self._lock:
            self._cleanup()

            if self._is_closed:
                return False,

            # Attempt to transfer val into buf
            if self._buf is not None and not self._buf.is_full():
                try:
                    handler.acquire()
                    if not handler.is_active:
                        return False,
                    handler.commit()
                finally:
                    handler.release()

                self._buf.put(val)

                # Transfer vals from buf to takers
                while len(self._takes) > 0 and len(self._buf) > 0:
                    taker = self._takes.popleft()
                    taker.acquire()
                    taker_cb = None
                    if taker.is_active:
                        taker_cb = taker.commit()
                    taker.release()
                    if taker_cb is not None:
                        taker_cb(self._buf.get())

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

            # Enqueue
            if len(self._puts) >= _MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._puts.append((handler, val))

    def _get(self, handler):
        with self._lock:
            self._cleanup()

            try:
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

                    # Transfer vals from putters into buf
                    while len(self._puts) > 0 and not self._buf.is_full():
                        putter, val = self._puts.popleft()
                        putter.acquire()
                        putter_cb = None
                        if putter.is_active:
                            putter_cb = putter.commit()
                        putter.release()
                        if putter_cb is not None:
                            self._buf.put(val)
                            putter_cb(True)

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

                if self._is_closed:
                    return None,

            finally:
                self._cancel_takes_if_done()

            # Enqueue
            if len(self._takes) >= _MAX_QUEUE_SIZE:
                raise MaxQueueSize
            self._takes.append(handler)

    def _cleanup(self):
        self._takes = deque(h for h in self._takes if h.is_active)
        self._puts = deque((h, v) for h, v in self._puts if h.is_active)

    def _cancel_takes_if_done(self):
        if (self._is_closed and
                len(self._puts) == 0 and
                (self._buf is None or len(self._buf) == 0)):
            for taker in self._takes:
                taker.acquire()
                take_cb = None
                if taker.is_active:
                    take_cb = taker.commit()
                taker.release()
                if take_cb is not None:
                    take_cb(None)
            self._takes.clear()

    def _close(self):
        self._cleanup()
        if self._is_closed:
            return
        self._is_closed = True
        self._cancel_takes_if_done()

    def __iter__(self):
        while True:
            value = self.get()
            if value is None:
                break
            yield value


def UnbufferedChannel():
    return Channel(UnbufferedDeliverer())


def BufferedChannel(buf, xform=identity):
    return Channel(BufferedDeliverer(buf, xform))


def isChan(ch):
    methodNames = 'maybeGet', 'maybePut', 'get', 'put', 'close', '__iter__'
    return all(hasattr(ch, method) for method in methodNames)


def alts(ports, priority=False):
    ports = list(ports)
    if len(ports) == 0:
        raise ValueError('alts must have at least one channel operation')
    if not priority:
        random.shuffle(ports)
    inbox = Promise()
    requests = {}

    def cancelRequests():
        inbox.close()
        for ch in requests.keys():
            ch.cancelRequest(inbox)

    # Parse ports into requests
    for p in ports:
        if type(p) in [list, tuple]:
            ch, val = p
            req = {'type': 'put', 'value': val}
        else:
            ch = p
            req = {'type': 'get'}
        if requests.get(ch, req)['type'] != req['type']:
            raise ValueError('cannot get and put to same channel')
        requests[ch] = req

    # Start requests
    for ch, req in requests.items():
        if req['type'] == 'get':
            response = ch.maybeGet(inbox)
        elif req['type'] == 'put':
            response = ch.maybePut(inbox, req['value'])

        if response is not PENDING:
            cancelRequests()
            return (response, ch)

    # Cancel requests after response
    secondResponse = inbox.get()
    cancelRequests()
    return (secondResponse['value'], secondResponse['ch'])


def chan(buf=None, xform=None):
    if buf is None:
        if xform is not None:
            raise TypeError('unbuffered channels cannot have an xform')
        return UnbufferedChannel()
    newBuf = FixedBuffer(buf) if isinstance(buf, int) else buf
    return BufferedChannel(newBuf, identity if xform is None else xform)


def reduce(f, init, ch):
    result = init
    while True:
        value = ch.get()
        if value is None:
            return result
        result = f(result, value)


def ontoChan(ch, coll, close=True):
    newCh = chan()

    def thread():
        for x in coll:
            ch.put(x)
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
            val = fromCh.get()
            if val is None or not toCh.put(val):
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
                toCh.put(val)
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
            item = self._srcCh.get()
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
            self._stateCh.put({'liveChs': liveChs, 'mutedChs': mutedChs})
        elif self._soloMode == 'pause':
            self._stateCh.put({'liveChs': soloedChs, 'mutedChs': set()})
        elif self._soloMode == 'mute':
            self._stateCh.put({'liveChs': soloedChs,
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
            elif not toCh.put(val):
                break


def mix(ch):
    return Mix(ch)
