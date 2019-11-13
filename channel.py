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

    def isEmpty(self):
        return len(self._deque) == 0

    def isFull(self):
        return len(self._deque) >= self._maxsize


class DroppingBuffer(FixedBuffer):
    def put(self, item):
        if len(self._deque) < self._maxsize:
            self._deque.append(item)

    def isFull(self):
        return False


class SlidingBuffer(FixedBuffer):
    def put(self, item):
        self._deque.append(item)
        if len(self._deque) > self._maxsize:
            self._deque.popleft()

    def isFull(self):
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
        if self._buffer.isFull():
            return False
        reduced = isReduced(self._bufferRf(None, item))
        self._deliverBufferItems(deliver, getWaiters)
        return Reduced(True) if reduced else True

    def get(self, deliver, putWaiters):
        if self._buffer.isEmpty():
            return None
        getItem = self._buffer.get()

        # Transfer pending put items into buffer
        while len(putWaiters) > 0 and not self._buffer.isFull():
            prom, putItem = putWaiters.popitem(last=False)
            if deliver(prom, True):
                if isReduced(self._bufferRf(None, putItem)):
                    return Reduced(getItem)

        return getItem

    def close(self, deliver, getWaiters):
        self._bufferRf(None)
        self._deliverBufferItems(deliver, getWaiters)
        return self._buffer.isEmpty()

    def _deliverBufferItems(self, deliver, getWaiters):
        while len(getWaiters) > 0 and not self._buffer.isEmpty():
            prom, _ = getWaiters.popitem(last=False)
            if deliver(prom, self._buffer.peek()):
                self._buffer.get()


class PENDING:
    pass


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
            for prom in self._putWaiters.keys():
                prom.put({'ch': self, 'value': False})
            self._putWaiters.clear()
            if self._deliverer.close(self._deliver, self._getWaiters):
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


def UnbufferedChannel():
    return Channel(UnbufferedDeliverer())


def BufferedChannel(buf, xform=identity):
    return Channel(BufferedDeliverer(buf, xform))


def isChan(ch):
    methodNames = 'maybeGet', 'maybePut', 'get', 'put', 'close', '__iter__'
    return all(hasattr(ch, method) for method in methodNames)


def alts(ports, priority=False):
    if not priority:
        ports = list(ports)
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
        self._stateMapCh = chan(SlidingBuffer(1))
        self._stateMap = {}
        self._lock = threading.Lock()
        threading.Thread(target=self._proc, args=[toCh], daemon=True).start()

    def toggle(self, stateMap):
        with self._lock:
            for ch, state in stateMap.items():
                if not isChan(ch):
                    raise ValueError(f'stateMap key is not a channel: {state}')
                if not set(state.keys()).issubset({'pause', 'mute'}):
                    raise ValueError(f'state contains invalid options: '
                                     f'{state}')
                if not set(state.values()).issubset({True, False}):
                    raise ValueError(f'state contains non-boolean values: '
                                     f'{state}')
            for fromCh, newState in stateMap.items():
                originalState = self._stateMap.get(ch, {'pause': False,
                                                        'mute': False})
                self._stateMap[fromCh] = {**originalState, **newState}
            self._stateMapCh.put(dict(self._stateMap))

    def admix(self, ch):
        self.toggle({ch: {}})

    def unmix(self, ch):
        with self._lock:
            self._stateMap.pop(ch, None)
            self._stateMapCh.put(dict(self._stateMap))

    def _proc(self, toCh):
        stateMap = {}
        while True:
            nonPausedChs = [ch for ch, state in stateMap.items()
                            if not state['pause']]
            val, ch = alts([self._stateMapCh, *nonPausedChs])
            if ch is self._stateMapCh:
                stateMap = val
            elif val is None:
                with self._lock:
                    self._stateMap.pop(ch, None)
                stateMap.pop(ch)
            elif stateMap[ch]['mute']:
                pass
            elif not toCh.put(val):
                break


def mix(ch):
    return Mix(ch)
