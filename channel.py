import threading
from collections import deque
from genericfuncs import multiArity, isReduced, Reduced, unreduced
from toolz import identity


class Atom:
    def __init__(self, value=None):
        self._value = value
        self._lock = threading.Lock()

    def get(self):
        with self._lock:
            return self._value

    def reset(self, value):
        with self._lock:
            self._value = value

    def update(self, updater):
        with self._lock:
            self._value = updater(self._value)


def _iter(ch):
    while True:
        value = ch.get()
        if value is None:
            break
        yield value


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

    def size(self):
        return len(self._deque)

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


class BufferedChannel:
    def __init__(self, buffer, xform=identity):
        self._lock = threading.Lock()
        self._notEmpty = threading.Condition(self._lock)
        self._notFull = threading.Condition(self._lock)
        self._buffer = buffer
        self._isClosed = False

        def step(_, val):
            assert val is not None
            self._buffer.put(val)

        self._rf = xform(multiArity(lambda: None, lambda _: None, step))

    def get(self, block=True):
        with self._notEmpty:
            if not block and self._buffer.isEmpty():
                return None
            self._notEmpty.wait_for((lambda: self._isClosed or
                                     not self._buffer.isEmpty()))
            if not self._buffer.isEmpty():
                item = self._buffer.get()
                if not self._buffer.isFull():
                    self._notFull.notify()
                return item
            return None

    def put(self, item, block=True):
        if item is None:
            raise TypeError('item cannot be None')
        with self._notFull:
            if not block and self._buffer.isFull():
                return False
            self._notFull.wait_for((lambda: self._isClosed or
                                    not self._buffer.isFull()))
            if self._isClosed:
                return False
            prevBufferSize = self._buffer.size()
            if isReduced(self._rf(None, item)):
                self._close()
            self._notEmpty.notify(self._buffer.size() - prevBufferSize)
            return True

    def close(self):
        with self._lock:
            self._close()

    def _close(self):
        if not self._isClosed:
            self._rf(None)
            self._isClosed = True
            self._notEmpty.notify_all()
            self._notFull.notify_all()

    def __iter__(self):
        return _iter(self)


class UnbufferedChannel:
    def __init__(self):
        self._itemCh = BufferedChannel(FixedBuffer(1))
        self._completeCh = BufferedChannel(FixedBuffer(1))
        self._consumerLock = threading.Lock()
        self._producerLock = threading.Lock()
        self._isConsumerWaiting = Atom(False)

    def get(self, block=True):
        with self._consumerLock:
            self._isConsumerWaiting.reset(block)
            item = self._itemCh.get(block=block)
            self._isConsumerWaiting.reset(False)
            if item is None or not self._completeCh.put('consumer finished'):
                return None
            return item

    def put(self, item, block=True):
        if item is None:
            raise TypeError('item cannot be None')
        with self._producerLock:
            if not block and not self._isConsumerWaiting.get():
                return False
            self._itemCh.put(item)
            return self._completeCh.get() is not None

    def close(self):
        self._itemCh.close()
        self._completeCh.close()

    def __iter__(self):
        return _iter(self)


class PENDING:
    pass


class UnbufferedDeliverer:
    def put(self, deliver, getWaiters, item):
        while len(getWaiters) > 0:
            getWaiter = getWaiters.popleft()
            if deliver(getWaiter, item):
                return True
        return False

    def get(self, deliver, putWaiters):
        while len(putWaiters) > 0:
            putWaiter = putWaiters.popleft()
            if deliver(putWaiter, True):
                return putWaiter['value']
        return None

    def close(self, ch, getWaiters):
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
        item = self._buffer.get()

        # Transfer pending put items into buffer
        while len(putWaiters) > 0 and not self._buffer.isFull():
            putWaiter = putWaiters.popleft()
            if deliver(putWaiter, True):
                if isReduced(self._bufferRf(None, putWaiter['value'])):
                    return Reduced(item)

        return item

    def close(self, deliver, getWaiters):
        self._bufferRf(None)
        self._deliverBufferItems(deliver, getWaiters)
        return self._buffer.isEmpty()

    def _deliverBufferItems(self, deliver, getWaiters):
        while len(getWaiters) > 0 and not self._buffer.isEmpty():
            getWaiter = getWaiters.popleft()
            if deliver(getWaiter, self._buffer.peek()):
                self._buffer.get()


class Channel:
    def __init__(self, deliverer):
        self._deliverer = deliverer
        self._lock = threading.Lock()
        self._putWaiters = deque()
        self._getWaiters = deque()
        self._deliver = lambda wait, val: wait['ch'].put({'ch': self,
                                                          'value': val})
        self._isClosed = False

    def maybePut(self, ch, item, block=True):
        if item is None:
            raise TypeError('item cannot be None')
        with self._lock:
            if self._isClosed:
                return False
            success = self._deliverer.put(self._deliver, self._getWaiters, item)
            if isReduced(success):
                self._close()
            if unreduced(success) or not block:
                return unreduced(success)
            self._putWaiters.append({'ch': ch, 'value': item})
            return PENDING

    def maybeGet(self, ch, block=True):
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
            self._getWaiters.append({'ch': ch, 'value': True})
            return PENDING

    def get(self, block=True):
        return self._commitReq('get', block)

    def put(self, item, block=True):
        return self._commitReq('put', block, item)

    def close(self):
        with self._lock:
            self._close()

    def _cancelGets(self):
        for getWaiter in self._getWaiters:
            getWaiter['ch'].put({'ch': self, 'value': None})
        self._getWaiters.clear()

    def _close(self):
        if not self._isClosed:
            for putWaiter in self._putWaiters:
                putWaiter['ch'].put({'ch': self, 'value': False})
            self._putWaiters.clear()
            if self._deliverer.close(self._deliver, self._getWaiters):
                self._cancelGets()
            self._isClosed = True

    def _commitReq(self, reqType, block, item=None):
        ch = UnbufferedChannel()
        response = (self.maybeGet(ch, block)
                    if reqType == 'get'
                    else self.maybePut(ch, item, block))

        if response is not PENDING:
            return response

        secondResponse = ch.get()
        ch.close()
        return secondResponse['value']

    def __iter__(self):
        return _iter(self)


def MaybeUnbufferedChannel():
    return Channel(UnbufferedDeliverer())


def MaybeBufferedChannel(buf, xform=identity):
    return Channel(BufferedDeliverer(buf, xform))


def alts(ports):
    inputCh = UnbufferedChannel()
    requests = {}

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
            response = ch.maybeGet(inputCh)
        elif req['type'] == 'put':
            response = ch.maybePut(inputCh, req['value'])

        if response is not PENDING:
            inputCh.close()
            return (response, ch)

    # Wait for second response
    secondResponse = inputCh.get()
    inputCh.close()
    return (secondResponse['value'], secondResponse['ch'])


def chan(buf=None, xform=None):
    if buf is None:
        if xform is not None:
            raise TypeError('unbuffered channels cannot have an xform')
        return MaybeUnbufferedChannel()
    newBuf = FixedBuffer(buf) if isinstance(buf, int) else buf
    return MaybeBufferedChannel(newBuf, identity if xform is None else xform)


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

    threading.Thread(target=thread).start()
    return newCh


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
    threading.Thread(target=thread).start()
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

    threading.Thread(target=thread).start()
    return toCh


class Mult:
    def __init__(self, ch):
        self._srcCh = ch
        self._consumers = {}
        self._lock = threading.Lock()
        threading.Thread(target=self._proc).start()

    def tap(self, ch, close=True):
        with self._lock:
            self._consumers[ch] = close

    def untap(self, ch):
        with self._lock:
            try:
                del self._consumers[ch]
            except KeyError:
                pass

    def _copy_consumers(self):
        with self._lock:
            return dict(self._consumers)

    def _proc(self):
        while True:
            # Get next item to distribute. Close consumers when srcCh closes.
            item = self._srcCh.get()
            if item is None:
                for consumer, close in self._copy_consumers().items():
                    if close:
                        consumer.close()
                break

            # Distribute item to consumers
            threads = []
            for consumer in self._copy_consumers():
                threads.append(threading.Thread(target=consumer.put,
                                                args=[item]))
                threads[-1].start()
            for thread in threads:
                thread.join()


def mult(ch):
    return Mult(ch)
