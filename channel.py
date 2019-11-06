import threading
from collections import deque
from genericfuncs import multiArity, isReduced
from toolz import identity


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
            if isReduced(self._rf(None, item)):
                self._close()
            elif not self._buffer.isEmpty():
                self._notEmpty.notify()
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
        self._isConsumerWaiting = False

    def get(self, block=True):
        with self._consumerLock:
            self._isConsumerWaiting = block
            item = self._itemCh.get(block=block)
            self._isConsumerWaiting = False
            if item is None or not self._completeCh.put('consumer finished'):
                return None
            return item

    def put(self, item, block=True):
        if item is None:
            raise TypeError('item cannot be None')
        with self._producerLock:
            if not block and not self._isConsumerWaiting:
                return False
            self._itemCh.put(item)
            return self._completeCh.get() is not None

    def close(self):
        self._itemCh.close()
        self._completeCh.close()

    def __iter__(self):
        return _iter(self)


def chan(buf=None, xform=None):
    if buf is None:
        if xform is not None:
            raise ValueError('unbuffered channels cannot have an xform')
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

    threading.Thread(target=thread).start()
    return newCh


def pipe(fromCh, toCh, close=True):
    completeCh = chan()

    def thread():
        while True:
            val = fromCh.get()
            if val is None:
                completeCh.close()
                if close:
                    toCh.close()
                return
            toCh.put(val)

    threading.Thread(target=thread).start()
    return completeCh


def merge(chs, buf=None):
    toCh = chan(buf)

    def thread():
        for doneCh in [pipe(fromCh, toCh, close=False) for fromCh in chs]:
            doneCh.get()
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
