import threading
from collections import deque


class Empty(Exception):
    """Empty"""


class Full(Exception):
    """Full"""


class FixedBuffer:
    def __init__(self, maxsize):
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise ValueError('maxsize must be a positive int')
        self._maxsize = maxsize
        self._q = deque()

    def get(self):
        if self.isEmpty():
            raise Empty
        return self._q.popleft()

    def put(self, item):
        if self.isFull():
            raise Full
        self._q.append(item)

    def isEmpty(self):
        return len(self._q) == 0

    def isFull(self):
        return len(self._q) >= self._maxsize


class BufferedChannel:
    def __init__(self, buffer):
        self._lock = threading.Lock()
        self._notEmpty = threading.Condition(self._lock)
        self._notFull = threading.Condition(self._lock)
        self._buffer = buffer
        self._isClosed = False

    def get(self, block=True):
        with self._notEmpty:
            if not block and self._buffer.isEmpty():
                return None
            self._notEmpty.wait_for((lambda: self._isClosed or
                                     not self._buffer.isEmpty()))
            if not self._buffer.isEmpty():
                item = self._buffer.get()
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
            self._buffer.put(item)
            self._notEmpty.notify()
            return True

    def close(self):
        with self._lock:
            self._isClosed = True
            self._notEmpty.notify_all()
            self._notFull.notify_all()


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
            self._isConsumerWaiting = False
            return self._completeCh.get() is not None

    def close(self):
        self._itemCh.close()
        self._completeCh.close()


def chan(buf=0):
    if buf == 0:
        return UnbufferedChannel()
    return BufferedChannel(FixedBuffer(buf) if isinstance(buf, int) else buf)
