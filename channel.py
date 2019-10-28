import queue
import threading
from collections import deque
from toolz import complement


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

    def get(self):
        with self._notEmpty:
            self._notEmpty.wait_for(complement(self._buffer.isEmpty))
            item = self._buffer.get()
            self._notFull.notify()
            return item

    def put(self, item):
        with self._notFull:
            self._notFull.wait_for(complement(self._buffer.isFull))
            self._buffer.put(item)
            self._notEmpty.notify()


class UnbufferedChannel:
    def __init__(self):
        self._itemQ = queue.Queue(1)
        self._completeQ = queue.Queue(1)
        self._consumerLock = threading.Lock()
        self._producerLock = threading.Lock()

    def get(self):
        with self._consumerLock:
            item = self._itemQ.get()
            self._completeQ.put(None)
            return item

    def put(self, item):
        with self._producerLock:
            self._itemQ.put(item)
            self._completeQ.get()


def chan(buf=0):
    if buf == 0:
        return UnbufferedChannel()
    return BufferedChannel(FixedBuffer(buf) if isinstance(buf, int) else buf)
