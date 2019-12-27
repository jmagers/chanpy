# Copyright 2019 Jake Magers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque
from numbers import Number


class FixedBuffer:
    def __init__(self, n):
        if not isinstance(n, Number):
            raise TypeError('n must be a positive number')
        if n <= 0:
            raise ValueError('n must be a positive number')
        self._maxsize = n
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
