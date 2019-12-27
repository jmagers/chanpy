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

import asyncio
import contextlib
import random
import threading
from collections import deque
from numbers import Number
from . import _buffers as bufs
from . import transducers as xf


__all__ = ['chan', 'alt', 'b_alt', 'QueueSizeError']


MAX_QUEUE_SIZE = 1024


class QueueSizeError(Exception):
    """Maximum pending channel operations exceeded.

    Raised when too many operations have been enqueued on a channel.
    Consider using a windowing buffer to prevent enqueuing too many puts or
    altering your design to have less asynchronous "processes" access the
    channel at once.

    Note:
        This exception is an indication of a design error. It should NOT be
        caught and discarded.
    """


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


class FlagFuture(asyncio.Future):
    def __init__(self, flag):
        self.__flag = flag
        self.__result = None
        super().__init__(loop=asyncio.get_running_loop())

    def set_result(self, result):
        raise AssertionError('cannot call set_result on a future provided by '
                             'a channel')

    def set_exception(self, exception):
        raise AssertionError('cannot call set_exception on a future provided '
                             'by a channel')

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


def future_deliver_fn(future):
    def set_result(result):
        try:
            asyncio.Future.set_result(future, result)
        except asyncio.InvalidStateError:
            assert future.result() is result

    def deliver(result):
        future._FlagFuture__result = result
        future.get_loop().call_soon_threadsafe(set_result, result)

    return deliver


def create_flag():
    return {'lock': threading.Lock(), 'is_active': True}


class HandlerManagerMixin:
    def __enter__(self):
        return self.acquire()

    def __exit__(self, e_type, e_val, traceback):
        self.release()


class FnHandler(HandlerManagerMixin):
    def __init__(self, cb, is_waitable=True):
        self._cb = cb
        self.is_waitable = is_waitable
        self.lock_id = 0
        self.is_active = True

    def acquire(self):
        return True

    def release(self):
        pass

    def commit(self):
        return self._cb


class FlagHandler(HandlerManagerMixin):
    def __init__(self, flag, cb, is_waitable=True):
        self._flag = flag
        self._cb = cb
        self.is_waitable = is_waitable
        self.lock_id = id(flag)

    @property
    def is_active(self):
        return self._flag['is_active']

    def acquire(self):
        return self._flag['lock'].acquire()

    def release(self):
        self._flag['lock'].release()

    def commit(self):
        self._flag['is_active'] = False
        return self._cb


@contextlib.contextmanager
def acquire_handlers(*handlers):
    """Returns a context manager for acquiring `handlers` without deadlock."""

    # Acquire locks in consistent order
    for h in sorted(handlers, key=lambda h: h.lock_id):
        is_acquired = h.acquire()
        assert is_acquired

    try:
        yield True
    finally:
        for h in handlers:
            h.release()


def nop_ex_handler(e):
    raise e


class chan:
    """A CSP channel with optional buffer, transducer, and exception handler.

    Channels support multiple producers and consumers and may be buffered or
    unbuffered. Additionally, buffered channels can optionally have a
    transformation applied to the values put to them through the use of a
    :any:`transducer`.

    Channels may be used by threads with or without a running asyncio event
    loop. The :meth:`get`, :meth:`put`, and :func:`alt` functions provide
    direct support for asyncio by returning awaitables. Channels additionally
    can be used as asynchronous generators when used with ``async for``.
    :meth:`b_get`, :meth:`b_put`, :func:`b_alt`, and :meth:`to_iter` provide
    blocking alternatives for threads which do not wish to use asyncio.
    Channels can even be used with callback based code via :meth:`f_put` and
    :meth:`f_get`. A very valuable feature of channels is that producers and
    consumers of them need not be of the same type. For example, a value placed
    onto a channel with :meth:`put` (asyncio) can be taken by a call to
    :meth:`b_get` (blocking) from a separate thread.

    A select/alt feature is also available using the :func:`alt` and
    :func:`b_alt` functions. This feature allows one to attempt many operations
    on a channel at once and only have the first operation to complete actually
    committed.

    Once closed, future puts will be unsuccessful but any pending puts will
    remain until consumed or until a :any:`reduced` value is returned from
    the transformation. Once exhausted, all future gets will complete with
    the value None. Because of this, None cannot be put onto a channel either
    directly or indirectly through a transformation.

    Args:
        buf_or_n: An optional buffer that may be expressed as a positive number.
            If it's a number, a fixed buffer of that capacity will be used.
            If None, the channel will be unbuffered.
        xform: An optional :any:`transducer <transducers>` for transforming
            elements put onto the channel. `buf_or_n` must not be None if this
            is provided.
        ex_handler: An optional function to handle exceptions raised during
            transformation. Must accept the raised exception as a parameter.
            Any non-None return value will be put onto the buffer.

    See Also:
        :any:`buffer()`
        :any:`dropping_buffer()`
        :any:`sliding_buffer()`
    """
    def __init__(self, buf_or_n=None, xform=None, ex_handler=None):
        if buf_or_n is None:
            if xform is not None:
                raise TypeError('unbuffered channels cannot have an xform')
            if ex_handler is not None:
                raise TypeError('unbuffered channels cannot have an ex_handler')

        self._buf = (bufs.FixedBuffer(buf_or_n)
                     if isinstance(buf_or_n, Number)
                     else buf_or_n)
        xform = xf.identity if xform is None else xform
        ex_handler = nop_ex_handler if ex_handler is None else ex_handler
        self._takes = deque()
        self._puts = deque()
        self._is_closed = False
        self._buf_rf_is_completed = False
        self._lock = threading.Lock()

        @xform
        @xf.completing
        def xrf(_, val):
            if val is None:
                raise AssertionError('xform cannot produce None')
            self._buf.put(val)

        def ex_handler_rf(*args):
            try:
                return xrf(*args)
            except Exception as e:
                val = ex_handler(e)
                if val is not None:
                    self._buf.put(val)

        self._buf_rf = ex_handler_rf

    def put(self, val, *, wait=True):
        """Attempts to put `val` onto the channel.

        Puts will fail in the following cases:

        * the channel is already closed
        * ``wait=False`` and `val` cannot be immediately put onto the channel
        * a :any:`reduced` value is returned during transformation

        Args:
            val: A non-None value to put onto the channel.
            wait: An optional bool that if False, fails the put operation when
                it cannot complete immediately.

        Returns:
            An awaitable that will evaluate to True if `val` is accepted onto
            the channel or False if it's not.

        Raises:
            RuntimeError: If the calling thread has no running event loop.
            QueueSizeError: If the channel has too many pending put operations.
        """
        flag = create_flag()
        future = FlagFuture(flag)
        handler = FlagHandler(flag, future_deliver_fn(future), wait)
        ret = self._p_put(handler, val)
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
        return future

    def get(self, *, wait=True):
        """Attempts to take a value from the channel.

        Gets will fail if the channel is exhausted or if ``wait=False`` and a
        value is not immediately available.

        Args:
            wait: An optional bool that if False, fails the get operation when
                a value is not immediately available.

        Returns:
            An awaitable that evaluates to a value taken from the channel or
            None if the operation fails.

        Raises:
            RuntimeError: If the calling thread has no running event loop.
            QueueSizeError: If the channel has too many pending get operations.
        """
        flag = create_flag()
        future = FlagFuture(flag)
        handler = FlagHandler(flag, future_deliver_fn(future), wait)
        ret = self._p_get(handler)
        if ret is not None:
            asyncio.Future.set_result(future, ret[0])
        return future

    def b_put(self, val, *, wait=True):
        """Same as :meth:`put` except it blocks instead of returning an awaitable.

        Does not require an event loop.
        """
        prom = Promise()
        ret = self._p_put(FnHandler(prom.deliver, wait), val)
        if ret is not None:
            return ret[0]
        return prom.deref()

    def b_get(self, *, wait=True):
        """Same as :meth:`get` except it blocks instead of returning an awaitable.

        Does not require an event loop.
        """
        prom = Promise()
        ret = self._p_get(FnHandler(prom.deliver, wait))
        if ret is not None:
            return ret[0]
        return prom.deref()

    def f_put(self, val, f=None):
        """Asynchronously puts `val` onto the channel and calls `f` when complete.

        Does not require an event loop.

        Args:
            val: A non-None value to put onto the channel.
            f: An optional non-blocking function accepting the completion
                status of the put operation.

        Returns:
            False if the channel is already closed or True if it's not.

        Raises:
            QueueSizeError: If the channel has too many pending put operations.
        """
        f = (lambda _: None) if f is None else f
        ret = self._p_put(FnHandler(f), val)
        if ret is None:
            return True
        f(ret[0])
        return ret[0]

    def f_get(self, f):
        """Asynchronously takes a value from the channel and calls `f` with it.

        Does not require an event loop.

        Args:
            f: A non-blocking function accepting a single argument. Will be
                passed the value taken from the channel or None if the channel
                is exhausted.

        Raises:
            QueueSizeError: If the channel has too many pending get operations.
        """
        ret = self._p_get(FnHandler(f))
        if ret is None:
            return
        f(ret[0])

    def offer(self, val):
        """Same as :meth:`b_put(val, wait=False) <b_put>`."""
        return self.b_put(val, wait=False)

    def poll(self):
        """Same as :meth:`b_get(wait=False) <b_get>`."""
        return self.b_get(wait=False)

    def close(self):
        """Closes the channel."""
        with self._lock:
            self._cleanup()
            self._close()

    async def __aiter__(self):
        """Returns an asynchronous iterator over the channel's values."""
        while True:
            value = await self.get()
            if value is None:
                break
            yield value

    def to_iter(self):
        """Returns an iterator over the channel's values.

        Calling ``next()`` on the returned iterator may block.
        Does not require an event loop.
        """
        while True:
            val = self.b_get()
            if val is None:
                break
            yield val

    def _p_put(self, handler, val):
        """Commits or enqueues a put operation to the channel.

        If the put operation completes immediately, then the `handler` will be
        committed but its callback will not be invoked. The completion status
        of the operation will be wrapped in a tuple and returned. The status
        will be True if `val` was accepted onto the channel or False otherwise.

        If the operation is unable to complete immediately, then `handler` and
        `val` will be enqueued and None will be returned. When the operation
        eventually completes, the `handler` will be committed and its callback
        will be invoked with the completion status.

        Args:
            handler: A handler that will be committed upon completion. Its
                callback will only be invoked if the operation is enqueued.
            val: A non-None value to put onto the channel.

        Returns:
            A tuple containing the completion status if the operation completes
            immediately. None if the operation is enqueued.

        Raises:
            QueueSizeError: If the channel has too many pending put operations.
        """
        if val is None:
            raise TypeError('item cannot be None')
        with self._lock:
            self._cleanup()

            if self._is_closed:
                return self._fail_op(handler, False)

            # Attempt to transfer val onto buf
            if self._buf is not None and not self._buf.is_full():
                with handler:
                    if not handler.is_active:
                        return False
                    handler.commit()

                self._buf_put(val)
                self._transfer_buf_vals_to_takers()
                return True,

            # Attempt to transfer val to a taker
            if self._buf is None:
                while len(self._takes) > 0:
                    taker = self._takes.popleft()
                    with acquire_handlers(handler, taker):
                        if handler.is_active and taker.is_active:
                            handler.commit()
                            taker.commit()(val)
                            return True,

            if not handler.is_waitable:
                return self._fail_op(handler, False)

            # Attempt to enqueue the operation
            if len(self._puts) >= MAX_QUEUE_SIZE:
                raise QueueSizeError('channel has too many pending puts')
            self._puts.append((handler, val))

    def _p_get(self, handler):
        """Commits or enqueues a get operation to the channel.

        If the get operation completes immediately, then the `handler` will be
        committed but its callback will not be invoked. If the channel is not
        already exhausted, then the value taken from the channel will be
        wrapped in a tuple and returned. If the channel is already exhausted
        then the tuple, ``(None,)``, will be returned.

        If the operation is unable to complete immediately, then `handler` and
        `val` will be enqueued and None will be returned. When the operation
        eventually completes, the `handler` will be committed and its callback
        will be invoked with the value taken from the channel or None if its
        exhausted.

        Args:
            handler: A handler that will be committed upon completion. Its
                callback will only be invoked if the operation is enqueued.

        Returns:
            A tuple containing the result of the get operation if it completes
            immediately. None if the operation is enqueued.

        Raises:
            QueueSizeError: If the channel has too many pending get operations.
        """
        with self._lock:
            self._cleanup()

            # Attempt to take val from buf
            if self._buf is not None and len(self._buf) > 0:
                with handler:
                    if not handler.is_active:
                        return None,
                    handler.commit()

                ret = self._buf.get()

                # Transfer vals from putters onto buf
                while len(self._puts) > 0 and not self._buf.is_full():
                    putter, val = self._puts.popleft()
                    with putter:
                        if putter.is_active:
                            putter.commit()(True)
                            self._buf_put(val)

                self._complete_buf_rf_if_ready()
                return ret,

            # Attempt to take val from a putter
            if self._buf is None:
                while len(self._puts) > 0:
                    putter, val = self._puts.popleft()
                    with acquire_handlers(handler, putter):
                        if handler.is_active and putter.is_active:
                            handler.commit()
                            putter.commit()(True)
                            return val,

            if self._is_closed or not handler.is_waitable:
                return self._fail_op(handler, None)

            # Attempt to enqueue the operation
            if len(self._takes) >= MAX_QUEUE_SIZE:
                raise QueueSizeError('channel has too many pending gets')
            self._takes.append(handler)

    def _cleanup(self):
        """Removes enqueued operations that are no longer active."""
        self._takes = deque(h for h in self._takes if h.is_active)
        self._puts = deque((h, v) for h, v in self._puts if h.is_active)

    @staticmethod
    def _fail_op(handler, val):
        with handler:
            if handler.is_active:
                handler.commit()
                return val,

    def _buf_put(self, val):
        if xf.is_reduced(self._buf_rf(None, val)):
            # If reduced value is returned then no more input is allowed onto
            # buf. To ensure this, remove all pending puts and close ch.
            for putter, _ in self._puts:
                with putter:
                    if putter.is_active:
                        putter.commit()(False)
            self._puts.clear()
            self._close()

    def _transfer_buf_vals_to_takers(self):
        while len(self._takes) > 0 and len(self._buf) > 0:
            taker = self._takes.popleft()
            with taker:
                if taker.is_active:
                    taker.commit()(self._buf.get())

    def _complete_buf_rf_if_ready(self):
        """Calls buf_rf completion arity once if all input has been put to buf.

        Guarantees that the buf_rf completion arity will be invoked only after
        all input has been placed onto the buffer and that it will never be
        called more than once. Invoking the completion arity will flush any
        remaining values from the transformed reducing function onto buf.
        """
        if (self._is_closed and
                len(self._puts) == 0 and
                not self._buf_rf_is_completed):
            self._buf_rf_is_completed = True
            self._buf_rf(None)

    def _close(self):
        self._is_closed = True

        if self._buf is not None:
            self._complete_buf_rf_if_ready()
            self._transfer_buf_vals_to_takers()

        # Remove pending takes
        # No-op if there are pending puts or buffer isn't empty
        for taker in self._takes:
            with taker:
                if taker.is_active:
                    taker.commit()(None)
        self._takes.clear()


class _Undefined:
    """A default parameter value that a user could never pass in."""


def _alts(flag, deliver_fn, ops, priority, default):
    ops = list(ops)
    if len(ops) == 0:
        raise ValueError('alts must have at least one channel operation')
    if not priority:
        random.shuffle(ops)

    ch_ops = {}

    # Parse ops into ch_ops
    for raw_op in ops:
        try:
            ch, val = raw_op
            op = {'type': 'put', 'value': val}
        except TypeError:
            ch = raw_op
            op = {'type': 'get'}
        if ch_ops.get(ch, op)['type'] != op['type']:
            raise ValueError('cannot get and put to same channel')
        ch_ops[ch] = op

    def create_handler(ch):
        return FlagHandler(flag, lambda val: deliver_fn((val, ch)))

    # Start ops
    for ch, op in ch_ops.items():
        if op['type'] == 'get':
            ret = ch._p_get(create_handler(ch))
        elif op['type'] == 'put':
            ret = ch._p_put(create_handler(ch), op['value'])
        if ret is not None:
            return ret[0], ch

    if default is not _Undefined:
        with flag['lock']:
            if flag['is_active']:
                flag['is_active'] = False
                return default, 'default'


def alt(*ops, priority=False, default=_Undefined):
    """
    alt(*ops, priority=False, default=Undefined)

    Returns an awaitable representing the first and only channel operation to finish.

    Accepts a variable number of operations that either get from or put to a
    channel and commits only one of them. If no `default` is provided, then
    only the first op to finish will be committed. If `default` is provided and
    none of the `ops` finish immediately, then no operation will be committed
    and `default` will instead be used to complete the returned awaitable.

    Args:
        ops: Operations that either get from or put to a channel.
            A get operation is represented as simply a channel to get from.
            A put operation is represented as an iterable of the form
            ``[channel, val]``, where `val` is an item to put onto `channel`.
        priority: An optional bool. If True, operations will be tried in order.
            If False, operations will be tried in random order.
        default: An optional value to use in case no operation finishes
            immediately.

    Returns:
        An awaitable that evaluates to a tuple of the form ``(val, ch)``.
        If `default` is not provided, then `val` will be what the first
        successful operation returned and `ch` will be the channel used in that
        operation. If `default` is provided and none of the operations complete
        immediately, then the awaitable will evaluate to
        ``(default, 'default')``.

    Raises:
        ValueError: If `ops` is empty or contains both a get and put operation
            to the same channel.
        RuntimeError: If the calling thread has no running event loop.

    See Also:
        :func:`b_alt`
    """
    flag = create_flag()
    future = FlagFuture(flag)
    ret = _alts(flag, future_deliver_fn(future), ops, priority, default)
    if ret is not None:
        asyncio.Future.set_result(future, ret)
    return future


def b_alt(*ops, priority=False, default=_Undefined):
    """
    b_alt(*ops, priority=False, default=Undefined)

    Same as :func:`alt` except it blocks instead of returning an awaitable.

    Does not require an event loop.
    """
    prom = Promise()
    ret = _alts(create_flag(), prom.deliver, ops, priority, default)
    return prom.deref() if ret is None else ret
