import asyncio as _asyncio
import contextlib as _contextlib
import random as _random
import threading as _threading
from . import buffers as _bufs
from .channel import chan, alts, b_alts, alt, b_alt, QueueSizeExceeded
from . import xf


class _Undefined:
    """A default parameter value that a user could never pass in."""


def buffer(n):
    """Returns a fixed buffer with a capacity of n.

    Puts to channels with this buffer will block if capacity is reached.

    Args:
        n: A positive number.
    """
    return _bufs.FixedBuffer(n)


def dropping_buffer(n):
    """Returns a windowing buffer with a capacity of n.

    Puts to channels with this buffer will appear successful after the capacity
    is reached but will not be added to the buffer.

    Args:
        n: A positive number.
    """
    return _bufs.DroppingBuffer(n)


def sliding_buffer(n):
    """Returns a windowing buffer with a capacity of n.

    Puts to channels with this buffer will complete successfully after the
    capacity is reached but will evict the oldest element in the buffer.

    Args:
        n: A positive number.
    """
    return _bufs.SlidingBuffer(n)


def is_unblocking_buffer(buf):
    """Returns True if puts to buf will never block."""
    return isinstance(buf, _bufs.UnblockingBufferMixin)


def promise_chan(xform=None, ex_handler=None):
    """Returns a channel that emits the same value forever.

    Creates a channel with an optional transducer and exception handler that
    always returns the same value to consumers. The value emitted will be the
    first item put onto the channel or None if the channel was closed before
    the first put.

    Args:
        xform: An optional transducer. See chan().
        ex_handler: An optional exception handler. See chan().
    """
    return chan(_bufs.PromiseBuffer(), xform, ex_handler)


def is_chan(ch):
    """Returns True if ch is a channel."""
    return isinstance(ch, chan)


# Thread local data
_local_data = _threading.local()


def get_loop():
    """Returns the event loop for the current thread.

    If set_loop() has been used to register an event loop to the current thread
    then that loop will be returned. If no such event loop exists then returns
    the running loop in the current thread.

    Raises:
        RuntimeError: If no event loop has been registered and no loop is
        running in the current thread.

    See Also:
        set_loop
    """
    loop = getattr(_local_data, 'loop', None)
    return _asyncio.get_running_loop() if loop is None else loop


def set_loop(loop):
    """Registers the event loop for the current thread.

    Returns: A context manager that on exit will unregister loop and reregister
        the event loop that was originally set before set_loop was invoked.
    """
    prev_loop = getattr(_local_data, 'loop', None)
    _local_data.loop = loop

    @_contextlib.contextmanager
    def context_manager():
        try:
            yield
        finally:
            _local_data.loop = prev_loop

    return context_manager()


def _in_loop(loop):
    """Returns True if loop is the running event loop for the current thread."""
    try:
        return _asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


def thread_call(f, executor=None):
    """Registers current loop to a separate thread and then calls f from it.

    Calls f in another thread, returning immediately to the calling thread.
    The separate thread will have the loop from the calling thread registered
    to it while f runs.

    Args:
        f: A function accepting no arguments.
        executor: An optional ThreadPoolExecutor to submit f to.

    Returns: A channel that will emit the return value of f exactly once.
    """
    loop = get_loop()
    ch = chan(1)

    def wrapper():
        with set_loop(loop):
            ret = f()
            if ret is not None:
                ch.b_put(ret)
            ch.close()

    if executor is None:
        _threading.Thread(target=wrapper).start()
    else:
        executor.submit(wrapper)
    return ch


def to_iter(ch):
    """Returns an iterator over the values from the provided channel."""
    while True:
        val = ch.b_get()
        if val is None:
            break
        yield val


def go(coro):
    """Adds a coroutine as a task to the current event loop.

    Args:
        coro: A coroutine.

    Returns: A channel that will emit the return value of coro exactly once.
    """
    loop = get_loop()
    ch = chan(1)

    def create_tasks():
        # Note: coro and put_result_to_ch coroutines need to be added
        # separately or else a 'coroutine never awaited' RuntimeWarning could
        # get raised when call_soon_threadsafe is used

        coro_task = _asyncio.create_task(coro)

        async def put_result_to_ch():
            ret = await coro_task
            if ret is not None:
                await ch.put(ret)
            ch.close()

        loop.create_task(put_result_to_ch())

    if _in_loop(loop):
        create_tasks()
    else:
        loop.call_soon_threadsafe(create_tasks)

    return ch


def timeout(msecs):
    """Returns a channel that closes after given milliseconds."""
    ch = chan()
    get_loop().call_later(msecs / 1000, ch.close)
    return ch


# TODO: Create goroutine decorator called goro
def _reduce(rf, init, ch):
    async def proc():
        result = init
        async for val in ch:
            result = rf(result, val)
            if xf.is_reduced(result):
                break
        return xf.unreduced(result)

    return go(proc())


def reduce(rf, init, ch=_Undefined):
    """
    reduce(rf, ch) -> result_ch
    reduce(rf, init, ch) -> result_ch

    Asynchronously reduces a channel.

    Asynchronously collects values from ch and reduces them using rf, placing
    the final result in the returned channel. If ch is exhausted then init will
    be used as the result. If ch is not exhausted then the first call to rf
    will be rf(init, val) where val is taken from ch. rf will continue to get
    called as rf(prev_return, next_ch_val) until either ch is exhausted or rf
    returns a reduced value.

    Args:
        rf: A reducing function accepting 2 args. If init is not provided then
            rf must return a value to be used as init when called with 0 args.
        init: An optional initial value.
        ch: A channel to get values from.

    Returns: A channel containing the result of the reduction.

    See Also:
        transduce
    """
    if ch is _Undefined:
        return _reduce(rf, rf(), init)
    return _reduce(rf, init, ch)


def _transduce(xform, rf, init, ch):
    async def proc():
        xrf = xform(rf)
        ret = await reduce(xrf, init, ch).get()
        return xrf(ret)

    return go(proc())


def transduce(xform, rf, init, ch=_Undefined):
    """
    transduce(xform, rf, ch) -> result_ch
    transduce(xform, rf, init, ch) -> result_ch

    Asynchronously reduces a channel with a transformation.

    Asynchronously collects values from ch and reduces them using a transformed
    reducing function equal to xform(rf). See reduce() for more information on
    reduction. After the transformed reducing function has received all input
    it will be called once more with a single argument, the result thus far.

    Args:
        xform: A transducer.
        rf: A reducing function accepting both 1 and 2 arguments. If init is
            not provided then rf must return a value to be used as init when
            called. with 0 arguments.
        init: An optional initial value.
        ch: A channel to get values from.

    Returns: A channel containing the result of the reduction.

    See Also:
        reduce
    """
    if ch is _Undefined:
        return _transduce(xform, rf, rf(), init)
    return _transduce(xform, rf, init, ch)


def to_list(ch):
    """Asynchronously reduces the values from ch to a list.

    Returns: A channel containing a list of values from ch.
    """
    return reduce(xf.append, ch)


def onto_chan(ch, coll, *, close=True):
    """Asynchronously transfers values from an iterable to a channel.

    Args:
        ch: A channel to put values onto.
        coll: An iterable to get values from.
        close: An optional bool. ch will be closed if true.

    Returns: A channel that closes when transfer is finished.
    """
    async def proc():
        for x in coll:
            await ch.put(x)
        if close:
            ch.close()

    return go(proc())


def to_chan(coll):
    """Returns a channel that emits all values from coll and then closes.

    Args:
        coll: An iterable to get values from.
    """
    ch = chan()
    onto_chan(ch, coll)
    return ch


def pipe(from_ch, to_ch, *, close=True):
    """Asynchronously transfers all values from from_ch to to_ch.

    Args:
        from_ch: A channel to get values from.
        to_ch: A channel to put values onto.
        close: An optional bool. If true, to_ch will be closed after transfer
            finishes.

    Returns: to_ch.
    """
    async def proc():
        async for val in from_ch:
            if not await to_ch.put(val):
                break
        if close:
            to_ch.close()

    go(proc())
    return to_ch


def merge(chs, buf_or_n=None):
    """Returns a channel that emits values from the provided source channels.

    Transfers all values from chs onto the returned channel. The returned
    channel closes after the transfer completes.

    Args:
        chs: An iterable of source channels.
        buf_or_n: An optional buffer or int >= 0. See chan().
    """
    to_ch = chan(buf_or_n)

    async def proc():
        ports = set(chs)
        while len(ports) > 0:
            val, ch = await alts(ports)
            if val is None:
                ports.remove(ch)
            else:
                await to_ch.put(val)
        to_ch.close()

    go(proc())
    return to_ch


class mult:
    """A mult(iple) of the source ch that puts each of its values to its taps.

    tap() can be used to subscribe a channel to the mult and therefore receive
    copies of the values from ch. Taps can later be unsubscribed using untap()
    or untap_all().

    No tap will receive the next value from ch until all taps have accepted the
    current value. If no tap exists, values will still be consumed from ch but
    will be discarded.

    Args:
        ch: A channel to get values form.
    """

    def __init__(self, ch):
        self._lock = _threading.Lock()
        self._from_ch = ch
        self._taps = {}  # ch->close
        self._is_closed = False
        go(self._proc())

    def tap(self, ch, *, close=True):
        """Subscribes a channel as a consumer of the mult.

        Args:
            ch: A channel to receive values from the mult's source channel.
            close: An optional bool. If true, ch will be closed after the
                source channel becomes exhausted.
        """
        with self._lock:
            if self._is_closed and close:
                ch.close()
            self._taps[ch] = close

    def untap(self, ch):
        """Unsubscribes a channel from the mult."""
        with self._lock:
            self._taps.pop(ch, None)

    def untap_all(self):
        """Unsubscribes all taps from the mult."""
        with self._lock:
            self._taps.clear()

    async def _proc(self):
        async for item in self._from_ch:
            with self._lock:
                chs = tuple(self._taps)
            results = await _asyncio.gather(*(ch.put(item) for ch in chs))
            with self._lock:
                for ch, is_open in zip(chs, results):
                    if not is_open:
                        self._taps.pop(ch, None)

        with self._lock:
            self._is_closed = True
            for ch, close in self._taps.items():
                if close:
                    ch.close()


class pub:
    """A pub(lication) of the source ch divided into topics.

    The values of ch will be categorized into topics defined by topic_fn.
    Each topic will be given its own mult for channels to subscribe to.
    Channels can be subscribed to a given topic with sub() and unsubscribed
    with unsub() or unsub_all().

    Args:
        ch: A channel to get values from.
        topic_fn: A function that given a value from ch returns a topic
            identifier.
        buf_fn: An optional function that given a topic returns a buffer to be
            used with that topic's mult channel.


    See Also:
        mult
    """
    def __init__(self, ch, topic_fn, buf_fn=lambda _: None):
        self._lock = _threading.Lock()
        self._from_ch = ch
        self._topic_fn = topic_fn
        self._buf_fn = buf_fn
        self._mults = {}  # topic->mult
        go(self._proc())

    def sub(self, topic, ch, *, close=True):
        """Subscribes a channel to the given topic.

        Args:
            topic: A topic identifier.
            ch: A channel to subscribe.
            close: An optional bool. If true, ch will be closed when the source
                channel is exhausted.
         """
        with self._lock:
            if topic not in self._mults:
                self._mults[topic] = mult(chan(self._buf_fn(topic)))
            self._mults[topic].tap(ch, close=close)

    def unsub(self, topic, ch):
        """"Unsubscribes a channel from the given topic."""
        with self._lock:
            m = self._mults.get(topic, None)
            if m is not None:
                m.untap(ch)
                if len(m._taps) == 0:
                    m._from_ch.close()
                    self._mults.pop(topic)

    def unsub_all(self, topic=_Undefined):
        """Unsubscribes all subs from a topic or all topics if not provided."""
        with self._lock:
            topics = tuple(self._mults) if topic is _Undefined else [topic]
            for t in topics:
                m = self._mults.get(t, None)
                if m is not None:
                    m.untap_all()
                    m._from_ch.close()
                    self._mults.pop(t)

    async def _proc(self):
        async for item in self._from_ch:
            with self._lock:
                m = self._mults.get(self._topic_fn(item), None)
            if m is not None:
                await m._from_ch.put(item)

        with self._lock:
            for m in self._mults.values():
                m._from_ch.close()
            self._mults.clear()


class mix:
    """Consumes values from each of its source channels and puts them onto ch.

    A source channel can be added with admix() and removed with unmix() or
    unmix_all().

    A source channel can be given a set of attribute flags to modify how it is
    consumed with toggle(). If a channel has its 'pause' attribute set to true
    then the mix will stop consuming from it. Else if its 'mute' attribute is
    set then the channel will still be consumed but its values discarded.

    A source channel may also be soloed by setting the 'solo' attribute. If any
    source channel is soloed then all of its other attributes will be ignored.
    Furthermore, non-soloed channels will have their attributes ignored and
    instead will take on whatever attribute has been set with solo_mode()
    (defaults to 'mute' if solo_mode() hasn't been invoked).

    Args:
        ch: A channel to put values onto.
    """
    def __init__(self, ch):
        self._lock = _threading.Lock()
        self._to_ch = ch
        self._state_ch = chan(sliding_buffer(1))
        self._state_map = {}  # ch->state
        self._solo_mode = 'mute'
        go(self._proc())

    def toggle(self, state_map):
        """Merges state_map with the current state of the mix.

        The provided state_map is used to update the attributes of the mix's
        source channels. state_map will be merged with the current state of the
        mix. If state_map contains a channel that is not currently in the mix
        then that channel will be added to the mix with the corresponding
        attributes in state_map.

        Args:
            state_map: A dictionary of the form ch->attribute_map where
                attribute_map is a dictionary of the form attribute->bool.
                See mix for a list of supported attributes and corresponding
                behaviors.
        """
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

        with self._lock:
            for ch, new_state in state_map.items():
                original_state = self._state_map.get(ch, {'solo': False,
                                                          'pause': False,
                                                          'mute': False})
                self._state_map[ch] = {**original_state, **new_state}
            self._sync_state()

    def admix(self, ch):
        """Adds ch as a source channel of the mix."""
        self.toggle({ch: {}})

    def unmix(self, ch):
        """Removes ch from the set of source channels."""
        with self._lock:
            self._state_map.pop(ch, None)
            self._sync_state()

    def unmix_all(self):
        """Removes all source channels from the mix."""
        with self._lock:
            self._state_map.clear()
            self._sync_state()

    def solo_mode(self, mode):
        """Sets the mode for non-soloed source channels.

        For as long as there is at least one soloed channel, non-soloed
        source channels will have their attributes ignored and will instead
        take on the provided mode.

        Args:
            mode: Either 'pause' or 'mute'. See mix for behaviors.
        """
        if mode not in ['pause', 'mute']:
            raise ValueError(f'solo-mode is invalid: {mode}')

        with self._lock:
            self._solo_mode = mode
            self._sync_state()

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
            self._state_ch.b_put({'live_chs': live_chs,
                                  'muted_chs': muted_chs})
        elif self._solo_mode == 'pause':
            self._state_ch.b_put({'live_chs': soloed_chs, 'muted_chs': set()})
        elif self._solo_mode == 'mute':
            self._state_ch.b_put({'live_chs': soloed_chs,
                                  'muted_chs': muted_chs.union(live_chs)})

    async def _proc(self):
        live_chs, muted_chs = set(), set()
        while True:
            data_chs = list(live_chs.union(muted_chs))
            _random.shuffle(data_chs)
            val, ch = await alts([self._state_ch, *data_chs], priority=True)
            if ch is self._state_ch:
                live_chs, muted_chs = val['live_chs'], val['muted_chs']
            elif val is None:
                with self._lock:
                    self._state_map.pop(ch, None)
                live_chs.discard(ch)
                muted_chs.discard(ch)
            elif ch in muted_chs:
                pass
            elif not await self._to_ch.put(val):
                break


def split(pred, ch, true_buf=None, false_buf=None):
    """Splits the values of channel into 2 channels based on the predicate.

    Returns a tuple of the form (true_ch, false_ch) where true_ch contains all
    the values from ch where the predicate returns true and false_ch contains
    all the values that return false.

    Args:
        pred: A predicate function.
        ch: A channel to get values from.
        true_buf: An optional buffer to use with true_ch. See chan().
        false_buf: An optional buffer to use with false_ch. See chan().

    Returns: A tuple of the form (true_ch, false_ch).
    """
    true_ch, false_ch = chan(true_buf), chan(false_buf)

    async def proc():
        async for x in ch:
            if pred(x):
                await true_ch.put(x)
            else:
                await false_ch.put(x)
        true_ch.close()
        false_ch.close()

    go(proc())
    return true_ch, false_ch
