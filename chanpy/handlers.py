import asyncio
import threading


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
    def __init__(self, cb, is_blockable=True):
        self._cb = cb
        self.is_blockable = is_blockable
        self.lock_id = 0
        self.is_active = True

    def acquire(self):
        return True

    def release(self):
        pass

    def commit(self):
        return self._cb


class FlagHandler(HandlerManagerMixin):
    def __init__(self, flag, cb, is_blockable=True):
        self._flag = flag
        self._cb = cb
        self.is_blockable = is_blockable
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
