#!/usr/bin/env python3

import asyncio
import threading
import time
import unittest
import chanpy as c
from chanpy import buffers, chan, xf


def b_list(ch):
    return list(ch.to_iter())


async def a_list(ch):
    return await c.to_list(ch).get()


class TestAsync(unittest.TestCase):
    def test_thread_put_to_async_get_without_wait(self):
        def putter(ch):
            ch.b_put('success')

        async def main():
            ch = chan()
            threading.Thread(target=putter, args=[ch]).start()
            return await ch.get()

        self.assertEqual(asyncio.run(main()), 'success')

    def test_thread_get_to_async_put_after_wait(self):
        result = None

        def getter(ch):
            nonlocal result
            result = ch.b_get()

        async def main():
            ch = chan()
            getter_thread = threading.Thread(target=getter, args=[ch])
            getter_thread.start()
            self.assertIs(await ch.put('success'), True)
            getter_thread.join()
            self.assertEqual(result, 'success')

        asyncio.run(main())

    def test_async_only_transfer(self):
        async def getter(ch):
            return await ch.get()

        async def main():
            ch = chan()
            get_ch = c.go(getter(ch))
            self.assertIs(await ch.put('success'), True)
            self.assertEqual(await get_ch.get(), 'success')

        asyncio.run(main())

    def test_go_from_different_thread(self):
        def getter_thread(ch):
            async def getter():
                return await ch.get()

            return c.go(getter()).b_get()

        async def main():
            ch = chan()
            thread_result_ch = c.thread_call(lambda: getter_thread(ch))
            self.assertIs(await ch.put('success'), True)
            self.assertEqual(await thread_result_ch.get(), 'success')

        asyncio.run(main())

    def test_go_coroutine_never_awaited(self):
        """ Test that no 'coroutine was not awaited' warning is raised

        The warning could be raised if the coroutine was added to the loop
        indirectly.

        Example:
            # If 'go' used a wrapper coroutine around 'coro' then 'coro' may
            # never be added to the loop. This is because there is no guarantee
            # that the wrapper coroutine will ever run and thus call await on
            # 'coro'.
            #
            # The following 'go' implementation would fail if wrapper never
            # ends up running:

            def go(coro):
                ch = chan(1)

                async def wrapper():
                    ret = await coro  # I may never run
                    if ret is not None:
                        await ch.put(ret)
                    ch.close()

                asyncio.run_coroutine_threadsafe(wrapper(), get_loop())
        """

        def thread():
            async def coro():
                pass
            c.go(coro())

        async def main():
            c.thread_call(thread).b_get()

        # Assert does NOT warn
        with self.assertRaises(AssertionError):
            with self.assertWarns(RuntimeWarning):
                asyncio.run(main())

    def test_alts_get_no_wait(self):
        get_ch, put_ch = chan(), chan()

        async def putter():
            await get_ch.put('success')

        async def main():
            c.go(putter())
            await asyncio.sleep(0.1)
            return await c.alts([[put_ch, 'noSend'], get_ch], priority=True)

        self.assertEqual(asyncio.run(main()), ('success', get_ch))

    def test_alts_put_after_wait(self):
        get_ch, put_ch = chan(), chan()

        async def putter():
            await asyncio.sleep(0.1)
            await put_ch.get()

        async def main():
            c.go(putter())
            return await c.alts([[put_ch, 'success'], get_ch], priority=True)

        self.assertEqual(asyncio.run(main()), (True, put_ch))

    def test_alts_timeout(self):
        async def main():
            start_time = time.time()
            timeout_ch = c.timeout(100)
            self.assertEqual(await c.alts([chan(), timeout_ch]),
                             (None, timeout_ch))
            elapsed_secs = time.time() - start_time
            self.assertIs(0.05 < elapsed_secs < 0.15, True)

        asyncio.run(main())

    def test_alts_default_when_available(self):
        async def main():
            ch = chan(1)
            await ch.put('success')
            self.assertEqual(await c.alts([ch], default='ignore me'),
                             ('success', ch))

        asyncio.run(main())

    def test_alts_default_when_unavailable(self):
        async def main():
            ch = chan()
            self.assertEqual(await c.alts([ch], default='success'),
                             ('success', 'default'))

        asyncio.run(main())

    def test_successful_cancel_get(self):
        async def main():
            ch = chan()
            get_future = ch.get()
            self.assertIs(get_future.cancelled(), False)
            self.assertIs(get_future.cancel(), True)
            self.assertIs(get_future.cancelled(), True)
            self.assertIs(ch.offer('reject me'), False)

        asyncio.run(main())

    def test_successful_cancel_put(self):
        async def main():
            ch = chan()
            put_future = ch.put('cancel me')
            self.assertIs(put_future.cancelled(), False)
            self.assertIs(put_future.cancel(), True)
            self.assertIs(put_future.cancelled(), True)
            self.assertIsNone(ch.poll())

        asyncio.run(main())

    def test_successful_cancel_alts(self):
        async def main():
            ch = chan()
            alts_future = c.alts([ch], priority=True)
            self.assertIs(alts_future.cancelled(), False)
            self.assertIs(alts_future.cancel(), True)
            self.assertIs(alts_future.cancelled(), True)
            self.assertIs(ch.offer('reject me'), False)

        asyncio.run(main())

    def test_unsuccessful_cancel_get(self):
        async def main():
            ch = chan()
            get_future = ch.get()
            self.assertIs(await ch.put('success'), True)

            # cancel() will end up calling set_result() since
            # set_result_threadsafe() callback won't have been called yet
            self.assertIs(get_future.cancel(), False)
            self.assertEqual(get_future.result(), 'success')

        asyncio.run(main())

    def test_unsuccessful_cancel_put(self):
        async def main():
            ch = chan()
            put_future = ch.put('val')
            self.assertEqual(await ch.get(), 'val')

            # cancel() will end up calling set_result() since
            # set_result_threadsafe() callback won't have been called yet
            self.assertIs(put_future.cancel(), False)
            self.assertIs(put_future.result(), True)

        asyncio.run(main())

    def test_unsuccessful_cancel_alts(self):
        async def main():
            success_ch, fail_ch = chan(), chan()
            alts_future = c.alts([fail_ch, success_ch])
            self.assertIs(await success_ch.put('success'), True)

            # cancel() will end up calling set_result() since
            # set_result_threadsafe() callback won't have been called yet
            self.assertIs(alts_future.cancel(), False)
            self.assertEqual(alts_future.result(), ('success', success_ch))

        asyncio.run(main())


class AbstractTestBufferedBlocking:
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan(1).b_put(None)

    def test_successful_blocking_get(self):
        ch = self.chan(1)
        threading.Thread(target=ch.b_put, args=['success']).start()
        self.assertEqual(ch.b_get(), 'success')

    def test_successful_blocking_put(self):
        self.assertIs(self.chan(1).b_put('success'), True)

    def test_blocking_get_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.b_get())

    def test_blocking_get_closed_full_buffer(self):
        ch = self.chan(1)
        ch.b_put('success')
        ch.close()
        self.assertEqual(ch.b_get(), 'success')

    def test_blocking_put_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.b_put('failure'), False)

    def test_blocking_put_closed_full_buffer(self):
        ch = self.chan(1)
        ch.b_put('fill buffer')
        ch.close()
        self.assertIs(ch.b_put('failure'), False)

    def test_close_while_blocking_get(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.b_get())

    def test_close_while_blocking_put(self):
        ch = self.chan(1)
        ch.b_put('fill buffer')

        def thread():
            time.sleep(0.1)
            ch.close()
            ch.b_get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.b_put('success'), True)
        self.assertEqual(ch.b_get(), 'success')
        self.assertIsNone(ch.b_get())

    def test_iter(self):
        ch = self.chan(2)
        ch.b_put('one')
        ch.b_put('two')
        ch.close()
        self.assertEqual(b_list(ch), ['one', 'two'])


class TestBufferedBlockingChan(unittest.TestCase,
                               AbstractTestBufferedBlocking):
    @staticmethod
    def chan(n):
        return c.chan(c.buffer(n))


class AbstractTestXform:
    def test_xform_map(self):
        async def main():
            ch = self.chan(1, xf.map(lambda x: x + 1))
            c.onto_chan(ch, [0, 1, 2])
            self.assertEqual(await a_list(ch), [1, 2, 3])

        asyncio.run(main())

    def test_xform_filter(self):
        async def main():
            ch = self.chan(1, xf.filter(lambda x: x % 2 == 0))
            c.onto_chan(ch, [0, 1, 2])
            self.assertEqual(await a_list(ch), [0, 2])

        asyncio.run(main())

    def test_xform_early_termination(self):
        async def main():
            ch = self.chan(1, xf.take(2))
            c.onto_chan(ch, [1, 2, 3, 4])
            self.assertEqual(await a_list(ch), [1, 2])

        asyncio.run(main())

    def test_xform_early_termination_works_after_close(self):
        async def main():
            ch = self.chan(1, xf.take_while(lambda x: x != 2))
            for i in range(4):
                ch.f_put(i)
            ch.close()
            self.assertEqual(await a_list(ch), [0, 1])
            self.assertEqual(len(ch._puts), 0)

        asyncio.run(main())

    def test_xform_successful_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.b_put([1, 2, 3])
        ch.close()
        self.assertEqual(b_list(ch), [1, 2, 3])

    def test_xform_unsuccessful_offer_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.b_put([1, 2])
        self.assertIs(ch.offer([1]), False)

    def test_unsuccessful_transformation_to_none(self):
        ch = self.chan(1, xf.map(lambda _: None))
        with self.assertRaises(TypeError):
            ch.b_put('failure')

    def test_close_flushes_xform_buffer(self):
        ch = self.chan(3, xf.partition_all(2))
        for i in range(3):
            ch.b_put(i)
        ch.close()
        self.assertEqual(b_list(ch), [(0, 1), (2,)])

    def test_close_does_not_flush_xform_with_pending_puts(self):
        ch = self.chan(1, xf.partition_all(2))
        for i in range(3):
            ch.f_put(i)
        ch.close()
        self.assertEqual(b_list(ch), [(0, 1), (2,)])

    def test_xform_ex_handler_non_none_return(self):
        def handler(e):
            if isinstance(e, ZeroDivisionError):
                return 'zero'

        ch = self.chan(3, xf.map(lambda x: 12 // x), handler)
        ch.b_put(-1)
        ch.b_put(0)
        ch.b_put(2)
        ch.close()
        self.assertEqual(b_list(ch), [-12, 'zero', 6])

    def test_xform_ex_handler_none_return(self):
        ch = self.chan(3, xf.map(lambda x: 12 // x), lambda _: None)
        ch.b_put(-1)
        ch.b_put(0)
        ch.b_put(2)
        ch.close()
        self.assertEqual(b_list(ch), [-12, 6])


class TestXformBufferedChan(unittest.TestCase, AbstractTestXform):
    @staticmethod
    def chan(n, xform, ex_handler=None):
        return c.chan(c.buffer(n), xform, ex_handler)


class AbstractTestBufferedNonblocking:
    def test_unsuccessful_offer_none(self):
        with self.assertRaises(TypeError):
            self.chan(1).offer(None)

    def test_successful_poll(self):
        ch = self.chan(1)
        threading.Thread(target=ch.b_put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.poll(), 'success')

    def test_successful_offer(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.offer('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.b_get(), 'success')

    def test_unsuccessful_poll(self):
        self.assertIsNone(self.chan(1).poll())

    def test_unsuccessful(self):
        ch = self.chan(1)
        ch.b_put('fill buffer')
        self.assertIs(ch.offer('failure'), False)

    def test_poll_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.poll())

    def test_poll_closed_full_buffer(self):
        ch = self.chan(1)
        ch.b_put('success')
        ch.close()
        self.assertEqual(ch.poll(), 'success')

    def test_offer_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.offer('failure'), False)

    def test_closed_full_buffer(self):
        ch = self.chan(1)
        ch.b_put('fill buffer')
        ch.close()
        self.assertIs(ch.offer('failure'), False)


class TestBufferedNonBlockingChan(unittest.TestCase,
                                  AbstractTestBufferedNonblocking):
    @staticmethod
    def chan(n):
        return chan(c.buffer(n))


class TestChan(unittest.TestCase):
    def test_ValueError_nonpositive_buffer(self):
        with self.assertRaises(ValueError):
            chan(0)


class AbstractTestUnbufferedBlocking:
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan().b_put(None)

    def test_blocking_get_first(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.b_put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.b_get(), 'success')

    def test_blocking_put_first(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.b_get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.b_put('success'), True)

    def test_put_blocks_until_get(self):
        status = 'failure'
        ch = self.chan()

        def thread():
            nonlocal status
            time.sleep(0.1)
            status = 'success'
            ch.b_get()

        threading.Thread(target=thread).start()
        ch.b_put(1)
        self.assertEqual(status, 'success')

    def test_blocking_get_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.b_get())

    def test_blocking_put_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.b_put('failure'), False)

    def test_close_while_blocking_get(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.b_get())

    def test_close_while_blocking_put(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.close()
            ch.b_get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.b_put('success'), True)
        self.assertIsNone(ch.b_get())

    def test_iter(self):
        ch = self.chan()
        ch.f_put('one')
        ch.f_put('two')
        ch.close()
        self.assertEqual(b_list(ch), ['one', 'two'])

    def test_xform_exception(self):
        with self.assertRaises(TypeError):
            self.chan(None, xf.cat)

    def test_ex_handler_exception(self):
        with self.assertRaises(TypeError):
            self.chan(ex_handler=xf.identity)


class TestUnbufferedBlockingChan(unittest.TestCase,
                                 AbstractTestUnbufferedBlocking):
    @staticmethod
    def chan():
        return chan()


class AbstractTestUnbufferedNonblocking:
    def test_unsuccessful_offer_none(self):
        with self.assertRaises(TypeError):
            self.chan().offer(None)

    def test_successful_poll(self):
        ch = self.chan()
        threading.Thread(target=ch.b_put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.poll(), 'success')

    def test_successful_offer(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.offer('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.b_get(), 'success')

    def test_unsuccessful_poll(self):
        self.assertIsNone(self.chan().poll())

    def test_unsuccessful_offer(self):
        self.assertIs(self.chan().offer('failure'), False)

    def test_poll_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.poll())

    def test_offer_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.offer('failure'), False)


class TestUnbufferedNonblockingChan(unittest.TestCase,
                                    AbstractTestUnbufferedNonblocking):
    @staticmethod
    def chan():
        return chan()


class TestPromiseChan(unittest.TestCase):
    def test_multiple_gets(self):
        ch = c.promise_chan()
        self.assertIs(ch.b_put('success'), True)
        self.assertEqual(ch.b_get(), 'success')
        self.assertEqual(ch.b_get(), 'success')

    def test_multiple_puts(self):
        ch = c.promise_chan()
        self.assertIs(ch.b_put('success'), True)
        self.assertIs(ch.b_put('drop me'), True)

    def test_after_close(self):
        ch = c.promise_chan()
        ch.b_put('success')
        ch.close()
        self.assertIs(ch.b_put('failure'), False)
        self.assertIs(ch.b_put('failure'), False)
        self.assertEqual(ch.b_get(), 'success')
        self.assertEqual(ch.b_get(), 'success')

    def test_xform_filter(self):
        ch = c.promise_chan(xf.filter(lambda x: x > 0))
        self.assertIs(ch.b_put(-1), True)
        self.assertIs(ch.b_put(1), True)
        self.assertIs(ch.b_put(2), True)

        self.assertEqual(ch.b_get(), 1)
        self.assertEqual(ch.b_get(), 1)

    def test_xform_complete_flush(self):
        ch = c.promise_chan(xf.partition_all(3))
        self.assertIs(ch.b_put(1), True)
        self.assertIs(ch.b_put(2), True)
        self.assertIsNone(ch.poll())
        ch.close()
        self.assertEqual(ch.b_get(), (1, 2))
        self.assertEqual(ch.b_get(), (1, 2))
        self.assertIs(ch.b_put('drop me'), False)

    def test_xform_with_reduced_return(self):
        ch = c.promise_chan(xf.take(1))
        self.assertIs(ch.b_put('success'), True)
        self.assertIs(ch.b_put('failure'), False)
        self.assertEqual(ch.b_get(), 'success')
        self.assertEqual(ch.b_get(), 'success')


class AbstractTestAlts:
    def _confirm_chs_not_closed(self, *chs):
        for ch in chs:
            ch.f_put('notClosed')
            self.assertEqual(ch.b_get(), 'notClosed')

    def test_no_operations(self):
        with self.assertRaises(ValueError):
            c.b_alts([])

    def test_single_successful_get_on_initial_request(self):
        ch = self.chan()
        ch.f_put('success')
        ch.f_put('notClosed')
        self.assertEqual(c.b_alts([ch]), ('success', ch))
        self.assertEqual(ch.b_get(), 'notClosed')

    def test_single_successful_get_on_wait(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.f_put('success')
            ch.f_put('notClosed')

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([ch]), ('success', ch))
        self.assertEqual(ch.b_get(), 'notClosed')

    def test_single_successful_put_on_initial_request(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.b_put(c.b_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        self.assertEqual(ch.b_get(), 'success')
        self.assertEqual(ch.b_get(), (True, ch))

    def test_get_put_same_channel(self):
        ch = self.chan()
        with self.assertRaises(ValueError):
            c.b_alts([ch, [ch, 'success']], priority=True)


class AbstractTestUnbufferedAlts(AbstractTestAlts):
    def test_single_successful_put_on_wait(self):
        ch = self.chan()

        def thread():
            ch.b_put(c.b_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(ch.b_get(), 'success')
        self.assertEqual(ch.b_get(), (True, ch))

    def test_multiple_successful_get_on_initial_request(self):
        successGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()
        successGetCh.f_put('success')
        time.sleep(0.1)
        self.assertEqual(c.b_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))
        self._confirm_chs_not_closed(successGetCh, cancelGetCh, cancelPutCh)

    def test_multiple_successful_get_on_wait(self):
        successGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            successGetCh.b_put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))
        self._confirm_chs_not_closed(successGetCh, cancelGetCh, cancelPutCh)

    def test_multiple_successful_put_on_initial_requst(self):
        successPutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            successPutCh.b_put(c.b_alts([cancelGetCh,
                                        [successPutCh, 'success'],
                                        [cancelPutCh, 'noSend']],
                                        priority=True))

        threading.Thread(target=thread).start()
        self.assertEqual(successPutCh.b_get(), 'success')
        self.assertEqual(successPutCh.b_get(), (True, successPutCh))
        self._confirm_chs_not_closed(cancelGetCh, successPutCh, cancelPutCh)

    def test_multiple_successful_put_on_wait(self):
        successPutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            successPutCh.b_put(c.b_alts([cancelGetCh,
                                        [successPutCh, 'success'],
                                        [cancelPutCh, 'noSend']],
                                        priority=True))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(successPutCh.b_get(), 'success')
        self.assertEqual(successPutCh.b_get(), (True, successPutCh))
        self._confirm_chs_not_closed(cancelGetCh, successPutCh, cancelPutCh)

    def test_close_before_get(self):
        closedGetCh = self.chan()
        cancelPutCh = self.chan()
        cancelGetCh = self.chan()
        closedGetCh.close()
        self.assertEqual(c.b_alts([[cancelPutCh, 'noSend'],
                                   closedGetCh,
                                   cancelGetCh], priority=True),
                         (None, closedGetCh))
        self.assertIsNone(closedGetCh.b_get())
        self._confirm_chs_not_closed(cancelPutCh, cancelGetCh)

    def test_close_before_put(self):
        closedPutCh = self.chan()
        cancelPutCh = self.chan()
        cancelGetCh = self.chan()
        closedPutCh.close()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  [closedPutCh, 'noSend'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (False, closedPutCh))
        self.assertIsNone(closedPutCh.b_get())
        self._confirm_chs_not_closed(cancelPutCh, cancelGetCh)

    def test_close_while_waiting_get(self):
        closeGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            closeGetCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  closeGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         (None, closeGetCh))
        self.assertIsNone(closeGetCh.b_get())
        self._confirm_chs_not_closed(cancelPutCh, cancelGetCh)

    def test_close_while_waiting_put(self):
        closePutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            closePutCh.close()
            closePutCh.b_get()

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  [closePutCh, 'success'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (True, closePutCh))
        self.assertIsNone(closePutCh.b_get())
        self._confirm_chs_not_closed(cancelPutCh, cancelGetCh)

    def test_double_b_alts_successful_transfer(self):
        ch = self.chan()

        def thread():
            ch.b_put(c.b_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([ch]), ('success', ch))
        self.assertEqual(ch.b_get(), (True, ch))


class AbstractTestBufferedAlts(AbstractTestAlts):
    def test_single_successful_put_on_wait(self):
        ch = self.chan(1)
        ch.b_put('fill buffer')

        def thread():
            ch.b_put(c.b_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(ch.b_get(), 'fill buffer')
        self.assertEqual(ch.b_get(), 'success')
        self.assertEqual(ch.b_get(), (True, ch))

    def test_multiple_successful_get_on_initial_request(self):
        successGetCh = self.chan(1)
        successGetCh.b_put('success')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')

        self.assertEqual(c.b_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))

    def test_multiple_successful_get_on_wait(self):
        successGetCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')

        def thread():
            time.sleep(0.1)
            successGetCh.b_put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))

    def test_multiple_successful_put_on_intial_request(self):
        successPutCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')

        altsValue = c.b_alts([cancelGetCh,
                             [cancelPutCh, 'noSend'],
                             [successPutCh, 'success']], priority=True)

        self.assertEqual(altsValue, (True, successPutCh))
        self.assertEqual(successPutCh.b_get(), 'success')

    def test_multiple_successful_put_on_wait(self):
        successPutCh = self.chan(1)
        successPutCh.b_put('fill buffer')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')

        def thread():
            successPutCh.b_put(c.b_alts([cancelGetCh,
                                        [successPutCh, 'success'],
                                        [cancelPutCh, 'noSend']],
                                        priority=True))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(successPutCh.b_get(), 'fill buffer')
        self.assertEqual(successPutCh.b_get(), 'success')
        self.assertEqual(successPutCh.b_get(), (True, successPutCh))

    def test_close_before_get(self):
        closedGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')
        cancelGetCh = self.chan(1)
        closedGetCh.close()
        self.assertEqual(c.b_alts([[cancelPutCh, 'noSend'],
                                   closedGetCh,
                                   cancelGetCh], priority=True),
                         (None, closedGetCh))
        self.assertIsNone(closedGetCh.b_get())

    def test_close_before_put(self):
        closedPutCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')
        cancelGetCh = self.chan(1)
        closedPutCh.close()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  [closedPutCh, 'noSend'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (False, closedPutCh))
        self.assertIsNone(closedPutCh.b_get())

    def test_close_while_waiting_get(self):
        closeGetCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')

        def thread():
            time.sleep(0.1)
            closeGetCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  closeGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         (None, closeGetCh))
        self.assertIsNone(closeGetCh.b_get())

    def test_close_while_waiting_put(self):
        closePutCh = self.chan(1)
        closePutCh.b_put('fill buffer')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.b_put('fill buffer')

        def thread():
            time.sleep(0.1)
            closePutCh.close()
            closePutCh.b_get()

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([cancelGetCh,
                                  [closePutCh, 'success'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (True, closePutCh))
        self.assertEqual(closePutCh.b_get(), 'success')
        self.assertIsNone(closePutCh.b_get())

    def test_double_b_alts_successful_transfer(self):
        ch = self.chan(1)

        self.assertEqual(c.b_alts([[ch, 'success']]), (True, ch))
        self.assertEqual(c.b_alts([ch]), ('success', ch))

    def test_xform_state_is_not_modified_when_canceled(self):
        xformCh = self.chan(1, xf.take(2))
        xformCh.b_put('firstTake')
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.b_put('altsValue')

        threading.Thread(target=thread).start()
        self.assertEqual(c.b_alts([ch, [xformCh, 'do not modify xform state']],
                                  priority=True),
                         ('altsValue', ch))
        xformCh.f_put('secondTake')
        xformCh.f_put('dropMe')
        self.assertEqual(b_list(xformCh), ['firstTake', 'secondTake'])


class TestUnbufferedAltsChan(unittest.TestCase, AbstractTestUnbufferedAlts):
    @staticmethod
    def chan():
        return chan()


class TestBufferedAltsChan(unittest.TestCase, AbstractTestBufferedAlts):
    @staticmethod
    def chan(n=1, xform=xf.identity):
        return chan(c.buffer(n), xform)


class TestAltsThreads(unittest.TestCase):
    def test_b_alts_default_when_available(self):
        ch = chan(1)
        ch.b_put('success')
        self.assertEqual(c.b_alts([ch], default='ignore me'), ('success', ch))

    def test_b_alts_default_when_unavailable(self):
        ch = chan()
        self.assertEqual(c.b_alts([ch], default='success'),
                         ('success', 'default'))


class TestDroppingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.dropping_buffer(1))
        ch.b_put('keep')
        ch.b_put('drop')
        self.assertIs(ch.b_put('drop'), True)

    def test_buffer_keeps_oldest_n_elements(self):
        ch = chan(c.dropping_buffer(2))
        ch.b_put('keep1')
        ch.b_put('keep2')
        ch.b_put('drop')
        ch.close()
        self.assertEqual(b_list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.dropping_buffer(2), xf.cat)
        ch.b_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(b_list(ch), [1, 2])

    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.dropping_buffer(1)), True)


class TestSlidingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.sliding_buffer(1))
        ch.b_put('drop')
        ch.b_put('drop')
        self.assertIs(ch.b_put('keep'), True)

    def test_buffer_keeps_newest_n_elements(self):
        ch = chan(c.sliding_buffer(2))
        ch.b_put('drop')
        ch.b_put('keep1')
        ch.b_put('keep2')
        ch.close()
        self.assertEqual(b_list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.sliding_buffer(2), xf.cat)
        ch.b_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(b_list(ch), [3, 4])

    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.sliding_buffer(1)), True)


class TestPromiseBuffer(unittest.TestCase):
    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(buffers.PromiseBuffer()), True)


if __name__ == '__main__':
    unittest.main()
