#!/usr/bin/env python3

import asyncio
import threading
import time
import unittest
import chanpy as c
from chanpy import buffers, chan, xf
from chanpy.channel import Chan


class TestAsync(unittest.TestCase):
    def test_thread_put_to_async_get_without_wait(self):
        def putter(ch):
            ch.t_put('success')

        async def main():
            ch = chan()
            threading.Thread(target=putter, args=[ch]).start()
            return await ch.a_get()

        self.assertEqual(asyncio.run(main()), 'success')

    def test_thread_get_to_async_put_after_wait(self):
        result = None

        def getter(ch):
            nonlocal result
            result = ch.t_get()

        async def main():
            ch = chan()
            getter_thread = threading.Thread(target=getter, args=[ch])
            getter_thread.start()
            self.assertIs(await ch.a_put('success'), True)
            getter_thread.join()
            self.assertEqual(result, 'success')

        asyncio.run(main())

    def test_async_only_transfer(self):
        async def getter(ch):
            return await ch.a_get()

        async def main():
            ch = chan()
            get_ch = c.go(getter(ch))
            self.assertIs(await ch.a_put('success'), True)
            self.assertEqual(await get_ch.a_get(), 'success')

        asyncio.run(main())

    def test_go_from_different_thread(self):
        def getter_thread(loop, ch):
            async def getter():
                return await ch.a_get()

            return c.go(getter(), loop).t_get()

        async def main():
            loop = asyncio.get_running_loop()
            ch = chan()
            thread_result_ch = c.thread_call(lambda: getter_thread(loop, ch))
            self.assertIs(await ch.a_put('success'), True)
            self.assertEqual(await thread_result_ch.a_get(), 'success')

        asyncio.run(main())

    def test_go_coroutine_never_awaited(self):
        """ Test that no 'coroutine was not awaited' warning is raised

        The warning could be raised if the coroutine was added to the loop
        indirectly.

        Example:
            # If 'go' used a wrapper couroutine around 'coro' then 'coro' may
            # never be added to the loop. This is because there is no guarantee
            # that the wrapper coroutine will ever run and thus call await on
            # 'coro'.
            #
            # The following 'go' implementation would fail if wrapper never
            # ends up running:

            def go(coro, loop):
                ch = chan(1)

                async def wrapper():
                    ret = await coro  # I may never run
                    if ret is not None:
                        await ch.a_put(ret)
                    ch.close()

                asyncio.run_coroutine_threadsafe(wrapper(), loop)
        """

        def thread(loop):
            async def coro():
                pass
            c.go(coro(), loop)

        async def main():
            loop = asyncio.get_running_loop()
            t = threading.Thread(target=thread, args=[loop])
            t.start()
            t.join()

        # Assert does NOT warn
        with self.assertRaises(AssertionError):
            with self.assertWarns(RuntimeWarning):
                asyncio.run(main())

    def test_a_alts_get_no_wait(self):
        get_ch, put_ch = chan(), chan()

        async def putter():
            await get_ch.a_put('success')

        async def main():
            c.go(putter())
            await asyncio.sleep(0.1)
            return await c.a_alts([[put_ch, 'noSend'], get_ch], priority=True)

        self.assertEqual(asyncio.run(main()), ('success', get_ch))

    def test_a_alts_put_after_wait(self):
        get_ch, put_ch = chan(), chan()

        async def putter():
            await asyncio.sleep(0.1)
            await put_ch.a_get()

        async def main():
            c.go(putter())
            return await c.a_alts([[put_ch, 'success'], get_ch], priority=True)

        self.assertEqual(asyncio.run(main()), (True, put_ch))

    def test_a_alts_timeout(self):
        async def main():
            start_time = time.time()
            timeout_ch = c.timeout(100)
            self.assertEqual(await c.a_alts([chan(), timeout_ch]),
                             (None, timeout_ch))
            elapsed_secs = time.time() - start_time
            self.assertIs(0.05 < elapsed_secs < 0.15, True)

        asyncio.run(main())

    def test_a_alts_default_when_available(self):
        async def main():
            ch = chan(1)
            await ch.a_put('success')
            self.assertEqual(await c.a_alts([ch], default='ignore me'),
                             ('success', ch))

        asyncio.run(main())

    def test_a_alts_default_when_unavailable(self):
        async def main():
            ch = chan()
            self.assertEqual(await c.a_alts([ch], default='success'),
                             ('success', 'default'))

        asyncio.run(main())

    def test_successful_cancel_get(self):
        async def main():
            ch = chan()
            get_future = ch.a_get()
            self.assertIs(get_future.cancelled(), False)
            self.assertIs(get_future.cancel(), True)
            self.assertIs(get_future.cancelled(), True)
            self.assertIs(ch.offer('reject me'), False)

        asyncio.run(main())

    def test_successful_cancel_put(self):
        async def main():
            ch = chan()
            put_future = ch.a_put('cancel me')
            self.assertIs(put_future.cancelled(), False)
            self.assertIs(put_future.cancel(), True)
            self.assertIs(put_future.cancelled(), True)
            self.assertIsNone(ch.poll())

        asyncio.run(main())

    def test_successful_cancel_alts(self):
        async def main():
            ch = chan()
            alts_future = c.a_alts([ch], priority=True)
            self.assertIs(alts_future.cancelled(), False)
            self.assertIs(alts_future.cancel(), True)
            self.assertIs(alts_future.cancelled(), True)
            self.assertIs(ch.offer('reject me'), False)

        asyncio.run(main())

    def test_unsuccessful_cancel_get(self):
        async def main():
            ch = chan()
            get_future = ch.a_get()
            self.assertIs(await ch.a_put('success'), True)

            # cancel() will end up calling set_result() since
            # set_result_threadsafe() callback won't have been called yet
            self.assertIs(get_future.cancel(), False)
            self.assertEqual(get_future.result(), 'success')

        asyncio.run(main())

    def test_unsuccessful_cancel_put(self):
        async def main():
            ch = chan()
            put_future = ch.a_put('val')
            self.assertEqual(await ch.a_get(), 'val')

            # cancel() will end up calling set_result() since
            # set_result_threadsafe() callback won't have been called yet
            self.assertIs(put_future.cancel(), False)
            self.assertIs(put_future.result(), True)

        asyncio.run(main())

    def test_unsuccessful_cancel_alts(self):
        async def main():
            success_ch, fail_ch = chan(), chan()
            alts_future = c.a_alts([fail_ch, success_ch])
            self.assertIs(await success_ch.a_put('success'), True)

            # cancel() will end up calling set_result() since
            # set_result_threadsafe() callback won't have been called yet
            self.assertIs(alts_future.cancel(), False)
            self.assertEqual(alts_future.result(), ('success', success_ch))

        asyncio.run(main())


class AbstractTestBufferedBlocking:
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan(1).t_put(None)

    def test_successful_blocking_get(self):
        ch = self.chan(1)
        threading.Thread(target=ch.t_put, args=['success']).start()
        self.assertEqual(ch.t_get(), 'success')

    def test_successful_blocking_put(self):
        self.assertIs(self.chan(1).t_put('success'), True)

    def test_blocking_get_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.t_get())

    def test_blocking_get_closed_full_buffer(self):
        ch = self.chan(1)
        ch.t_put('success')
        ch.close()
        self.assertEqual(ch.t_get(), 'success')

    def test_blocking_put_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.t_put('failure'), False)

    def test_blocking_put_closed_full_buffer(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')
        ch.close()
        self.assertIs(ch.t_put('failure'), False)

    def test_close_while_blocking_get(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.t_get())

    def test_close_while_blocking_put(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')

        def thread():
            time.sleep(0.1)
            ch.close()
            ch.t_get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.t_put('success'), True)
        self.assertEqual(ch.t_get(), 'success')
        self.assertIsNone(ch.t_get())

    def test_iter(self):
        ch = self.chan(2)
        ch.t_put('one')
        ch.t_put('two')
        ch.close()
        self.assertEqual(c.t_list(ch), ['one', 'two'])


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
            self.assertEqual(await c.a_list(ch), [1, 2, 3])

        asyncio.run(main())

    def test_xform_filter(self):
        async def main():
            ch = self.chan(1, xf.filter(lambda x: x % 2 == 0))
            c.onto_chan(ch, [0, 1, 2])
            self.assertEqual(await c.a_list(ch), [0, 2])

        asyncio.run(main())

    def test_xform_early_termination(self):
        async def main():
            ch = self.chan(1, xf.take(2))
            c.onto_chan(ch, [1, 2, 3, 4])
            self.assertEqual(await c.a_list(ch), [1, 2])

        asyncio.run(main())

    def test_xform_early_termination_works_after_close(self):
        async def main():
            ch = self.chan(1, xf.take_while(lambda x: x != 2))
            for i in range(4):
                c.async_put(ch, i)
            ch.close()
            self.assertEqual(await c.a_list(ch), [0, 1])
            self.assertEqual(len(ch._puts), 0)

        asyncio.run(main())

    def test_xform_successful_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.t_put([1, 2, 3])
        ch.close()
        self.assertEqual(c.t_list(ch), [1, 2, 3])

    def test_xform_unsuccessful_offer_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.t_put([1, 2])
        self.assertIs(ch.offer([1]), False)

    def test_unsuccessful_transformation_to_none(self):
        ch = self.chan(1, xf.map(lambda _: None))
        with self.assertRaises(TypeError):
            ch.t_put('failure')

    def test_close_flushes_xform_buffer(self):
        ch = self.chan(3, xf.partition_all(2))
        for i in range(3):
            ch.t_put(i)
        ch.close()
        self.assertEqual(c.t_list(ch), [(0, 1), (2,)])

    def test_close_does_not_flush_xform_with_pending_puts(self):
        ch = self.chan(1, xf.partition_all(2))
        for i in range(3):
            c.async_put(ch, i)
        ch.close()
        self.assertEqual(c.t_list(ch), [(0, 1), (2,)])

    def test_xform_ex_handler_non_none_return(self):
        def handler(e):
            if isinstance(e, ZeroDivisionError):
                return 'zero'

        ch = self.chan(3, xf.map(lambda x: 12 // x), handler)
        ch.t_put(-1)
        ch.t_put(0)
        ch.t_put(2)
        ch.close()
        self.assertEqual(c.t_list(ch), [-12, 'zero', 6])

    def test_xform_ex_handler_none_return(self):
        def handler(e):
            return None

        ch = self.chan(3, xf.map(lambda x: 12 // x), handler)
        ch.t_put(-1)
        ch.t_put(0)
        ch.t_put(2)
        ch.close()
        self.assertEqual(c.t_list(ch), [-12, 6])


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
        threading.Thread(target=ch.t_put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.poll(), 'success')

    def test_successful_offer(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.offer('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.t_get(), 'success')

    def test_unsuccessful_poll(self):
        self.assertIsNone(self.chan(1).poll())

    def test_unsuccessful(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')
        self.assertIs(ch.offer('failure'), False)

    def tespoll_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.poll())

    def tespoll_closed_full_buffer(self):
        ch = self.chan(1)
        ch.t_put('success')
        ch.close()
        self.assertEqual(ch.poll(), 'success')

    def tesoffer_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.offer('failure'), False)

    def test_closed_full_buffer(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')
        ch.close()
        self.assertIs(ch.offer('failure'), False)


class TestBufferedNonBlockingChan(unittest.TestCase,
                                  AbstractTestBufferedNonblocking):
    @staticmethod
    def chan(n):
        return Chan(c.buffer(n))


class TestChan(unittest.TestCase):
    def test_ValueError_nonpositive_buffer(self):
        with self.assertRaises(ValueError):
            chan(0)


class AbstractTestUnbufferedBlocking:
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan().t_put(None)

    def test_blocking_get_first(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.t_put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.t_get(), 'success')

    def test_blocking_put_first(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.t_get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.t_put('success'), True)

    def test_put_blocks_until_get(self):
        status = 'failure'
        ch = self.chan()

        def thread():
            nonlocal status
            time.sleep(0.1)
            status = 'success'
            ch.t_get()

        threading.Thread(target=thread).start()
        ch.t_put(1)
        self.assertEqual(status, 'success')

    def test_blocking_get_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.t_get())

    def test_blocking_put_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.t_put('failure'), False)

    def test_close_while_blocking_get(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.t_get())

    def test_close_while_blocking_put(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.close()
            ch.t_get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.t_put('success'), True)
        self.assertIsNone(ch.t_get())

    def test_iter(self):
        ch = self.chan()
        c.async_put(ch, 'one')
        c.async_put(ch, 'two')
        ch.close()
        self.assertEqual(c.t_list(ch), ['one', 'two'])

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
        return Chan()


class AbstractTestUnbufferedNonblocking:
    def test_unsuccessful_offer_none(self):
        with self.assertRaises(TypeError):
            self.chan().offer(None)

    def test_successful_poll(self):
        ch = self.chan()
        threading.Thread(target=ch.t_put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.poll(), 'success')

    def test_successful_offer(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.offer('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.t_get(), 'success')

    def test_unsuccessful_poll(self):
        self.assertIsNone(self.chan().poll())

    def test_unsuccessful_offer(self):
        self.assertIs(self.chan().offer('failure'), False)

    def tespoll_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.poll())

    def tesoffer_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.offer('failure'), False)


class TestUnbufferedNonblockingChan(unittest.TestCase,
                                    AbstractTestUnbufferedNonblocking):
    @staticmethod
    def chan():
        return Chan()


class TestPromiseChan(unittest.TestCase):
    def test_multiple_gets(self):
        ch = c.promise_chan()
        self.assertIs(ch.t_put('success'), True)
        self.assertEqual(ch.t_get(), 'success')
        self.assertEqual(ch.t_get(), 'success')

    def test_multiple_puts(self):
        ch = c.promise_chan()
        self.assertIs(ch.t_put('success'), True)
        self.assertIs(ch.t_put('drop me'), True)

    def test_after_close(self):
        ch = c.promise_chan()
        ch.t_put('success')
        ch.close()
        self.assertIs(ch.t_put('failure'), False)
        self.assertIs(ch.t_put('failure'), False)
        self.assertEqual(ch.t_get(), 'success')
        self.assertEqual(ch.t_get(), 'success')

    def test_xform_filter(self):
        ch = c.promise_chan(xf.filter(lambda x: x > 0))
        self.assertIs(ch.t_put(-1), True)
        self.assertIs(ch.t_put(1), True)
        self.assertIs(ch.t_put(2), True)

        self.assertEqual(ch.t_get(), 1)
        self.assertEqual(ch.t_get(), 1)

    def test_xform_complete_flush(self):
        ch = c.promise_chan(xf.partition_all(3))
        self.assertIs(ch.t_put(1), True)
        self.assertIs(ch.t_put(2), True)
        self.assertIsNone(ch.poll())
        ch.close()
        self.assertEqual(ch.t_get(), (1, 2))
        self.assertEqual(ch.t_get(), (1, 2))
        self.assertIs(ch.t_put('drop me'), False)

    def test_xform_with_reduced_return(self):
        ch = c.promise_chan(xf.take(1))
        self.assertIs(ch.t_put('success'), True)
        self.assertIs(ch.t_put('failure'), False)
        self.assertEqual(ch.t_get(), 'success')
        self.assertEqual(ch.t_get(), 'success')


class AbstractTestAlts:
    def _confirm_chans_not_closed(self, *chs):
        for ch in chs:
            c.async_put(ch, 'notClosed')
            self.assertEqual(ch.t_get(), 'notClosed')

    def test_no_operations(self):
        with self.assertRaises(ValueError):
            c.t_alts([])

    def test_single_successful_get_on_initial_request(self):
        ch = self.chan()
        c.async_put(ch, 'success')
        c.async_put(ch, 'notClosed')
        self.assertEqual(c.t_alts([ch]), ('success', ch))
        self.assertEqual(ch.t_get(), 'notClosed')

    def test_single_successful_get_on_wait(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            c.async_put(ch, 'success')
            c.async_put(ch, 'notClosed')

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([ch]), ('success', ch))
        self.assertEqual(ch.t_get(), 'notClosed')

    def test_single_successful_put_on_initial_request(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.t_put(c.t_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        self.assertEqual(ch.t_get(), 'success')
        self.assertEqual(ch.t_get(), (True, ch))

    def test_get_put_same_channel(self):
        ch = self.chan()
        with self.assertRaises(ValueError):
            c.t_alts([ch, [ch, 'success']], priority=True)


class AbstractTestUnbufferedAlts(AbstractTestAlts):
    def test_single_successful_put_on_wait(self):
        ch = self.chan()

        def thread():
            ch.t_put(c.t_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(ch.t_get(), 'success')
        self.assertEqual(ch.t_get(), (True, ch))

    def test_multiple_successful_get_on_initial_request(self):
        successGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()
        c.async_put(successGetCh, 'success')
        time.sleep(0.1)
        self.assertEqual(c.t_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))
        self._confirm_chans_not_closed(successGetCh, cancelGetCh, cancelPutCh)

    def test_multiple_successful_get_on_wait(self):
        successGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            successGetCh.t_put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))
        self._confirm_chans_not_closed(successGetCh, cancelGetCh, cancelPutCh)

    def test_multiple_successful_put_on_initial_requst(self):
        successPutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            successPutCh.t_put(c.t_alts([cancelGetCh,
                                        [successPutCh, 'success'],
                                        [cancelPutCh, 'noSend']],
                                        priority=True))

        threading.Thread(target=thread).start()
        self.assertEqual(successPutCh.t_get(), 'success')
        self.assertEqual(successPutCh.t_get(), (True, successPutCh))
        self._confirm_chans_not_closed(cancelGetCh, successPutCh, cancelPutCh)

    def test_multiple_successful_put_on_wait(self):
        successPutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            successPutCh.t_put(c.t_alts([cancelGetCh,
                                        [successPutCh, 'success'],
                                        [cancelPutCh, 'noSend']],
                                        priority=True))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(successPutCh.t_get(), 'success')
        self.assertEqual(successPutCh.t_get(), (True, successPutCh))
        self._confirm_chans_not_closed(cancelGetCh, successPutCh, cancelPutCh)

    def test_close_before_get(self):
        closedGetCh = self.chan()
        cancelPutCh = self.chan()
        cancelGetCh = self.chan()
        closedGetCh.close()
        self.assertEqual(c.t_alts([[cancelPutCh, 'noSend'],
                                   closedGetCh,
                                   cancelGetCh], priority=True),
                         (None, closedGetCh))
        self.assertIsNone(closedGetCh.t_get())
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_close_before_put(self):
        closedPutCh = self.chan()
        cancelPutCh = self.chan()
        cancelGetCh = self.chan()
        closedPutCh.close()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  [closedPutCh, 'noSend'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (False, closedPutCh))
        self.assertIsNone(closedPutCh.t_get())
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_close_while_waiting_get(self):
        closeGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            closeGetCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  closeGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         (None, closeGetCh))
        self.assertIsNone(closeGetCh.t_get())
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_close_while_waiting_put(self):
        closePutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            closePutCh.close()
            closePutCh.t_get()

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  [closePutCh, 'success'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (True, closePutCh))
        self.assertIsNone(closePutCh.t_get())
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_double_t_alts_successful_transfer(self):
        ch = self.chan()

        def thread():
            ch.t_put(c.t_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([ch]), ('success', ch))
        self.assertEqual(ch.t_get(), (True, ch))


class AbstractTestBufferedAlts(AbstractTestAlts):
    def test_single_successful_put_on_wait(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')

        def thread():
            ch.t_put(c.t_alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(ch.t_get(), 'fill buffer')
        self.assertEqual(ch.t_get(), 'success')
        self.assertEqual(ch.t_get(), (True, ch))

    def test_multiple_successful_get_on_initial_request(self):
        successGetCh = self.chan(1)
        successGetCh.t_put('success')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')

        self.assertEqual(c.t_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))

    def test_multiple_successful_get_on_wait(self):
        successGetCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')

        def thread():
            time.sleep(0.1)
            successGetCh.t_put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  successGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         ('success', successGetCh))

    def test_multiple_successful_put_on_intial_request(self):
        successPutCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')

        altsValue = c.t_alts([cancelGetCh,
                             [cancelPutCh, 'noSend'],
                             [successPutCh, 'success']], priority=True)

        self.assertEqual(altsValue, (True, successPutCh))
        self.assertEqual(successPutCh.t_get(), 'success')

    def test_multiple_successful_put_on_wait(self):
        successPutCh = self.chan(1)
        successPutCh.t_put('fill buffer')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')

        def thread():
            successPutCh.t_put(c.t_alts([cancelGetCh,
                                        [successPutCh, 'success'],
                                        [cancelPutCh, 'noSend']],
                                        priority=True))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(successPutCh.t_get(), 'fill buffer')
        self.assertEqual(successPutCh.t_get(), 'success')
        self.assertEqual(successPutCh.t_get(), (True, successPutCh))

    def test_close_before_get(self):
        closedGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')
        cancelGetCh = self.chan(1)
        closedGetCh.close()
        self.assertEqual(c.t_alts([[cancelPutCh, 'noSend'],
                                   closedGetCh,
                                   cancelGetCh], priority=True),
                         (None, closedGetCh))
        self.assertIsNone(closedGetCh.t_get())

    def test_close_before_put(self):
        closedPutCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')
        cancelGetCh = self.chan(1)
        closedPutCh.close()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  [closedPutCh, 'noSend'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (False, closedPutCh))
        self.assertIsNone(closedPutCh.t_get())

    def test_close_while_waiting_get(self):
        closeGetCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')

        def thread():
            time.sleep(0.1)
            closeGetCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  closeGetCh,
                                  [cancelPutCh, 'noSend']], priority=True),
                         (None, closeGetCh))
        self.assertIsNone(closeGetCh.t_get())

    def test_close_while_waiting_put(self):
        closePutCh = self.chan(1)
        closePutCh.t_put('fill buffer')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.t_put('fill buffer')

        def thread():
            time.sleep(0.1)
            closePutCh.close()
            closePutCh.t_get()

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([cancelGetCh,
                                  [closePutCh, 'success'],
                                  [cancelPutCh, 'noSend']], priority=True),
                         (True, closePutCh))
        self.assertEqual(closePutCh.t_get(), 'success')
        self.assertIsNone(closePutCh.t_get())

    def test_double_t_alts_successful_transfer(self):
        ch = self.chan(1)

        self.assertEqual(c.t_alts([[ch, 'success']]), (True, ch))
        self.assertEqual(c.t_alts([ch]), ('success', ch))

    def test_xform_state_is_not_modified_when_canceled(self):
        xformCh = self.chan(1, xf.take(2))
        xformCh.t_put('firstTake')
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.t_put('altsValue')

        threading.Thread(target=thread).start()
        self.assertEqual(c.t_alts([ch, [xformCh, 'do not modify xform state']],
                                  priority=True),
                         ('altsValue', ch))
        c.async_put(xformCh, 'secondTake')
        c.async_put(xformCh, 'dropMe')
        self.assertEqual(c.t_list(xformCh), ['firstTake', 'secondTake'])


class TestUnbufferedAltsChan(unittest.TestCase, AbstractTestUnbufferedAlts):
    @staticmethod
    def chan():
        return Chan()


class TestBufferedAltsChan(unittest.TestCase, AbstractTestBufferedAlts):
    @staticmethod
    def chan(n=1, xform=xf.identity):
        return Chan(c.buffer(n), xform)


class TestAltsThreads(unittest.TestCase):
    def test_t_alts_default_when_available(self):
        ch = chan(1)
        ch.t_put('success')
        self.assertEqual(c.t_alts([ch], default='ignore me'), ('success', ch))

    def test_t_alts_default_when_unavailable(self):
        ch = chan()
        self.assertEqual(c.t_alts([ch], default='success'),
                         ('success', 'default'))


class TestDroppingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.dropping_buffer(1))
        ch.t_put('keep')
        ch.t_put('drop')
        self.assertIs(ch.t_put('drop'), True)

    def test_buffer_keeps_oldest_n_elements(self):
        ch = chan(c.dropping_buffer(2))
        ch.t_put('keep1')
        ch.t_put('keep2')
        ch.t_put('drop')
        ch.close()
        self.assertEqual(c.t_list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.dropping_buffer(2), xf.cat)
        ch.t_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(c.t_list(ch), [1, 2])

    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.dropping_buffer(1)), True)


class TestSlidingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.sliding_buffer(1))
        ch.t_put('drop')
        ch.t_put('drop')
        self.assertIs(ch.t_put('keep'), True)

    def test_buffer_keeps_newest_n_elements(self):
        ch = chan(c.sliding_buffer(2))
        ch.t_put('drop')
        ch.t_put('keep1')
        ch.t_put('keep2')
        ch.close()
        self.assertEqual(c.t_list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.sliding_buffer(2), xf.cat)
        ch.t_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(c.t_list(ch), [3, 4])

    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.sliding_buffer(1)), True)


class TestPromiseBuffer(unittest.TestCase):
    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(buffers.PromiseBuffer()), True)


if __name__ == '__main__':
    unittest.main()
