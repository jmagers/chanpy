#!/usr/bin/env python3

import asyncio
import threading
import time
import unittest
import transducers as xf
import channel as c
from channel import chan
from toolz import identity
from genericfuncs import Reduced


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
            result = ch.t_get('success')

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
        return c.Chan(c.FixedBuffer(n))


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
            ch = self.chan(1, xf.takeWhile(lambda x: x != 2))
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
        ch = self.chan(3, xf.partitionAll(2))
        for i in range(3):
            ch.t_put(i)
        ch.close()
        self.assertEqual(c.t_list(ch), [(0, 1), (2,)])

    def test_close_does_not_flush_xform_with_pending_puts(self):
        ch = self.chan(1, xf.partitionAll(2))
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
    def chan(n, xform, ex_handler=c.nop_ex_handler):
        return c.Chan(c.FixedBuffer(n), xform, ex_handler)


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
        return c.Chan(c.FixedBuffer(n))


class TestChan(unittest.TestCase):
    def test_unsuccessful_nonpositive_buffer(self):
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
            self.chan(ex_handler=identity)


class TestUnbufferedBlockingChan(unittest.TestCase,
                                 AbstractTestUnbufferedBlocking):
    @staticmethod
    def chan():
        return c.Chan()


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
        return c.Chan()


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
        ch = c.promise_chan(xf.partitionAll(3))
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
        return c.Chan()


class TestBufferedAltsChan(unittest.TestCase, AbstractTestBufferedAlts):
    @staticmethod
    def chan(n=1, xform=identity):
        return c.Chan(c.FixedBuffer(n), xform)


class TestDroppingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.DroppingBuffer(1))
        ch.t_put('keep')
        ch.t_put('drop')
        self.assertIs(ch.t_put('drop'), True)

    def test_buffer_keeps_oldest_n_elements(self):
        ch = chan(c.DroppingBuffer(2))
        ch.t_put('keep1')
        ch.t_put('keep2')
        ch.t_put('drop')
        ch.close()
        self.assertEqual(c.t_list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.DroppingBuffer(2), xf.cat)
        ch.t_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(c.t_list(ch), [1, 2])

    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.DroppingBuffer(1)), True)


class TestSlidingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.SlidingBuffer(1))
        ch.t_put('drop')
        ch.t_put('drop')
        self.assertIs(ch.t_put('keep'), True)

    def test_buffer_keeps_newest_n_elements(self):
        ch = chan(c.SlidingBuffer(2))
        ch.t_put('drop')
        ch.t_put('keep1')
        ch.t_put('keep2')
        ch.close()
        self.assertEqual(c.t_list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.SlidingBuffer(2), xf.cat)
        ch.t_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(c.t_list(ch), [3, 4])

    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.SlidingBuffer(1)), True)


class TestPromiseBuffer(unittest.TestCase):
    def test_is_unblocking_buffer(self):
        self.assertIs(c.is_unblocking_buffer(c.SlidingBuffer(1)), True)


class TestMultAsyncio(unittest.TestCase):
    def test_tap(self):
        async def main():
            src, dest = chan(), chan()
            m = c.mult(src)
            m.tap(dest)
            await src.a_put('success')
            self.assertEqual(await dest.a_get(), 'success')
            src.close()

        asyncio.run(main())

    def test_untap(self):
        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            await src.a_put('item1')
            await dest1.a_get()
            await dest2.a_get()
            m.untap(dest2)
            await src.a_put('item2')
            await dest1.a_get()
            await asyncio.sleep(0.1)
            self.assertIsNone(dest2.poll())
            src.close()

        asyncio.run(main())

    def test_untap_all(self):
        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            await src.a_put('item')
            await dest1.a_get()
            await dest2.a_get()
            m.untap_all()
            self.assertIs(await src.a_put('dropMe'), True)
            await asyncio.sleep(0.1)
            self.assertIsNone(dest1.poll())
            self.assertIsNone(dest2.poll())

        asyncio.run(main())

    def test_untap_nonexistent_tap(self):
        async def main():
            src = chan()
            m = c.mult(src)
            self.assertIsNone(m.untap(chan()))
            src.close()

        asyncio.run(main())

    def test_mult_blocks_until_all_taps_accept(self):
        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            await src.a_put('item')
            await dest1.a_get()
            await asyncio.sleep(0.1)
            self.assertIs(src.offer('failure'), False)
            await dest2.a_get()
            src.close()

        asyncio.run(main())

    def test_only_correct_taps_close(self):
        async def main():
            src, close_dest, no_close_dest = chan(), chan(1), chan(1)
            m = c.mult(src)
            m.tap(close_dest)
            m.tap(no_close_dest, close=False)
            src.close()
            await asyncio.sleep(0.1)
            self.assertIs(await close_dest.a_put('closed'), False)
            self.assertIs(await no_close_dest.a_put('not closed'), True)

        asyncio.run(main())

    def test_tap_closes_when_added_after_mult_closes(self):
        async def main():
            src_ch, tap_ch = chan(), chan()
            m = c.mult(src_ch)
            src_ch.close()
            await asyncio.sleep(0.1)
            m.tap(tap_ch)
            self.assertIsNone(await tap_ch.a_get())

        asyncio.run(main())


class TestMultThread(unittest.TestCase):
    def test_tap(self):
        def thread(loop, src, dest):
            m = c.mult(src, loop=loop)
            m.tap(dest)
            src.t_put('success')

        async def main():
            src, dest = chan(), chan()
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread, args=(loop, src, dest)).start()
            self.assertEqual(await dest.a_get(), 'success')
            src.close()

        asyncio.run(main())

    def test_untap(self):
        def thread(loop, src, dest1, dest2):
            m = c.mult(src, loop=loop)
            m.tap(dest1)
            m.tap(dest2)
            src.t_put('item1')
            dest1.t_get()
            dest2.t_get()
            m.untap(dest2)
            src.t_put('item2')
            dest1.t_get()

        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread,
                             args=(loop, src, dest1, dest2)).start()
            await asyncio.sleep(0.1)
            self.assertIsNone(dest2.poll())
            src.close()

        asyncio.run(main())

    def test_untap_all(self):
        def thread(loop, src, dest1, dest2):
            m = c.mult(src, loop=loop)
            m.tap(dest1)
            m.tap(dest2)
            src.t_put('item')
            dest1.t_get()
            dest2.t_get()
            m.untap_all()

        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread,
                             args=(loop, src, dest1, dest2)).start()
            await asyncio.sleep(0.1)
            self.assertIs(await src.a_put('dropMe'), True)
            await asyncio.sleep(0.1)
            self.assertIsNone(dest1.poll())
            self.assertIsNone(dest2.poll())

        asyncio.run(main())

    def test_untap_nonexistent_tap(self):
        def thread(loop, src, complete):
            m = c.mult(src, loop=loop)
            m.untap(chan())
            src.close()
            complete.close()

        async def main():
            src, complete = chan(), chan()
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread, args=(loop, src, complete)).start()
            self.assertIsNone(await complete.a_get())

        asyncio.run(main())

    def test_mult_blocks_until_all_taps_accept(self):
        def thread(loop, src, dest1, dest2, complete):
            m = c.mult(src, loop=loop)
            m.tap(dest1)
            m.tap(dest2)
            src.t_put('item')
            dest1.t_get()
            time.sleep(0.1)
            self.assertIs(src.offer('failure'), False)
            dest2.t_get()
            src.close()
            complete.close()

        async def main():
            src, dest1, dest2, complete = chan(), chan(), chan(), chan()
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread,
                             args=(loop, src, dest1, dest2, complete)).start()
            self.assertIsNone(await complete.a_get())

        asyncio.run(main())

    def test_only_correct_taps_close(self):
        def thread(loop, src, close_dest, open_dest):
            m = c.mult(src, loop=loop)
            m.tap(close_dest)
            m.tap(open_dest, close=False)
            src.close()

        async def main():
            src, close_dest, open_dest = chan(), chan(1), chan(1)
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread,
                             args=(loop, src, close_dest, open_dest)).start()
            await asyncio.sleep(0.1)
            self.assertIs(await close_dest.a_put('closed'), False)
            self.assertIs(await open_dest.a_put('not closed'), True)

        asyncio.run(main())

    def test_tap_closes_when_added_after_mult_closes(self):
        def thread(loop, src_ch, tap_ch):
            m = c.mult(src_ch, loop=loop)
            src_ch.close()
            time.sleep(0.1)
            m.tap(tap_ch)

        async def main():
            src_ch, tap_ch = chan(), chan()
            loop = asyncio.get_running_loop()
            threading.Thread(target=thread,
                             args=(loop, src_ch, tap_ch)).start()
            self.assertIsNone(await tap_ch.a_get())

        asyncio.run(main())


class TestPubAsyncio(unittest.TestCase):
    def test_sub(self):
        async def main():
            from_ch = chan(1)
            a1_ch, a2_ch, b1_ch, b2_ch = chan(), chan(), chan(), chan()
            p = c.pub(from_ch, lambda x: x[0])
            p.sub('a', a1_ch)
            p.sub('a', a2_ch)
            p.sub('b', b1_ch)
            p.sub('b', b2_ch)

            await from_ch.a_put('apple')
            self.assertEqual(await a1_ch.a_get(), 'apple')
            self.assertEqual(await a2_ch.a_get(), 'apple')
            await from_ch.a_put('bat')
            self.assertEqual(await b1_ch.a_get(), 'bat')
            self.assertEqual(await b2_ch.a_get(), 'bat')

            await from_ch.a_put('ant')
            self.assertEqual(await a1_ch.a_get(), 'ant')
            self.assertEqual(await a2_ch.a_get(), 'ant')
            await from_ch.a_put('bear')
            self.assertEqual(await b1_ch.a_get(), 'bear')
            self.assertEqual(await b2_ch.a_get(), 'bear')

        asyncio.run(main())

    def test_unsub(self):
        async def main():
            from_ch = chan(1)
            a1_ch, a2_ch, b_ch = chan(), chan(), chan()
            p = c.pub(from_ch, lambda x: x[0])
            p.sub('a', a1_ch)
            p.sub('a', a2_ch)
            p.sub('b', b_ch)

            p.unsub('a', a2_ch)
            await from_ch.a_put('apple')
            self.assertEqual(await a1_ch.a_get(), 'apple')
            await from_ch.a_put('bat')
            self.assertEqual(await b_ch.a_get(), 'bat')
            await asyncio.sleep(0.1)
            self.assertIsNone(a2_ch.poll())

            p.sub('a', a2_ch)
            from_ch.a_put('air')
            self.assertEqual(await a2_ch.a_get(), 'air')

        asyncio.run(main())

    def test_unsub_nonexistent_topic(self):
        async def main():
            from_ch, to_ch = chan(1), chan()
            p = c.pub(from_ch, identity)
            p.sub('a', to_ch)

            p.unsub('b', to_ch)
            await from_ch.a_put('a')
            self.assertEqual(await to_ch.a_get(), 'a')

        asyncio.run(main())

    def test_unsub_nonexistent_ch(self):
        async def main():
            from_ch, to_ch = chan(1), chan()
            p = c.pub(from_ch, identity)
            p.sub('a', to_ch)

            p.unsub('b', chan())
            await from_ch.a_put('a')
            self.assertEqual(await to_ch.a_get(), 'a')

        asyncio.run(main())

    def test_unsub_all(self):
        async def main():
            from_ch, a_ch, b_ch = chan(2), chan(), chan()
            p = c.pub(from_ch, lambda x: x[0])
            p.sub('a', a_ch)
            p.sub('b', b_ch)

            p.unsub_all()
            await from_ch.a_put('apple')
            await from_ch.a_put('bat')
            await asyncio.sleep(0.1)
            self.assertIsNone(from_ch.poll())
            self.assertIsNone(a_ch.poll())
            self.assertIsNone(b_ch.poll())

            p.sub('a', a_ch)
            await from_ch.a_put('air')
            self.assertEqual(await a_ch.a_get(), 'air')

        asyncio.run(main())

    def test_unsub_all_topic(self):
        async def main():
            from_ch = chan(2)
            a1_ch, a2_ch, b_ch = chan(), chan(), chan()
            p = c.pub(from_ch, lambda x: x[0])
            p.sub('a', a1_ch)
            p.sub('a', a2_ch)
            p.sub('b', b_ch)

            p.unsub_all('a')
            await from_ch.a_put('apple')
            await from_ch.a_put('bat')
            await asyncio.sleep(0.1)
            self.assertIsNone(a1_ch.poll())
            self.assertIsNone(a2_ch.poll())
            self.assertEqual(b_ch.poll(), 'bat')
            self.assertIsNone(from_ch.poll())

            p.sub('a', a1_ch)
            await from_ch.a_put('air')
            self.assertEqual(await a1_ch.a_get(), 'air')

        asyncio.run(main())

    def test_only_correct_subs_get_closed(self):
        async def main():
            from_ch, close_ch, open_ch = chan(1), chan(1), chan(1)
            p = c.pub(from_ch, identity)
            p.sub('close', close_ch)
            p.sub('open', open_ch, close=False)

            from_ch.close()
            await asyncio.sleep(0.1)
            self.assertIs(await close_ch.a_put('fail'), False)
            self.assertIs(await open_ch.a_put('success'), True)

        asyncio.run(main())

    def test_buf_fn(self):
        async def main():
            from_ch = chan()
            a_ch, b_ch = chan(), chan()
            p = c.pub(from_ch, lambda x: x[0],
                      lambda x: None if x == 'a' else 2)

            p.sub('a', a_ch)
            p.sub('b', b_ch)
            await from_ch.a_put('a1')
            await from_ch.a_put('a2')
            await asyncio.sleep(0.1)
            self.assertIs(from_ch.offer('a fail'), False)
            self.assertEqual(await a_ch.a_get(), 'a1')
            self.assertEqual(await a_ch.a_get(), 'a2')
            await asyncio.sleep(0.1)
            self.assertIsNone(a_ch.poll())

            await from_ch.a_put('b1')
            await from_ch.a_put('b2')
            await from_ch.a_put('b3')
            await from_ch.a_put('b4')
            await asyncio.sleep(0.1)
            self.assertIs(from_ch.offer('b fail'), False)
            self.assertEqual(await b_ch.a_get(), 'b1')
            self.assertEqual(await b_ch.a_get(), 'b2')
            self.assertEqual(await b_ch.a_get(), 'b3')
            self.assertEqual(await b_ch.a_get(), 'b4')
            await asyncio.sleep(0.1)
            self.assertIsNone(b_ch.poll())

        asyncio.run(main())


class TestMixAsyncio(unittest.TestCase):
    def test_toggle_exceptions(self):
        async def main():
            ch = chan()
            m = c.mix(ch)
            with self.assertRaises(ValueError):
                m.toggle({'not a channel': {}})
            with self.assertRaises(ValueError):
                m.toggle({ch: {'invalid option': True}})
            with self.assertRaises(ValueError):
                m.toggle({ch: {'solo': 'not a boolean'}})
            with self.assertRaises(ValueError):
                m.toggle({ch: {'pause': 'not a boolean'}})
            with self.assertRaises(ValueError):
                m.toggle({ch: {'mute': 'not a boolean'}})

        asyncio.run(main())

    def test_solo_mode_exception(self):
        async def main():
            m = c.mix(chan())
            with self.assertRaises(ValueError):
                m.solo_mode('invalid mode')

        asyncio.run(main())

    def test_admix(self):
        async def main():
            from_ch1, from_ch2, to_ch = chan(), chan(), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch1)
            await from_ch1.a_put('from_ch1')
            self.assertEqual(await to_ch.a_get(), 'from_ch1')
            m.admix(from_ch2)
            await from_ch1.a_put('from_ch1 again')
            self.assertEqual(await to_ch.a_get(), 'from_ch1 again')
            await from_ch2.a_put('from_ch2')
            self.assertEqual(await to_ch.a_get(), 'from_ch2')

        asyncio.run(main())

    def test_unmix(self):
        async def main():
            from_ch1, from_ch2, to_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch1)
            await from_ch1.a_put('from_ch1')
            self.assertEqual(await to_ch.a_get(), 'from_ch1')
            m.admix(from_ch2)
            m.unmix(from_ch1)
            await from_ch2.a_put('from_ch2')
            self.assertEqual(await to_ch.a_get(), 'from_ch2')
            await from_ch1.a_put('remain in from_ch1')
            await asyncio.sleep(0.1)
            self.assertIsNone(to_ch.poll())
            self.assertEqual(await from_ch1.a_get(), 'remain in from_ch1')

        asyncio.run(main())

    def test_unmix_all(self):
        async def main():
            from_ch1, from_ch2, to_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch1)
            m.admix(from_ch2)
            await from_ch1.a_put('from_ch1')
            self.assertEqual(await to_ch.a_get(), 'from_ch1')
            await from_ch2.a_put('from_ch2')
            self.assertEqual(await to_ch.a_get(), 'from_ch2')
            m.unmix_all()
            await asyncio.sleep(0.1)
            await from_ch1.a_put('ignore from_ch1 item')
            await from_ch2.a_put('ignore from_ch2 item')
            await asyncio.sleep(0.1)
            self.assertIsNone(to_ch.poll())

        asyncio.run(main())

    def test_mute(self):
        async def main():
            unmuted_ch, muted_ch = chan(), chan()
            to_ch = chan(1)
            m = c.mix(to_ch)
            m.toggle({unmuted_ch: {'mute': False},
                      muted_ch: {'mute': True}})
            await unmuted_ch.a_put('not muted')
            self.assertEqual(await to_ch.a_get(), 'not muted')
            await muted_ch.a_put('mute me')
            self.assertIsNone(to_ch.poll())

            m.toggle({unmuted_ch: {'mute': True},
                      muted_ch: {'mute': False}})
            await muted_ch.a_put('the mute can now talk')
            self.assertEqual(await to_ch.a_get(), 'the mute can now talk')
            await unmuted_ch.a_put('i made a deal with Ursula')
            self.assertIsNone(to_ch.poll())

        asyncio.run(main())

    def test_pause(self):
        async def main():
            unpaused_ch, paused_ch, to_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.toggle({unpaused_ch: {'pause': False},
                      paused_ch: {'pause': True}})
            await unpaused_ch.a_put('not paused')
            self.assertEqual(await to_ch.a_get(), 'not paused')
            await paused_ch.a_put('remain in paused_ch')
            await asyncio.sleep(0.1)
            self.assertEqual(await paused_ch.a_get(), 'remain in paused_ch')

            m.toggle({unpaused_ch: {'pause': True},
                      paused_ch: {'pause': False}})
            await paused_ch.a_put('no longer paused')
            self.assertEqual(await to_ch.a_get(), 'no longer paused')
            await unpaused_ch.a_put('paused now')
            await asyncio.sleep(0.1)
            self.assertEqual(await unpaused_ch.a_get(), 'paused now')

        asyncio.run(main())

    def test_pause_dominates_mute(self):
        async def main():
            from_ch, to_ch = chan(1), chan(1)
            m = c.mix(to_ch)
            m.toggle({from_ch: {'pause': True, 'mute': True}})
            await from_ch.a_put('stay in from_ch')
            await asyncio.sleep(0.1)
            self.assertEqual(await from_ch.a_get(), 'stay in from_ch')

        asyncio.run(main())

    def test_solo_domintates_pause_and_mute(self):
        async def main():
            from_ch, to_ch = chan(), chan(1)
            m = c.mix(to_ch)
            m.toggle({from_ch: {'solo': True, 'pause': True, 'mute': True}})
            await from_ch.a_put('success')
            self.assertEqual(await to_ch.a_get(), 'success')

        asyncio.run(main())

    def test_solomode_mute(self):
        async def main():
            solo_ch1, solo_ch2, non_solo_ch = chan(), chan(), chan()
            to_ch = chan(1)
            m = c.mix(to_ch)

            m.solo_mode('mute')
            m.toggle({solo_ch1: {'solo': True},
                      solo_ch2: {'solo': True},
                      non_solo_ch: {}})
            await solo_ch1.a_put('solo_ch1 not muted')
            self.assertEqual(await to_ch.a_get(), 'solo_ch1 not muted')
            await solo_ch2.a_put('solo_ch2 not muted')
            self.assertEqual(await to_ch.a_get(), 'solo_ch2 not muted')
            await non_solo_ch.a_put('drop me')
            await asyncio.sleep(0.1)
            self.assertIsNone(to_ch.poll())

            m.toggle({solo_ch1: {'solo': False},
                      solo_ch2: {'solo': False}})
            await asyncio.sleep(0.1)
            await solo_ch1.a_put('solo_ch1 still not muted')
            self.assertEqual(await to_ch.a_get(), 'solo_ch1 still not muted')
            await solo_ch2.a_put('solo_ch2 still not muted')
            self.assertEqual(await to_ch.a_get(), 'solo_ch2 still not muted')
            await non_solo_ch.a_put('non_solo_ch not muted')
            self.assertEqual(await to_ch.a_get(), 'non_solo_ch not muted')

        asyncio.run(main())

    def test_solomode_pause(self):
        async def main():
            to_ch = chan(1)
            solo_ch1, solo_ch2, non_solo_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)

            m.solo_mode('pause')
            m.toggle({solo_ch1: {'solo': True},
                      solo_ch2: {'solo': True},
                      non_solo_ch: {}})
            await solo_ch1.a_put('solo_ch1 not paused')
            self.assertEqual(await to_ch.a_get(), 'solo_ch1 not paused')
            await solo_ch2.a_put('solo_ch2 not paused')
            self.assertEqual(await to_ch.a_get(), 'solo_ch2 not paused')
            await non_solo_ch.a_put('stay in non_solo_ch')
            await asyncio.sleep(0.1)
            self.assertEqual(await non_solo_ch.a_get(), 'stay in non_solo_ch')

            m.toggle({solo_ch1: {'solo': False},
                      solo_ch2: {'solo': False}})
            await asyncio.sleep(0.1)
            await solo_ch1.a_put('solo_ch1 still not paused')
            self.assertEqual(await to_ch.a_get(), 'solo_ch1 still not paused')
            await solo_ch2.a_put('solo_ch2 still not paused')
            self.assertEqual(await to_ch.a_get(), 'solo_ch2 still not paused')
            await non_solo_ch.a_put('non_solo_ch not paused')
            self.assertEqual(await to_ch.a_get(), 'non_solo_ch not paused')

        asyncio.run(main())

    def test_admix_unmix_toggle_do_not_interrupt_put(self):
        async def main():
            to_ch, from_ch = chan(), chan(1)
            admix_ch, unmix_ch, pause_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.toggle({from_ch: {}, unmix_ch: {}})

            # Start waiting put to to_ch
            await from_ch.a_put('successful transfer')
            await asyncio.sleep(0.1)

            # Apply operations while mix is waiting on to_ch
            m.admix(admix_ch)
            m.unmix(unmix_ch)
            m.toggle({pause_ch: {'pause': True}})

            # Confirm state is correct
            self.assertEqual(await to_ch.a_get(), 'successful transfer')

            await admix_ch.a_put('admix_ch added')
            self.assertEqual(await to_ch.a_get(), 'admix_ch added')

            await unmix_ch.a_put('unmix_ch removed')
            await asyncio.sleep(0.1)
            self.assertEqual(await unmix_ch.a_get(), 'unmix_ch removed')

            await pause_ch.a_put('pause_ch paused')
            await asyncio.sleep(0.1)
            self.assertEqual(await pause_ch.a_get(), 'pause_ch paused')

        asyncio.run(main())

    def test_to_ch_does_not_close_when_from_chs_do(self):
        async def main():
            from_ch, to_ch = chan(), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch)
            from_ch.close()
            await asyncio.sleep(0.1)
            self.assertIs(await to_ch.a_put('success'), True)

        asyncio.run(main())

    def test_mix_consumes_only_one_after_to_ch_closes(self):
        async def main():
            from_ch, to_ch = chan(1), chan()
            m = c.mix(to_ch)
            m.admix(from_ch)
            await asyncio.sleep(0.1)
            to_ch.close()
            await from_ch.a_put('mix consumes me')
            await from_ch.a_put('mix ignores me')
            await asyncio.sleep(0.1)
            self.assertEqual(await from_ch.a_get(), 'mix ignores me')

        asyncio.run(main())


class TestPipe(unittest.TestCase):
    def test_pipe_copy(self):
        async def main():
            src, dest = chan(), chan()
            c.pipe(src, dest)
            c.async_put(src, 1)
            c.async_put(src, 2)
            src.close()
            self.assertEqual(await c.a_list(dest), [1, 2])

        asyncio.run(main())

    def test_pipe_close_dest(self):
        async def main():
            src, dest = chan(), chan()
            c.pipe(src, dest)
            src.close()
            self.assertIsNone(await dest.a_get())

        asyncio.run(main())

    def test_complete_ch(self):
        async def main():
            src, dest = chan(), chan()
            complete_ch = c.pipe(src, dest)
            src.close()
            self.assertIsNone(await complete_ch.a_get())

        asyncio.run(main())

    def test_pipe_no_close_dest(self):
        async def main():
            src, dest = chan(), chan(1)
            complete_ch = c.pipe(src, dest, close=False)
            src.close()
            complete_ch.get()
            dest.a_put('success')
            self.assertEqual(await dest.a_get(), 'success')

    def test_stop_consuming_when_dest_closes(self):
        async def main():
            src, dest = chan(3), chan(1)
            c.onto_chan(src, ['intoDest1', 'intoDest2', 'dropMe'], close=False)
            c.pipe(src, dest)
            await asyncio.sleep(0.1)
            dest.close()
            self.assertEqual(await dest.a_get(), 'intoDest1')
            self.assertEqual(await dest.a_get(), 'intoDest2')
            self.assertIsNone(await dest.a_get())
            await asyncio.sleep(0.1)
            self.assertIsNone(src.poll())

        asyncio.run(main())


class TestReduce(unittest.TestCase):
    def test_empty_ch(self):
        async def main():
            ch = chan()
            ch.close()
            result_ch = c.reduce(lambda: None, 'init', ch)
            self.assertEqual(await result_ch.a_get(), 'init')

        asyncio.run(main())

    def test_non_empty_ch(self):
        async def main():
            in_ch = c.to_chan(range(4))
            result_ch = c.reduce(lambda x, y: x + y, 100, in_ch)
            self.assertEqual(await result_ch.a_get(), 106)

        asyncio.run(main())

    def test_reduced(self):
        async def main():
            in_ch = c.to_chan(range(4))

            def rf(result, val):
                if val == 2:
                    return Reduced(result + 2)
                return result + val

            result_ch = c.reduce(rf, 100, in_ch)
            self.assertEqual(await result_ch.a_get(), 103)

        asyncio.run(main())


class TestTransduce(unittest.TestCase):
    def test_xform_is_flushed_on_completion(self):
        async def main():
            ch = c.to_chan([1, 2, 3])

            def rf(result, val=None):
                if val is None:
                    return result
                result.append(val)
                return result

            result_ch = c.transduce(xf.partitionAll(2), rf, [], ch)
            self.assertEqual(await result_ch.a_get(), [(1, 2), (3,)])

        asyncio.run(main())

    def test_xform_early_termination(self):
        async def main():
            ch = c.to_chan([1, 2, 3])

            def rf(result, val=None):
                if val is None:
                    return result
                result.append(val)
                return result

            result_ch = c.transduce(xf.take(2), rf, [], ch)
            self.assertEqual(await result_ch.a_get(), [1, 2])

        asyncio.run(main())


class TestMerge(unittest.TestCase):
    def test_merge(self):
        async def main():
            src1, src2 = chan(), chan()
            m = c.merge([src1, src2], 2)
            await src1.a_put('src1')
            await src2.a_put('src2')
            src1.close()
            src2.close()
            self.assertEqual([x async for x in m], ['src1', 'src2'])

        asyncio.run(main())


class TestSplit(unittest.TestCase):
    def test_chans_close_with_closed_source(self):
        async def main():
            src_ch = chan()
            src_ch.close()
            t_ch, f_ch = c.split(lambda _: True, src_ch)
            self.assertIsNone(await t_ch.a_get())
            self.assertIsNone(await f_ch.a_get())

        asyncio.run(main())

    def test_true_false_chans(self):
        async def main():
            t_ch, f_ch = c.split(lambda x: x % 2 == 0,
                                 c.to_chan([1, 2, 3, 4]))
            self.assertEqual(await f_ch.a_get(), 1)
            self.assertEqual(await t_ch.a_get(), 2)
            self.assertEqual(await f_ch.a_get(), 3)
            self.assertEqual(await t_ch.a_get(), 4)
            self.assertIsNone(await f_ch.a_get())
            self.assertIsNone(await t_ch.a_get())

        asyncio.run(main())

    def test_bufs(self):
        async def main():
            t_ch, f_ch = c.split(lambda x: x % 2 == 0,
                                 c.to_chan([1, 2, 3, 4, 5]),
                                 2, 3)
            self.assertEqual(await c.a_list(t_ch), [2, 4])
            self.assertEqual(await c.a_list(f_ch), [1, 3, 5])

        asyncio.run(main())


class TestAsyncPut(unittest.TestCase):
    def test_return_true_if_buffer_not_full(self):
        self.assertIs(c.async_put(chan(1), 'val'), True)

    def test_returns_true_if_buffer_full_not_closed(self):
        self.assertIs(c.async_put(chan(), 'val'), True)

    def test_return_false_if_closed(self):
        ch = chan()
        ch.close()
        self.assertIs(c.async_put(ch, 'val'), False)

    def test_cb_called_if_buffer_full(self):
        ch = chan()
        prom = c.Promise()
        c.async_put(ch, 'val', prom.deliver)
        self.assertEqual(ch.t_get(), 'val')
        self.assertIs(prom.deref(), True)

    def test_cb_called_on_caller_if_buffer_not_full(self):
        prom = c.Promise()
        c.async_put(chan(1),
                    'val',
                    lambda x: prom.deliver([x, threading.get_ident()]))
        self.assertEqual(prom.deref(), [True, threading.get_ident()])

    def test_cb_called_on_different_thread_if_buffer_not_full(self):
        prom = c.Promise()
        c.async_put(chan(1),
                    'val',
                    lambda x: prom.deliver([x, threading.get_ident()]),
                    on_caller=False)
        val, thread_id = prom.deref()
        self.assertIs(val, True)
        self.assertNotEqual(thread_id, threading.get_ident())


class TestAsyncGet(unittest.TestCase):
    def test_return_none_if_buffer_not_empty(self):
        ch = chan(1)
        ch.t_put('val')
        self.assertIsNone(c.async_get(ch, identity))

    def test_return_none_if_buffer_empty(self):
        self.assertIsNone(c.async_get(chan(), identity))

    def test_return_none_if_closed(self):
        ch = chan()
        ch.close()
        self.assertIsNone(c.async_get(ch, identity))

    def test_cb_called_if_buffer_empty(self):
        prom = c.Promise()
        ch = chan()
        c.async_get(ch, prom.deliver)
        ch.t_put('val')
        self.assertEqual(prom.deref(), 'val')

    def test_cb_called_on_caller_if_buffer_not_empty(self):
        prom = c.Promise()
        ch = chan(1)
        ch.t_put('val')
        c.async_get(ch, lambda x: prom.deliver([x, threading.get_ident()]))
        self.assertEqual(prom.deref(), ['val', threading.get_ident()])

    def test_cb_called_on_different_thread_if_buffer_not_empty(self):
        prom = c.Promise()
        ch = chan(1)
        ch.t_put('val')
        c.async_get(ch,
                    lambda x: prom.deliver([x, threading.get_ident()]),
                    on_caller=False)
        val, thread_id = prom.deref()
        self.assertEqual(val, 'val')
        self.assertNotEqual(thread_id, threading.get_ident())


if __name__ == '__main__':
    unittest.main()
