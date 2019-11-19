#!/usr/bin/env python3

import asyncio
import threading
import time
import unittest
import transducers as xf
import channel as c
from channel import chan, mult, pipe
from toolz import identity


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
            go = c.Go()
            get_ch = go.get(getter(ch))
            self.assertIs(await ch.a_put('success'), True)
            self.assertEqual(await get_ch.a_get(), 'success')

        asyncio.run(main())

    def test_nonblocking_unsuccessful_get(self):
        async def main():
            ch = chan()
            self.assertIsNone(await ch.a_get(block=False))

        asyncio.run(main())

    def test_nonblocking_successful_put(self):
        ch = chan(1)

        async def main():
            return await ch.a_put('success', block=False)

        self.assertIs(asyncio.run(main()), True)
        self.assertEqual(ch.t_get(), 'success')

    def test_go_from_different_thread(self):
        def getter_thread(go, ch):
            async def getter():
                return await ch.a_get()

            return go.get(getter()).t_get()

        async def main():
            go = c.Go()
            ch = chan()
            thread_result_ch = c.thread_call(lambda: getter_thread(go, ch))
            self.assertIs(await ch.a_put('success'), True)
            self.assertEqual(await thread_result_ch.a_get(), 'success')

        asyncio.run(main())

    def test_a_alts_get_no_wait(self):
        get_ch, put_ch = chan(), chan()

        async def putter():
            await get_ch.a_put('success')

        async def main():
            c.Go()(putter())
            await asyncio.sleep(0.1)
            return await c.a_alts([[put_ch, 'noSend'], get_ch], priority=True)

        self.assertEqual(asyncio.run(main()), ('success', get_ch))

    def test_a_alts_put_after_wait(self):
        get_ch, put_ch = chan(), chan()

        async def putter():
            await asyncio.sleep(0.1)
            await put_ch.a_get()

        async def main():
            c.Go()(putter())
            return await c.a_alts([[put_ch, 'success'], get_ch], priority=True)

        self.assertEqual(asyncio.run(main()), (True, put_ch))


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
        self.assertEqual(list(ch), ['one', 'two'])


class TestBufferedBlockingChan(unittest.TestCase,
                               AbstractTestBufferedBlocking):
    @staticmethod
    def chan(n):
        return c.Chan(c.FixedBuffer(n))


class AbstractTestXform:
    def test_xform_map(self):
        async def main():
            ch = self.chan(1, xf.map(lambda x: x + 1))
            c.onto_chan(c.Go(), ch, [0, 1, 2])
            self.assertEqual(await c.a_list(ch), [1, 2, 3])

        asyncio.run(main())

    def test_xform_filter(self):
        async def main():
            ch = self.chan(1, xf.filter(lambda x: x % 2 == 0))
            c.onto_chan(c.Go(), ch, [0, 1, 2])
            self.assertEqual(await c.a_list(ch), [0, 2])

        asyncio.run(main())

    def test_xform_early_termination(self):
        async def main():
            ch = self.chan(1, xf.take(2))
            c.onto_chan(c.Go(), ch, [1, 2, 3, 4])
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
        self.assertEqual(list(ch), [1, 2, 3])

    def test_xform_unsuccessful_nonblocking_put_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.t_put([1, 2])
        self.assertIs(ch.t_put([1], block=False), False)

    def test_unsuccessful_transformation_to_none(self):
        ch = self.chan(1, xf.map(lambda _: None))
        with self.assertRaises(AssertionError):
            ch.t_put('failure')

    def test_close_flushes_xform_buffer(self):
        ch = self.chan(3, xf.partitionAll(2))
        for i in range(3):
            ch.t_put(i)
        ch.close()
        self.assertEqual(list(ch), [(0, 1), (2,)])

    def test_close_does_not_flush_xform_with_pending_puts(self):
        ch = self.chan(1, xf.partitionAll(2))
        for i in range(3):
            c.async_put(ch, i)
        ch.close()
        self.assertEqual(list(ch), [(0, 1), (2,)])


class TestXformBufferedChan(unittest.TestCase, AbstractTestXform):
    @staticmethod
    def chan(n, xform):
        return c.Chan(c.FixedBuffer(n), xform)


class AbstractTestBufferedNonblocking:
    def test_unsuccessful_nonblocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan(1).t_put(None, block=False)

    def test_successful_nonblocking_get(self):
        ch = self.chan(1)
        threading.Thread(target=ch.t_put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.t_get(block=False), 'success')

    def test_successful_nonblocking_put(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.t_put('success', block=False)

        threading.Thread(target=thread).start()
        self.assertEqual(ch.t_get(), 'success')

    def test_unsuccessful_nonblocking_get(self):
        self.assertIsNone(self.chan(1).t_get(block=False))

    def test_unsuccessful_nonblocking_put(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')
        self.assertIs(ch.t_put('failure', block=False), False)

    def test_nonblocking_get_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.t_get(block=False))

    def test_nonblocking_get_closed_full_buffer(self):
        ch = self.chan(1)
        ch.t_put('success')
        ch.close()
        self.assertEqual(ch.t_get(block=False), 'success')

    def test_nonblocking_put_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.t_put('failure', block=False), False)

    def test_nonblocking_put_closed_full_buffer(self):
        ch = self.chan(1)
        ch.t_put('fill buffer')
        ch.close()
        self.assertIs(ch.t_put('failure', block=False), False)


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
        self.assertEqual(list(ch), ['one', 'two'])

    def test_xform_exception(self):
        with self.assertRaises(TypeError):
            self.chan(None, xf.cat)


class TestUnbufferedBlockingChan(unittest.TestCase,
                                 AbstractTestUnbufferedBlocking):
    @staticmethod
    def chan():
        return c.Chan()


class AbstractTestUnbufferedNonblocking:
    def test_unsuccessful_nonblocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan().t_put(None, block=False)

    def test_successful_nonblocking_get(self):
        ch = self.chan()
        threading.Thread(target=ch.t_put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.t_get(block=False), 'success')

    def test_successful_nonblocking_put(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.t_put('success', block=False)

        threading.Thread(target=thread).start()
        self.assertEqual(ch.t_get(), 'success')

    def test_unsuccessful_nonblocking_get(self):
        self.assertIsNone(self.chan().t_get(block=False))

    def test_unsuccessful_nonblocking_put(self):
        self.assertIs(self.chan().t_put('failure', block=False), False)

    def test_nonblocking_get_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.t_get(block=False))

    def test_nonblocking_put_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.t_put('failure', block=False), False)


class TestUnbufferedNonblockingChan(unittest.TestCase,
                                    AbstractTestUnbufferedNonblocking):
    @staticmethod
    def chan():
        return c.Chan()


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
        self.assertEqual(list(xformCh), ['firstTake', 'secondTake'])


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
        self.assertEqual(list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.DroppingBuffer(2), xf.cat)
        ch.t_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(list(ch), [1, 2])


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
        self.assertEqual(list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.SlidingBuffer(2), xf.cat)
        ch.t_put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(list(ch), [3, 4])


class TestMult(unittest.TestCase):
    def test_tap(self):
        src, dest = chan(), chan()
        m = mult(src)
        m.tap(dest)
        src.t_put('success')
        self.assertEqual(dest.t_get(), 'success')
        src.close()

    def test_untap(self):
        src, dest1, dest2 = chan(), chan(), chan()
        m = mult(src)
        m.tap(dest1)
        m.tap(dest2)
        src.t_put('item1')
        dest1.t_get()
        dest2.t_get()
        m.untap(dest2)
        src.t_put('item2')
        dest1.t_get()
        time.sleep(0.1)
        self.assertIsNone(dest2.t_get(block=False))
        src.close()

    def test_untapAll(self):
        src, dest1, dest2 = chan(), chan(), chan()
        m = mult(src)
        m.tap(dest1)
        m.tap(dest2)
        src.t_put('item')
        dest1.t_get()
        dest2.t_get()
        m.untapAll()
        self.assertIs(src.t_put("dropMe"), True)
        time.sleep(0.1)
        self.assertIsNone(dest1.t_get(block=False))
        self.assertIsNone(dest2.t_get(block=False))

    def test_untap_nonexistant_tap(self):
        src = chan()
        m = mult(src)
        self.assertIsNone(m.untap(chan()))
        src.close()

    def test_mult_blocks_until_all_taps_accept(self):
        src, dest1, dest2 = chan(), chan(), chan()
        m = mult(src)
        m.tap(dest1)
        m.tap(dest2)
        src.t_put('item')
        dest1.t_get()
        time.sleep(0.1)
        self.assertIs(src.t_put('failure', block=False), False)
        dest2.t_get()
        src.close()

    def test_only_correct_taps_close(self):
        src, closeDest, noCloseDest = chan(), chan(1), chan(1)
        m = mult(src)
        m.tap(closeDest)
        m.tap(noCloseDest, close=False)
        src.close()
        time.sleep(0.1)
        self.assertIs(closeDest.t_put('closed'), False)
        self.assertIs(noCloseDest.t_put('not closed'), True)

    def test_tap_closes_when_added_after_mult_closes(self):
        srcCh, tapCh = chan(), chan()
        m = mult(srcCh)
        srcCh.close()
        time.sleep(0.1)
        m.tap(tapCh)
        self.assertIsNone(tapCh.t_get())


class TestMix(unittest.TestCase):
    def test_toggle_exceptions(self):
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

    def test_solo_mode_exception(self):
        m = c.mix(chan())
        with self.assertRaises(ValueError):
            m.solo_mode('invalid mode')

    def test_admix(self):
        fromCh1, fromCh2, toCh = chan(), chan(), chan(1)
        m = c.mix(toCh)
        m.admix(fromCh1)
        fromCh1.t_put('fromCh1')
        self.assertEqual(toCh.t_get(), 'fromCh1')
        m.admix(fromCh2)
        fromCh1.t_put('fromCh1 again')
        self.assertEqual(toCh.t_get(), 'fromCh1 again')
        fromCh2.t_put('fromCh2')
        self.assertEqual(toCh.t_get(), 'fromCh2')

    def test_unmix(self):
        fromCh1, fromCh2, toCh = chan(1), chan(1), chan(1)
        m = c.mix(toCh)
        m.admix(fromCh1)
        fromCh1.t_put('fromCh1')
        self.assertEqual(toCh.t_get(), 'fromCh1')
        m.admix(fromCh2)
        m.unmix(fromCh1)
        fromCh2.t_put('fromCh2')
        self.assertEqual(toCh.t_get(), 'fromCh2')
        fromCh1.t_put('remain in fromCh1')
        time.sleep(0.1)
        self.assertIsNone(toCh.t_get(block=False))
        self.assertEqual(fromCh1.t_get(), 'remain in fromCh1')

    def test_unmix_all(self):
        fromCh1, fromCh2, toCh = chan(1), chan(1), chan(1)
        m = c.mix(toCh)
        m.admix(fromCh1)
        m.admix(fromCh2)
        fromCh1.t_put('fromCh1')
        self.assertEqual(toCh.t_get(), 'fromCh1')
        fromCh2.t_put('fromCh2')
        self.assertEqual(toCh.t_get(), 'fromCh2')
        m.unmix_all()
        time.sleep(0.1)
        fromCh1.t_put('ignore fromCh1 item')
        fromCh2.t_put('ignore fromCh2 item')
        time.sleep(0.1)
        self.assertIsNone(toCh.t_get(block=False))

    def test_mute(self):
        unmutedCh, mutedCh = chan(), chan()
        toCh = chan(1)
        m = c.mix(toCh)
        m.toggle({unmutedCh: {'mute': False},
                  mutedCh: {'mute': True}})
        unmutedCh.t_put('not muted')
        self.assertEqual(toCh.t_get(), 'not muted')
        mutedCh.t_put('mute me')
        self.assertIsNone(toCh.t_get(block=False))

        m.toggle({unmutedCh: {'mute': True},
                  mutedCh: {'mute': False}})
        mutedCh.t_put('the mute can now talk')
        self.assertEqual(toCh.t_get(), 'the mute can now talk')
        unmutedCh.t_put('i made a deal with Ursula')
        self.assertIsNone(toCh.t_get(block=False))

    def test_pause(self):
        unpausedCh, pausedCh, toCh = chan(1), chan(1), chan(1)
        m = c.mix(toCh)
        m.toggle({unpausedCh: {'pause': False},
                  pausedCh: {'pause': True}})
        unpausedCh.t_put('not paused')
        self.assertEqual(toCh.t_get(), 'not paused')
        pausedCh.t_put('remain in pausedCh')
        time.sleep(0.1)
        self.assertEqual(pausedCh.t_get(), 'remain in pausedCh')

        m.toggle({unpausedCh: {'pause': True},
                  pausedCh: {'pause': False}})
        pausedCh.t_put('no longer paused')
        self.assertEqual(toCh.t_get(), 'no longer paused')
        unpausedCh.t_put('paused now')
        time.sleep(0.1)
        self.assertEqual(unpausedCh.t_get(), 'paused now')

    def test_pause_dominates_mute(self):
        fromCh, toCh = chan(1), chan(1)
        m = c.mix(toCh)
        m.toggle({fromCh: {'pause': True, 'mute': True}})
        fromCh.t_put('stay in fromCh')
        time.sleep(0.1)
        self.assertEqual(fromCh.t_get(), 'stay in fromCh')

    def test_solo_domintates_pause_and_mute(self):
        fromCh, toCh = chan(), chan(1)
        m = c.mix(toCh)
        m.toggle({fromCh: {'solo': True, 'pause': True, 'mute': True}})
        fromCh.t_put('success')
        self.assertEqual(toCh.t_get(), 'success')

    def test_solomode_mute(self):
        soloCh1, soloCh2, nonSoloCh = chan(), chan(), chan()
        toCh = chan(1)
        m = c.mix(toCh)

        m.toggle({soloCh1: {'solo': True},
                  soloCh2: {'solo': True},
                  nonSoloCh: {}})
        m.solo_mode('mute')
        soloCh1.t_put('soloCh1 not muted')
        self.assertEqual(toCh.t_get(), 'soloCh1 not muted')
        soloCh2.t_put('soloCh2 not muted')
        self.assertEqual(toCh.t_get(), 'soloCh2 not muted')
        nonSoloCh.t_put('drop me')
        self.assertIsNone(nonSoloCh.t_get(block=False))
        self.assertIsNone(toCh.t_get(block=False))

        m.toggle({soloCh1: {'solo': False},
                  soloCh2: {'solo': False}})
        soloCh1.t_put('soloCh1 still not muted')
        self.assertEqual(toCh.t_get(), 'soloCh1 still not muted')
        soloCh2.t_put('soloCh2 still not muted')
        self.assertEqual(toCh.t_get(), 'soloCh2 still not muted')
        nonSoloCh.t_put('nonSoloCh not muted')
        self.assertEqual(toCh.t_get(), 'nonSoloCh not muted')

    def test_solomode_pause(self):
        soloCh1, soloCh2, nonSoloCh, toCh = chan(1), chan(1), chan(1), chan(1)
        m = c.mix(toCh)

        m.toggle({soloCh1: {'solo': True},
                  soloCh2: {'solo': True},
                  nonSoloCh: {}})
        m.solo_mode('pause')
        soloCh1.t_put('soloCh1 not paused')
        self.assertEqual(toCh.t_get(), 'soloCh1 not paused')
        soloCh2.t_put('soloCh2 not paused')
        self.assertEqual(toCh.t_get(), 'soloCh2 not paused')
        nonSoloCh.t_put('stay in nonSoloCh')
        time.sleep(0.1)
        self.assertEqual(nonSoloCh.t_get(), 'stay in nonSoloCh')

        m.toggle({soloCh1: {'solo': False},
                  soloCh2: {'solo': False}})
        soloCh1.t_put('soloCh1 still not paused')
        self.assertEqual(toCh.t_get(), 'soloCh1 still not paused')
        soloCh2.t_put('soloCh2 still not paused')
        self.assertEqual(toCh.t_get(), 'soloCh2 still not paused')
        nonSoloCh.t_put('nonSoloCh not paused')
        self.assertEqual(toCh.t_get(), 'nonSoloCh not paused')

    def test_admix_unmix_toggle_do_not_interrupt_put(self):
        toCh = chan()
        fromCh, admixCh, unmixCh, pauseCh = chan(1), chan(1), chan(1), chan(1)
        m = c.mix(toCh)
        m.toggle({fromCh: {}, unmixCh: {}})

        # Start blocking put
        fromCh.t_put('successful transfer')
        time.sleep(0.1)

        # Apply operations while mix is waiting on toCh
        m.admix(admixCh)
        m.unmix(unmixCh)
        m.toggle({pauseCh: {'pause': True}})

        # Confirm state is correct
        self.assertEqual(toCh.t_get(), 'successful transfer')

        admixCh.t_put('admixCh added')
        self.assertEqual(toCh.t_get(), 'admixCh added')

        unmixCh.t_put('unmixCh removed')
        time.sleep(0.1)
        self.assertEqual(unmixCh.t_get(), 'unmixCh removed')

        pauseCh.t_put('pauseCh paused')
        time.sleep(0.1)
        self.assertEqual(pauseCh.t_get(), 'pauseCh paused')

    def test_toCh_does_not_close_when_fromChs_do(self):
        fromCh, toCh = chan(), chan(1)
        m = c.mix(toCh)
        m.admix(fromCh)
        fromCh.close()
        time.sleep(0.1)
        self.assertIs(toCh.t_put('success'), True)

    def test_mix_consumes_only_one_after_toCh_closes(self):
        fromCh, toCh = chan(1), chan()
        m = c.mix(toCh)
        m.admix(fromCh)
        toCh.close()
        fromCh.t_put('mix consumes me')
        fromCh.t_put('mix ignores me')
        time.sleep(0.1)
        self.assertEqual(fromCh.t_get(), 'mix ignores me')


class TestPipe(unittest.TestCase):
    def test_pipe_copy(self):
        src, dest = chan(), chan()
        pipe(src, dest)
        c.async_put(src, 1)
        c.async_put(src, 2)
        src.close()
        self.assertEqual(list(dest), [1, 2])

    def test_pipe_close_dest(self):
        src, dest = chan(), chan()
        pipe(src, dest)
        src.close()
        self.assertIsNone(dest.t_get())

    def test_pipe_no_close_dest(self):
        src, dest = chan(), chan(1)
        pipe(src, dest, close=False)
        src.close()
        time.sleep(0.1)
        dest.t_put('success')
        self.assertEqual(dest.t_get(), 'success')

    def test_stop_consuming_when_dest_closes(self):
        src, dest = chan(3), chan(1)
        src.t_put('intoDest1')
        src.t_put('intoDest2')
        src.t_put('dropMe')
        pipe(src, dest)
        time.sleep(0.1)
        dest.close()
        self.assertEqual(dest.t_get(), 'intoDest1')
        self.assertEqual(dest.t_get(), 'intoDest2')
        self.assertIsNone(dest.t_get())
        time.sleep(0.1)
        self.assertIsNone(src.t_get(block=False))


class TestMerge(unittest.TestCase):
    def test_merge(self):
        async def main():
            go = c.Go()
            src1, src2 = chan(), chan()
            m = c.merge(go, [src1, src2], 2)
            await src1.a_put('src1')
            await src2.a_put('src2')
            src1.close()
            src2.close()
            self.assertEqual([x async for x in m], ['src1', 'src2'])

        asyncio.run(main())


if __name__ == '__main__':
    unittest.main()
