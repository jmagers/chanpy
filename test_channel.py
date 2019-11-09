#!/usr/bin/env python3

import threading
import time
import unittest
import transducers as xf
import channel as c
from channel import chan, ontoChan, mult, pipe, merge


class AbstractTestBufferedBlockingCalls:
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan(1).put(None)

    def test_successful_blocking_get(self):
        ch = self.chan(1)
        threading.Thread(target=ch.put, args=['success']).start()
        self.assertEqual(ch.get(), 'success')

    def test_successful_blocking_put(self):
        self.assertIs(self.chan(1).put('success'), True)

    def test_blocking_get_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.get())

    def test_blocking_get_closed_full_buffer(self):
        ch = self.chan(1)
        ch.put('success')
        ch.close()
        self.assertEqual(ch.get(), 'success')

    def test_blocking_put_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.put('failure'), False)

    def test_blocking_put_closed_full_buffer(self):
        ch = self.chan(1)
        ch.put('fill buffer')
        ch.close()
        self.assertIs(ch.put('failure'), False)

    def test_close_while_blocking_get(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.get())

    def test_close_while_blocking_put(self):
        ch = self.chan(1)
        ch.put('fill buffer')

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIs(ch.put('failure'), False)

    def test_iter(self):
        ch = self.chan(2)
        ontoChan(ch, ['one', 'two'])
        self.assertEqual(list(ch), ['one', 'two'])


class TestBlockingBufferedChannel(unittest.TestCase,
                                  AbstractTestBufferedBlockingCalls):
    @staticmethod
    def chan(n):
        return c.BufferedChannel(c.FixedBuffer(n))


class TestBlockingMaybeBufferedChannel(unittest.TestCase,
                                       AbstractTestBufferedBlockingCalls):
    @staticmethod
    def chan(n):
        return c.MaybeBufferedChannel(c.FixedBuffer(n))


class AbstractTestXform:
    def test_xform_map(self):
        ch = self.chan(1, xf.map(lambda x: x + 1))
        ontoChan(ch, [0, 1, 2])
        self.assertEqual(list(ch), [1, 2, 3])

    def test_xform_filter(self):
        ch = self.chan(1, xf.filter(lambda x: x % 2 == 0))
        ontoChan(ch, [0, 1, 2])
        self.assertEqual(list(ch), [0, 2])

    def test_xform_early_termination(self):
        ch = self.chan(1, xf.take(2))
        ontoChan(ch, [1, 2, 3, 4])
        self.assertEqual(list(ch), [1, 2])

    def test_xform_successful_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.put([1, 2, 3])
        ch.close()
        self.assertEqual(list(ch), [1, 2, 3])

    def test_xform_unsuccessful_nonblocking_put_overfilled_buffer(self):
        ch = self.chan(1, xf.cat)
        ch.put([1, 2])
        self.assertIs(ch.put([1], block=False), False)

    def test_unsuccessful_transformation_to_none(self):
        ch = self.chan(1, xf.map(lambda _: None))
        with self.assertRaises(AssertionError):
            ch.put('failure')

    def test_close_flushes_xform_buffer(self):
        ch = self.chan(3, xf.partitionAll(2))
        ontoChan(ch, range(3))
        ch.close()
        self.assertEqual(list(ch), [(0, 1), (2,)])


class TestXformBufferedChannel(unittest.TestCase, AbstractTestXform):
    @staticmethod
    def chan(n, xform):
        return c.BufferedChannel(c.FixedBuffer(n), xform)


class TestXformMaybeBufferedChannel(unittest.TestCase, AbstractTestXform):
    @staticmethod
    def chan(n, xform):
        return c.MaybeBufferedChannel(c.FixedBuffer(n), xform)


class AbstractTestBufferedNonblockingCalls:
    def test_unsuccessful_nonblocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan(1).put(None, block=False)

    def test_successful_nonblocking_get(self):
        ch = self.chan(1)
        threading.Thread(target=ch.put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.get(block=False), 'success')

    def test_successful_nonblocking_put(self):
        ch = self.chan(1)

        def thread():
            time.sleep(0.1)
            ch.put('success', block=False)

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')

    def test_unsuccessful_nonblocking_get(self):
        self.assertIsNone(self.chan(1).get(block=False))

    def test_unsuccessful_nonblocking_put(self):
        ch = self.chan(1)
        ch.put('fill buffer')
        self.assertIs(ch.put('failure', block=False), False)

    def test_nonblocking_get_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIsNone(ch.get(block=False))

    def test_nonblocking_get_closed_full_buffer(self):
        ch = self.chan(1)
        ch.put('success')
        ch.close()
        self.assertEqual(ch.get(block=False), 'success')

    def test_nonblocking_put_closed_empty_buffer(self):
        ch = self.chan(1)
        ch.close()
        self.assertIs(ch.put('failure', block=False), False)

    def test_nonblocking_put_closed_full_buffer(self):
        ch = self.chan(1)
        ch.put('fill buffer')
        ch.close()
        self.assertIs(ch.put('failure', block=False), False)


class TestBufferedNonBlockingCalls(unittest.TestCase,
                                   AbstractTestBufferedNonblockingCalls):
    @staticmethod
    def chan(n):
        return c.BufferedChannel(c.FixedBuffer(n))


class TestMaybeBufferedNonblockingCalls(unittest.TestCase,
                                        AbstractTestBufferedNonblockingCalls):
    @staticmethod
    def chan(n):
        return c.MaybeBufferedChannel(c.FixedBuffer(n))


class TestChan(unittest.TestCase):
    def test_unsuccessful_nonpositive_buffer(self):
        with self.assertRaises(ValueError):
            chan(0)


class AbstractTestUnbufferedBlockingCalls:
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan().put(None)

    def test_blocking_get_first(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')

    def test_blocking_put_first(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.put('success'), True)

    def test_put_blocks_until_get(self):
        status = 'failure'
        ch = self.chan()

        def thread():
            nonlocal status
            time.sleep(0.1)
            status = 'success'
            ch.get()

        threading.Thread(target=thread).start()
        ch.put(1)
        self.assertEqual(status, 'success')

    def test_blocking_get_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.get())

    def test_blocking_put_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.put('failure'), False)

    def test_close_while_blocking_get(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.get())

    def test_close_while_blocking_put(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIs(ch.put('failure'), False)

    def test_iter(self):
        ch = self.chan()
        ontoChan(ch, ['one', 'two'])
        self.assertEqual(list(ch), ['one', 'two'])

    def test_xform_exception(self):
        with self.assertRaises(TypeError):
            self.chan(None, xf.cat)


class TestUnbufferedBlockingCalls(unittest.TestCase,
                                  AbstractTestUnbufferedBlockingCalls):
    chan = c.UnbufferedChannel


class TestMaybeUnbufferedBlockingCalls(unittest.TestCase,
                                       AbstractTestUnbufferedBlockingCalls):
    chan = c.MaybeUnbufferedChannel


class AbstractTestUnbufferedNonblockingCalls:
    def test_unsuccessful_nonblocking_put_none(self):
        with self.assertRaises(TypeError):
            self.chan().put(None, block=False)

    def test_successful_nonblocking_get(self):
        ch = self.chan()
        threading.Thread(target=ch.put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.get(block=False), 'success')

    def test_successful_nonblocking_put(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.put('success', block=False)

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')

    def test_unsuccessful_nonblocking_get(self):
        self.assertIsNone(self.chan().get(block=False))

    def test_unsuccessful_nonblocking_put(self):
        self.assertIs(self.chan().put('failure', block=False), False)

    def test_nonblocking_get_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIsNone(ch.get(block=False))

    def test_nonblocking_put_after_close(self):
        ch = self.chan()
        ch.close()
        self.assertIs(ch.put('failure', block=False), False)


class TestUnbufferedNonblocking(unittest.TestCase,
                                AbstractTestUnbufferedNonblockingCalls):
    chan = c.UnbufferedChannel


class TestMaybeUnbufferedNonblocking(unittest.TestCase,
                                     AbstractTestUnbufferedNonblockingCalls):
    chan = c.MaybeUnbufferedChannel


class AbstractTestAlts:
    def _confirm_chans_not_closed(self, *chs):
        for ch in chs:
            ontoChan(ch, ['notClosed'], close=False)
            self.assertEqual(ch.get(), 'notClosed')

    def test_single_successful_get_on_initial_request(self):
        ch = self.chan()
        ontoChan(ch, ['success', 'notClosed'])
        time.sleep(0.1)
        self.assertEqual(c.alts([ch]), ('success', ch))
        self.assertEqual(ch.get(), 'notClosed')

    def test_single_successful_get_on_wait(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ontoChan(ch, ['success', 'notClosed'])

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([ch]), ('success', ch))
        self.assertEqual(ch.get(), 'notClosed')

    def test_single_successful_put_on_initial_request(self):
        ch = self.chan()

        def thread():
            time.sleep(0.1)
            ch.put(c.alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')
        self.assertEqual(ch.get(), (True, ch))

    def test_get_put_same_channel(self):
        ch = self.chan()
        with self.assertRaises(ValueError):
            c.alts([ch, [ch, 'success']])


class AbstractTestUnbufferedAlts(AbstractTestAlts):
    def test_single_successful_put_on_wait(self):
        ch = self.chan()

        def thread():
            ch.put(c.alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(ch.get(), 'success')
        self.assertEqual(ch.get(), (True, ch))

    def test_multiple_successful_get_on_initial_request(self):
        successGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()
        ontoChan(successGetCh, ['success'], close=False)
        time.sleep(0.1)
        self.assertEqual(c.alts([cancelGetCh,
                                 successGetCh,
                                 [cancelPutCh, 'noSend']]),
                         ('success', successGetCh))
        self._confirm_chans_not_closed(successGetCh, cancelGetCh, cancelPutCh)

    def test_multiple_successful_get_on_wait(self):
        successGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            successGetCh.put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([cancelGetCh,
                                 successGetCh,
                                 [cancelPutCh, 'noSend']]),
                         ('success', successGetCh))
        self._confirm_chans_not_closed(successGetCh, cancelGetCh, cancelPutCh)

    def test_multiple_successful_put_on_initial_requst(self):
        successPutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            successPutCh.put(c.alts([cancelGetCh,
                                     [successPutCh, 'success'],
                                     [cancelPutCh, 'noSend']]))

        threading.Thread(target=thread).start()
        self.assertEqual(successPutCh.get(), 'success')
        self.assertEqual(successPutCh.get(), (True, successPutCh))
        self._confirm_chans_not_closed(cancelGetCh, successPutCh, cancelPutCh)

    def test_multiple_successful_put_on_wait(self):
        successPutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            successPutCh.put(c.alts([cancelGetCh,
                                     [successPutCh, 'success'],
                                     [cancelPutCh, 'noSend']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(successPutCh.get(), 'success')
        self.assertEqual(successPutCh.get(), (True, successPutCh))
        self._confirm_chans_not_closed(cancelGetCh, successPutCh, cancelPutCh)

    def test_close_before_get(self):
        closedGetCh = self.chan()
        cancelPutCh = self.chan()
        cancelGetCh = self.chan()
        closedGetCh.close()
        self.assertEqual(c.alts([[cancelPutCh, 'noSend'],
                                 closedGetCh,
                                 cancelGetCh]),
                         (None, closedGetCh))
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_close_before_put(self):
        closedPutCh = self.chan()
        cancelPutCh = self.chan()
        cancelGetCh = self.chan()
        closedPutCh.close()
        self.assertEqual(c.alts([cancelGetCh,
                                 [closedPutCh, 'noSend'],
                                 [cancelPutCh, 'noSend']]),
                         (False, closedPutCh))
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_close_while_waiting_get(self):
        closeGetCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            closeGetCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([cancelGetCh,
                                 closeGetCh,
                                 [cancelPutCh, 'noSend']]),
                         (None, closeGetCh))
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_close_while_waiting_put(self):
        closePutCh = self.chan()
        cancelGetCh = self.chan()
        cancelPutCh = self.chan()

        def thread():
            time.sleep(0.1)
            closePutCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([cancelGetCh,
                                 [closePutCh, 'noSend'],
                                 [cancelPutCh, 'noSend']]),
                         (False, closePutCh))
        self._confirm_chans_not_closed(cancelPutCh, cancelGetCh)

    def test_double_alts_successful_transfer(self):
        ch = self.chan()

        def thread():
            ch.put(c.alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([ch]), ('success', ch))
        self.assertEqual(ch.get(), (True, ch))


class AbstractTestBufferedAlts(AbstractTestAlts):
    def test_single_successful_put_on_wait(self):
        ch = self.chan(1)
        ch.put('fill buffer')

        def thread():
            ch.put(c.alts([[ch, 'success']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(ch.get(), 'fill buffer')
        self.assertEqual(ch.get(), 'success')
        self.assertEqual(ch.get(), (True, ch))

    def test_multiple_successful_get_on_initial_request(self):
        successGetCh = self.chan(1)
        successGetCh.put('success')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')

        self.assertEqual(c.alts([cancelGetCh,
                                 successGetCh,
                                 [cancelPutCh, 'noSend']]),
                         ('success', successGetCh))

    def test_multiple_successful_get_on_wait(self):
        successGetCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')

        def thread():
            time.sleep(0.1)
            successGetCh.put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([cancelGetCh,
                                 successGetCh,
                                 [cancelPutCh, 'noSend']]),
                         ('success', successGetCh))

    def test_multiple_successful_put_on_intial_request(self):
        successPutCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')

        altsValue = c.alts([cancelGetCh,
                            [cancelPutCh, 'noSend'],
                            [successPutCh, 'success']])

        self.assertEqual(altsValue, (True, successPutCh))
        self.assertEqual(successPutCh.get(), 'success')

    def test_multiple_successful_put_on_wait(self):
        successPutCh = self.chan(1)
        successPutCh.put('fill buffer')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')

        def thread():
            successPutCh.put(c.alts([cancelGetCh,
                                     [successPutCh, 'success'],
                                     [cancelPutCh, 'noSend']]))

        threading.Thread(target=thread).start()
        time.sleep(0.1)
        self.assertEqual(successPutCh.get(), 'fill buffer')
        self.assertEqual(successPutCh.get(), 'success')
        self.assertEqual(successPutCh.get(), (True, successPutCh))

    def test_close_before_get(self):
        closedGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')
        cancelGetCh = self.chan(1)
        closedGetCh.close()
        self.assertEqual(c.alts([[cancelPutCh, 'noSend'],
                                 closedGetCh,
                                 cancelGetCh]),
                         (None, closedGetCh))

    def test_close_before_put(self):
        closedPutCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')
        cancelGetCh = self.chan(1)
        closedPutCh.close()
        self.assertEqual(c.alts([cancelGetCh,
                                 [closedPutCh, 'noSend'],
                                 [cancelPutCh, 'noSend']]),
                         (False, closedPutCh))

    def test_close_while_waiting_get(self):
        closeGetCh = self.chan(1)
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')

        def thread():
            time.sleep(0.1)
            closeGetCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([cancelGetCh,
                                 closeGetCh,
                                 [cancelPutCh, 'noSend']]),
                         (None, closeGetCh))

    def test_close_while_waiting_put(self):
        closePutCh = self.chan(1)
        closePutCh.put('fill buffer')
        cancelGetCh = self.chan(1)
        cancelPutCh = self.chan(1)
        cancelPutCh.put('fill buffer')

        def thread():
            time.sleep(0.1)
            closePutCh.close()

        threading.Thread(target=thread).start()
        self.assertEqual(c.alts([cancelGetCh,
                                 [closePutCh, 'noSend'],
                                 [cancelPutCh, 'noSend']]),
                         (False, closePutCh))

    def test_double_alts_successful_transfer(self):
        ch = self.chan(1)

        self.assertEqual(c.alts([[ch, 'success']]), (True, ch))
        self.assertEqual(c.alts([ch]), ('success', ch))


class TestAltsMaybeUnbuffered(unittest.TestCase, AbstractTestUnbufferedAlts):
    chan = c.MaybeUnbufferedChannel


class TestAltsMaybeBufferedAlts(unittest.TestCase, AbstractTestBufferedAlts):
    @staticmethod
    def chan(n=1):
        return c.MaybeBufferedChannel(c.FixedBuffer(n))


class TestDroppingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.DroppingBuffer(1))
        ch.put('keep')
        ch.put('drop')
        self.assertIs(ch.put('drop'), True)

    def test_buffer_keeps_oldest_n_elements(self):
        ch = chan(c.DroppingBuffer(2))
        ch.put('keep1')
        ch.put('keep2')
        ch.put('drop')
        ch.close()
        self.assertEqual(list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.DroppingBuffer(2), xf.cat)
        ch.put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(list(ch), [1, 2])


class TestSlidingBuffer(unittest.TestCase):
    def test_put_does_not_block(self):
        ch = chan(c.SlidingBuffer(1))
        ch.put('drop')
        ch.put('drop')
        self.assertIs(ch.put('keep'), True)

    def test_buffer_keeps_newest_n_elements(self):
        ch = chan(c.SlidingBuffer(2))
        ch.put('drop')
        ch.put('keep1')
        ch.put('keep2')
        ch.close()
        self.assertEqual(list(ch), ['keep1', 'keep2'])

    def test_buffer_does_not_overfill_with_xform(self):
        ch = chan(c.SlidingBuffer(2), xf.cat)
        ch.put([1, 2, 3, 4])
        ch.close()
        self.assertEqual(list(ch), [3, 4])


class TestMult(unittest.TestCase):
    def test_tap(self):
        src, dest = chan(), chan()
        m = mult(src)
        m.tap(dest)
        src.put('success')
        self.assertEqual(dest.get(), 'success')
        src.close()

    def test_untap(self):
        src, dest1, dest2 = chan(), chan(), chan()
        m = mult(src)
        m.tap(dest1)
        m.tap(dest2)
        src.put('item1')
        dest1.get()
        dest2.get()
        m.untap(dest2)
        src.put('item2')
        dest1.get()
        time.sleep(0.1)
        self.assertIsNone(dest2.get(block=False))
        src.close()

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
        src.put('item')
        dest1.get()
        time.sleep(0.1)
        self.assertIs(src.put('failure', block=False), False)
        dest2.get()
        src.close()

    def test_only_correct_taps_close(self):
        src, closeDest, noCloseDest = chan(), chan(1), chan(1)
        m = mult(src)
        m.tap(closeDest)
        m.tap(noCloseDest, close=False)
        src.close()
        time.sleep(0.1)
        self.assertIs(closeDest.put('closed'), False)
        self.assertIs(noCloseDest.put('not closed'), True)


class TestPipe(unittest.TestCase):
    def test_pipe_copy(self):
        src, dest = chan(), chan()
        pipe(src, dest)
        ontoChan(src, [1, 2, 3])
        self.assertEqual(list(dest), [1, 2, 3])

    def test_pipe_close_dest(self):
        src, dest = chan(), chan()
        pipe(src, dest)
        src.close()
        self.assertIsNone(dest.get())

    def test_pipe_no_close_dest(self):
        src, dest = chan(), chan(1)
        pipe(src, dest, close=False)
        src.close()
        time.sleep(0.1)
        dest.put('success')
        self.assertEqual(dest.get(), 'success')

    def test_stop_consuming_when_dest_closes(self):
        src, dest = chan(3), chan(1)
        src.put('intoDest')
        src.put('dropMe')
        src.put('remainInSrc')
        pipe(src, dest)
        time.sleep(0.1)
        dest.close()
        self.assertEqual(dest.get(), 'intoDest')
        self.assertIsNone(dest.get())
        self.assertEqual(src.get(), 'remainInSrc')


class TestMerge(unittest.TestCase):
    def test_merge(self):
        src1, src2 = chan(), chan()
        m = merge([src1, src2], 2)
        src1.put('src1')
        src2.put('src2')
        src1.close()
        src2.close()
        self.assertEqual(list(m), ['src1', 'src2'])


if __name__ == '__main__':
    unittest.main()
