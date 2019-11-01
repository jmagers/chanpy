#!/usr/bin/env python3

import threading
import time
import unittest
import transducers as xf
from channel import chan, ontoChan, mult


class TestBufferedChannel(unittest.TestCase):
    def test_unsuccessful_nonpositive_buffer(self):
        with self.assertRaises(ValueError):
            chan(0)

    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            chan(1).put(None)

    def test_unsuccessful_nonblocking_put_none(self):
        with self.assertRaises(TypeError):
            chan(1).put(None, block=False)

    def test_successful_blocking_get(self):
        ch = chan(1)
        threading.Thread(target=ch.put, args=['success']).start()
        self.assertEqual(ch.get(), 'success')

    def test_successful_blocking_put(self):
        self.assertIs(chan(1).put('success'), True)

    def test_successful_nonblocking_get(self):
        ch = chan(1)
        threading.Thread(target=ch.put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.get(block=False), 'success')

    def test_successful_nonblocking_put(self):
        ch = chan(1)

        def thread():
            time.sleep(0.1)
            ch.put('success', block=False)

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')

    def test_unsuccessful_nonblocking_get(self):
        self.assertIsNone(chan(1).get(block=False))

    def test_unsuccessful_nonblocking_put(self):
        ch = chan(1)
        ch.put('fill buffer')
        self.assertIs(ch.put('failure', block=False), False)

    def test_blocking_get_closed_empty_buffer(self):
        ch = chan(1)
        ch.close()
        self.assertIsNone(ch.get())

    def test_blocking_get_closed_full_buffer(self):
        ch = chan(1)
        ch.put('success')
        ch.close()
        self.assertEqual(ch.get(), 'success')

    def test_blocking_put_closed_empty_buffer(self):
        ch = chan(1)
        ch.close()
        self.assertIs(ch.put('failure'), False)

    def test_blocking_put_closed_full_buffer(self):
        ch = chan(1)
        ch.put('fill buffer')
        ch.close()
        self.assertIs(ch.put('failure'), False)

    def test_nonblocking_get_closed_empty_buffer(self):
        ch = chan(1)
        ch.close()
        self.assertIsNone(ch.get(block=False))

    def test_nonblocking_get_closed_full_buffer(self):
        ch = chan(1)
        ch.put('success')
        ch.close()
        self.assertEqual(ch.get(block=False), 'success')

    def test_nonblocking_put_closed_empty_buffer(self):
        ch = chan(1)
        ch.close()
        self.assertIs(ch.put('failure', block=False), False)

    def test_nonblocking_put_closed_full_buffer(self):
        ch = chan(1)
        ch.put('fill buffer')
        ch.close()
        self.assertIs(ch.put('failure', block=False), False)

    def test_close_while_blocking_get(self):
        ch = chan(1)

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.get())

    def test_close_while_blocking_put(self):
        ch = chan(1)
        ch.put('fill buffer')

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIs(ch.put('failure'), False)

    def test_iter(self):
        ch = chan(2)
        ontoChan(ch, ['one', 'two'])
        self.assertEqual(list(ch), ['one', 'two'])

    def test_xform_map(self):
        ch = chan(1, xf.map(lambda x: x + 1))
        ontoChan(ch, [0, 1, 2])
        self.assertEqual(list(ch), [1, 2, 3])

    def test_xform_filter(self):
        ch = chan(1, xf.filter(lambda x: x % 2 == 0))
        ontoChan(ch, [0, 1, 2])
        self.assertEqual(list(ch), [0, 2])

    def test_xform_early_termination(self):
        ch = chan(1, xf.take(2))
        ontoChan(ch, [1, 2, 3, 4])
        self.assertEqual(list(ch), [1, 2])

    def test_xform_successful_overfilled_buffer(self):
        ch = chan(1, xf.cat)
        ch.put([1, 2, 3])
        ch.close()
        self.assertEqual(list(ch), [1, 2, 3])

    def test_xform_unsuccessful_nonblocking_put_overfilled_buffer(self):
        ch = chan(1, xf.cat)
        ch.put([1, 2])
        self.assertIs(ch.put([1], block=False), False)

    def test_unsuccessful_transformation_to_none(self):
        ch = chan(1, xf.map(lambda _: None))
        with self.assertRaises(AssertionError):
            ch.put('failure')

    def test_close_flushes_xform_buffer(self):
        ch = chan(3, xf.partitionAll(2))
        ontoChan(ch, range(3))
        ch.close()
        self.assertEqual(list(ch), [(0, 1), (2,)])


class TestUnBufferedChannel(unittest.TestCase):
    def test_unsuccessful_blocking_put_none(self):
        with self.assertRaises(TypeError):
            chan().put(None)

    def test_unsuccessful_nonblocking_put_none(self):
        with self.assertRaises(TypeError):
            chan().put(None, block=False)

    def test_blocking_get_first(self):
        ch = chan()

        def thread():
            time.sleep(0.1)
            ch.put('success')

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')

    def test_blocking_put_first(self):
        ch = chan()

        def thread():
            time.sleep(0.1)
            ch.get()

        threading.Thread(target=thread).start()
        self.assertIs(ch.put('success'), True)

    def test_put_blocks_until_get(self):
        status = 'failure'
        ch = chan()

        def thread():
            nonlocal status
            time.sleep(0.1)
            status = 'success'
            ch.get()

        threading.Thread(target=thread).start()
        ch.put(1)
        self.assertEqual(status, 'success')

    def test_successful_nonblocking_get(self):
        ch = chan()
        threading.Thread(target=ch.put, args=['success']).start()
        time.sleep(0.1)
        self.assertEqual(ch.get(block=False), 'success')

    def test_successful_nonblocking_put(self):
        ch = chan()

        def thread():
            time.sleep(0.1)
            ch.put('success', block=False)

        threading.Thread(target=thread).start()
        self.assertEqual(ch.get(), 'success')

    def test_unsuccessful_nonblocking_get(self):
        self.assertIsNone(chan().get(block=False))

    def test_unsuccessful_nonblocking_put(self):
        self.assertIs(chan().put('failure', block=False), False)

    def test_blocking_get_after_close(self):
        ch = chan()
        ch.close()
        self.assertIsNone(ch.get())

    def test_blocking_put_after_close(self):
        ch = chan()
        ch.close()
        self.assertIs(ch.put('failure'), False)

    def test_nonblocking_get_after_close(self):
        ch = chan()
        ch.close()
        self.assertIsNone(ch.get(block=False))

    def test_nonblocking_put_after_close(self):
        ch = chan()
        ch.close()
        self.assertIs(ch.put('failure', block=False), False)

    def test_close_while_blocking_get(self):
        ch = chan()

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIsNone(ch.get())

    def test_close_while_blocking_put(self):
        ch = chan()

        def thread():
            time.sleep(0.1)
            ch.close()

        threading.Thread(target=thread).start()
        self.assertIs(ch.put('failure'), False)

    def test_iter(self):
        ch = chan()
        ontoChan(ch, ['one', 'two'])
        self.assertEqual(list(ch), ['one', 'two'])


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

    def test_taps_close(self):
        src, dest = chan(), chan()
        m = mult(src)
        m.tap(dest)
        src.close()
        self.assertIsNone(dest.get())


if __name__ == '__main__':
    unittest.main()
