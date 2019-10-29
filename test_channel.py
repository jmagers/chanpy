#!/usr/bin/env python3

import threading
import time
import unittest
from channel import chan


class TestBufferedChannel(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
