#!/usr/bin/env python3

import unittest
import xf


class TestTake(unittest.TestCase):
    def test_take_pos(self):
        taken = list(xf.xiter(xf.take(2), [1, 2, 3, 4]))
        self.assertEqual(taken, [1, 2])

    def test_take_zero(self):
        taken = list(xf.xiter(xf.take(0), [1, 2, 3, 4]))
        self.assertEqual(taken, [])

    def test_take_neg(self):
        taken = list(xf.xiter(xf.take(-1), [1, 2, 3, 4]))
        self.assertEqual(taken, [])

    def test_arity_zero(self):
        self.assertEqual(xf.take(1)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.take(3), xf.partition_all(2))
        taken = list(xf.xiter(xform, [1, 2, 3, 4]))
        self.assertEqual(list(taken), [(1, 2), (3,)])


class TestTakeWhile(unittest.TestCase):
    def test_take_some(self):
        taken = list(xf.xiter(xf.take_while(lambda x: x < 3), [1, 2, 3, 4]))
        self.assertEqual(taken, [1, 2])

    def test_take_none(self):
        taken = list(xf.xiter(xf.take_while(lambda x: x < 0), [1, 2, 3, 4]))
        self.assertEqual(taken, [])

    def test_pred_ignored_after_first_drop(self):
        xform = xf.take_while(lambda x: x < 0)
        taken = list(xf.xiter(xform, [-1, -2, 3, -4, -5]))
        self.assertEqual(taken, [-1, -2])

    def test_arity_zero(self):
        self.assertEqual(xf.take_while(xf.identity)(lambda: 'success')(),
                         'success')

    def test_complete(self):
        xform = xf.comp(xf.take_while(lambda x: x < 4), xf.partition_all(2))
        dropped = list(xf.xiter(xform, [1, 2, 3, 4, 5]))
        self.assertEqual(list(dropped), [(1, 2), (3,)])


class TestDrop(unittest.TestCase):
    def test_drop_pos(self):
        dropped = list(xf.xiter(xf.drop(2), [1, 2, 3, 4]))
        self.assertEqual(dropped, [3, 4])

    def test_drop_zero(self):
        dropped = list(xf.xiter(xf.drop(0), [1, 2, 3, 4]))
        self.assertEqual(dropped, [1, 2, 3, 4])

    def test_drop_neg(self):
        dropped = list(xf.xiter(xf.drop(-1), [1, 2, 3, 4]))
        self.assertEqual(dropped, [1, 2, 3, 4])

    def test_reduced(self):
        xform = xf.comp(xf.drop(2), xf.take(2))
        dropped = list(xf.xiter(xform, range(8)))
        self.assertEqual(dropped, [2, 3])

    def test_arity_zero(self):
        self.assertEqual(xf.drop(1)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.drop(2), xf.partition_all(2))
        dropped = list(xf.xiter(xform, [1, 2, 3, 4, 5]))
        self.assertEqual(list(dropped), [(3, 4), (5,)])


class TestDropWhile(unittest.TestCase):
    def test_drop_some(self):
        dropped = list(xf.xiter(xf.drop_while(lambda x: x < 3), [1, 2, 3, 4]))
        self.assertEqual(dropped, [3, 4])

    def test_drop_none(self):
        dropped = list(xf.xiter(xf.drop_while(lambda x: x < 0), [1, 2, 3, 4]))
        self.assertEqual(dropped, [1, 2, 3, 4])

    def test_pred_ignored_after_first_take(self):
        dropped = list(xf.xiter(xf.drop_while(lambda x: x < 3),
                                [1, 2, 3, -4, -5]))
        self.assertEqual(dropped, [3, -4, -5])

    def test_reduced(self):
        xform = xf.comp(xf.drop_while(lambda x: x < 3), xf.take(2))
        dropped = list(xf.xiter(xform, range(8)))
        self.assertEqual(list(dropped), [3, 4])

    def test_arity_zero(self):
        self.assertEqual(xf.drop_while(xf.identity)(lambda: 'success')(),
                         'success')

    def test_complete(self):
        xform = xf.comp(xf.drop_while(lambda x: x < 3), xf.partition_all(2))
        dropped = list(xf.xiter(xform, range(8)))
        self.assertEqual(list(dropped), [(3, 4), (5, 6), (7,)])


if __name__ == '__main__':
    unittest.main()
