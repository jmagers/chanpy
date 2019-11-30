#!/usr/bin/env python3

import unittest
import xf


append_rf = xf.multi_arity(list, xf.identity, lambda x, y: x.append(y) or x)
sum_rf = xf.multi_arity(lambda: 0, xf.identity, lambda x, y: x + y)


class TestPartitionAll(unittest.TestCase):
    def test_partition_every(self):
        xform = xf.partition_all(1)
        self.assertEqual(list(xf.xiter(xform, range(3))), [(0,), (1,), (2,)])

    def test_partition_pos(self):
        xform = xf.partition_all(3)
        self.assertEqual(list(xf.xiter(xform, range(6))),
                         [(0, 1, 2), (3, 4, 5)])

    def test_partition_empty(self):
        xform = xf.partition_all(1)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_partition_n_fraction(self):
        with self.assertRaises(ValueError):
            xf.partition_all(1.5)

    def test_partition_n_zero(self):
        with self.assertRaises(ValueError):
            xf.partition_all(0)

    def test_partition_n_neg(self):
        with self.assertRaises(ValueError):
            xf.partition_all(-1)

    def test_partition_step_fraction(self):
        with self.assertRaises(ValueError):
            xf.partition_all(1, 1.5)

    def test_partition_step_zero(self):
        with self.assertRaises(ValueError):
            xf.partition_all(1, 0)

    def test_partition_step_neg(self):
        with self.assertRaises(ValueError):
            xf.partition_all(1, -1)

    def test_reduced(self):
        xform = xf.comp(xf.partition_all(1), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, range(12))), [(0,), (1,)])

    def test_arity_zero(self):
        self.assertEqual(xf.partition_all(1)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.partition_all(3)
        self.assertEqual(list(xf.xiter(xform, range(5))), [(0, 1, 2), (3, 4)])

    def test_partition_with_smaller_step(self):
        xform = xf.partition_all(3, 1)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4, 5])),
                         [(1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5), (5,)])

    def test_partition_with_smaller_step_reduced_during_complete(self):
        xform = xf.comp(xf.partition_all(3, 1), xf.take(4))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4, 5])),
                         [(1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5)])

    def test_partition_with_larger_step(self):
        xform = xf.partition_all(2, 4)
        self.assertEqual(list(xf.xiter(xform, range(1, 10))),
                         [(1, 2), (5, 6), (9,)])


class TestTake(unittest.TestCase):
    def test_take_pos(self):
        taken = list(xf.xiter(xf.take(2), [1, 2, 3, 4]))
        self.assertEqual(taken, [1, 2])

    def test_take_too_many(self):
        taken = list(xf.xiter(xf.take(10), [1, 2, 3, 4]))
        self.assertEqual(taken, [1, 2, 3, 4])

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
        self.assertEqual(xf.take_while(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.take_while(lambda x: x < 4), xf.partition_all(2))
        dropped = list(xf.xiter(xform, [1, 2, 3, 4, 5]))
        self.assertEqual(list(dropped), [(1, 2), (3,)])


class TestTakeNth(unittest.TestCase):
    def test_take_every(self):
        xform = xf.take_nth(1)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [1, 2, 3, 4])

    def test_take_few(self):
        xform = xf.take_nth(3)
        self.assertEqual(list(xf.xiter(xform, range(12))), [0, 3, 6, 9])

    def test_empty(self):
        xform = xf.take_nth(1)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_take_fraction(self):
        with self.assertRaises(ValueError):
            xf.take_nth(1.5)

    def test_take_zero(self):
        with self.assertRaises(ValueError):
            xf.take_nth(0)

    def test_take_nega(self):
        with self.assertRaises(ValueError):
            xf.take_nth(-1)

    def test_reduced(self):
        xform = xf.comp(xf.take_nth(3), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, range(12))), [0, 3])

    def test_arity_zero(self):
        self.assertEqual(xf.take_nth(1)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.take_nth(1), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [(1, 2), (3,)])


class TestDrop(unittest.TestCase):
    def test_drop_pos(self):
        dropped = list(xf.xiter(xf.drop(2), [1, 2, 3, 4]))
        self.assertEqual(dropped, [3, 4])

    def test_drop_too_many(self):
        dropped = list(xf.xiter(xf.drop(10), [1, 2, 3, 4]))
        self.assertEqual(dropped, [])

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
        self.assertEqual(xf.drop_while(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.drop_while(lambda x: x < 3), xf.partition_all(2))
        dropped = list(xf.xiter(xform, range(8)))
        self.assertEqual(list(dropped), [(3, 4), (5, 6), (7,)])


class TestMap(unittest.TestCase):
    def test_map_some(self):
        xform = xf.map(lambda x: x * 2)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [2, 4, 6])

    def test_map_none(self):
        xform = xf.map(None)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.map(lambda x: x * 2), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [2, 4])

    def test_arity_zero(self):
        self.assertEqual(xf.map(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.map(lambda x: x * 2), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [(2, 4), (6,)])


class TestMapIndexed(unittest.TestCase):
    def test_map_some(self):
        xform = xf.map_indexed(lambda i, x: {i: x})
        self.assertEqual(list(xf.xiter(xform, ['zero', 'one', 'two'])),
                         [{0: 'zero'}, {1: 'one'}, {2: 'two'}])

    def test_map_empty(self):
        xform = xf.map_indexed(None)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.map_indexed(lambda i, x: {i: x}), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, ['zero', 'one', '_', '_'])),
                         [{0: 'zero'}, {1: 'one'}])

    def test_arity_zero(self):
        self.assertEqual(xf.map_indexed(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.map_indexed(lambda i, x: {i: x}),
                        xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, ['zero', 'one', 'two'])),
                         [({0: 'zero'}, {1: 'one'}), ({2: 'two'},)])


class TestFilter(unittest.TestCase):
    def test_filter_some(self):
        xform = xf.filter(lambda x: x % 2 == 0)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [2, 4])

    def test_filter_none(self):
        xform = xf.filter(lambda x: x % 2 == 0)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.filter(lambda x: x % 2 == 0), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4, 5, 6])), [2, 4])

    def test_arity_zero(self):
        self.assertEqual(xf.filter(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.filter(lambda x: x % 2 == 0), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [2, 4, 5, 6])), [(2, 4), (6,)])


class TestRemove(unittest.TestCase):
    def test_remove_some(self):
        xform = xf.remove(lambda x: x % 2 == 0)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [1, 3])

    def test_remove_none(self):
        xform = xf.remove(lambda x: x % 2 == 0)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.remove(lambda x: x % 2 == 0), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4, 5])), [1, 3])

    def test_arity_zero(self):
        self.assertEqual(xf.remove(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.remove(lambda x: x % 2 == 0), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 5])), [(1, 3), (5,)])


class TestKeep(unittest.TestCase):
    def test_keep_some(self):
        xform = xf.keep(lambda x: x if x % 2 == 0 else None)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [2, 4])

    def test_keep_empty(self):
        xform = xf.keep(lambda x: x if x % 2 == 0 else None)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.keep(lambda x: x if x % 2 == 0 else None),
                        xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4, 5, 6])), [2, 4])

    def test_arity_zero(self):
        self.assertEqual(xf.keep(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.keep(lambda x: x if x % 2 == 0 else None),
                        xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [2, 4, 5, 6])), [(2, 4), (6,)])


class TestKeepIndexed(unittest.TestCase):
    @staticmethod
    def even_set(i, x):
        return {x} if x % 2 == 0 else None

    def test_keep_some(self):
        xform = xf.keep_indexed(self.even_set)
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [{2}, {4}])

    def test_keep_empty(self):
        xform = xf.keep_indexed(None)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.keep_indexed(self.even_set), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [2, 3, 4, 5, 6])), [{2}, {4}])

    def test_arity_zero(self):
        self.assertEqual(xf.keep_indexed(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.keep_indexed(self.even_set), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [2, 4, 5, 6])),
                         [({2}, {4}), ({6},)])


class TestCat(unittest.TestCase):
    def test_cat_some(self):
        self.assertEqual(list(xf.xiter(xf.cat, [[1, 2, 3], [4, 5]])),
                         [1, 2, 3, 4, 5])

    def test_cat_none(self):
        self.assertEqual(list(xf.xiter(xf.cat, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.cat, xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [[1, 2], [3]])), [1, 2])

    def test_arity_zero(self):
        self.assertEqual(xf.cat(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.cat, xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [[1, 2], [3]])), [(1, 2), (3,)])


class TestMapcat(unittest.TestCase):
    def test_mapcat_some(self):
        xform = xf.mapcat(lambda x: [x, x * 2])
        self.assertEqual(list(xf.xiter(xform, [1, 4, 16])),
                         [1, 2, 4, 8, 16, 32])

    def test_mapcat_none(self):
        xform = xf.mapcat(lambda x: [x, x * 2])
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.mapcat(lambda x: [x, x * 2]), xf.take(3))
        self.assertEqual(list(xf.xiter(xform, [1, 4, 16])), [1, 2, 4])

    def test_arity_zero(self):
        self.assertEqual(xf.mapcat(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.mapcat(lambda x: [x, x * 2, x * 3]),
                        xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1])), [(1, 2), (3,)])


class TestDistinct(unittest.TestCase):
    def test_remove_duplicates(self):
        self.assertEqual(list(xf.xiter(xf.distinct, [1, 2, 3, 2, 1, 3, 4, 5])),
                         [1, 2, 3, 4, 5])

    def test_none(self):
        self.assertEqual(list(xf.xiter(xf.distinct, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.distinct, xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 1, 2, 3, 4, 5])), [1, 2])

    def test_arity_zero(self):
        self.assertEqual(xf.distinct(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.distinct, xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 1, 2, 3, 3])),
                         [(1, 2), (3,)])


class TestDedupe(unittest.TestCase):
    def test_remove_duplicates(self):
        self.assertEqual(list(xf.xiter(xf.dedupe, [1, 1, 1, 2, 2, 3, 2, 3])),
                         [1, 2, 3, 2, 3])

    def test_none(self):
        self.assertEqual(list(xf.xiter(xf.dedupe, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.dedupe, xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 1, 2, 2, 3, 4])), [1, 2])

    def test_arity_zero(self):
        self.assertEqual(xf.dedupe(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.dedupe, xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 2, 3])), [(1, 2), (3,)])


class TestPartitionBy(unittest.TestCase):
    def test_partition_some(self):
        xform = xf.partition_by(lambda x: x % 2 == 0)
        self.assertEqual(list(xf.xiter(xform, [1, 3, 5, 2, 4, 8, 9])),
                         [(1, 3, 5), (2, 4, 8), (9,)])

    def test_partition_none(self):
        xform = xf.partition_by(None)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.partition_by(lambda x: x % 2 == 0), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 3, 2, 4, 5, 7])),
                         [(1, 3), (2, 4)])

    def test_arity_zero(self):
        self.assertEqual(xf.partition_by(None)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.partition_by(lambda x: x % 2 == 0), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [2, 4, 6, 1, 3, 5, 8])),
                         [(2, 4, 6), (1, 3, 5)])


class TestReductions(unittest.TestCase):
    def test_reductions_some(self):
        xform = xf.reductions(lambda x, y: x + y, 1)
        self.assertEqual(list(xf.xiter(xform, [2, 3])), [1, 3, 6])

    def test_reductions_init_only(self):
        xform = xf.reductions(lambda x, y: x + y, 'success')
        self.assertEqual(list(xf.xiter(xform, [])), ['success'])

    def test_reductions_init_only_complete(self):
        xform = xf.comp(xf.reductions(lambda x, y: x + y, [1, 2, 3]),
                        xf.cat,
                        xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [])), [(1, 2), (3,)])

    def test_reductions_init_only_reduced(self):
        xform = xf.comp(xf.reductions(lambda x, y: x + y, 'success'),
                        xf.take(1))
        self.assertEqual(list(xf.xiter(xform, [])), ['success'])

    def test_reductions_reduced(self):
        xform = xf.comp(xf.reductions(lambda x, y: x + y, 1), xf.take(3))
        self.assertEqual(list(xf.xiter(xform, [2, 3, 4, 5])), [1, 3, 6])

    def test_arity_zero(self):
        self.assertEqual(xf.reductions(xf.identity, 1)(lambda: 'success')(),
                         'success')

    def test_complete(self):
        xform = xf.comp(xf.reductions(lambda x, y: x + y, 1),
                        xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [2, 3])), [(1, 3), (6,)])


class TestInterpose(unittest.TestCase):
    def test_interpose_some(self):
        xform = xf.interpose('s')
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [1, 's', 2, 's', 3])

    def test_interpose_empty(self):
        xform = xf.interpose('s')
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.interpose('s'), xf.take(3))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [1, 's', 2])

    def test_arity_zero(self):
        self.assertEqual(xf.interpose('s')(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.interpose('s'), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2])), [(1, 's'), (2,)])


class TestReplace(unittest.TestCase):
    def test_replace_some(self):
        xform = xf.replace({1: 'one', 2: 'two'})
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), ['one', 'two', 3])

    def test_empty(self):
        xform = xf.replace({1: 'one'})
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.replace({1: 'one'}), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), ['one', 2])

    def test_arity_zero(self):
        self.assertEqual(xf.replace({})(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.replace({1: 'one'}), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [('one', 2), (3,)])


class TestRandomSample(unittest.TestCase):
    def test_1(self):
        xform = xf.random_sample(1)
        self.assertEqual(list(xf.xiter(xform, range(100))), list(range(100)))

    def test_0(self):
        xform = xf.random_sample(0)
        self.assertEqual(list(xf.xiter(xform, range(100))), [])

    def test_neg(self):
        xform = xf.random_sample(-1)
        self.assertEqual(list(xf.xiter(xform, range(100))), [])

    def test_gt_1(self):
        xform = xf.random_sample(2)
        self.assertEqual(list(xf.xiter(xform, range(100))), list(range(100)))

    def test_fraction(self):
        xform = xf.random_sample(0.5)
        vals = set(range(1000))
        results = set(xf.xiter(xform, vals))
        self.assertTrue(results.issubset(vals))
        self.assertTrue(0 < len(results) < 1000)  # Very unlikely to be false

    def test_empty(self):
        xform = xf.random_sample(1)
        self.assertEqual(list(xf.xiter(xform, [])), [])

    def test_reduced(self):
        xform = xf.comp(xf.random_sample(1), xf.take(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3, 4])), [1, 2])

    def test_arity_zero(self):
        self.assertEqual(xf.random_sample(1)(lambda: 'success')(), 'success')

    def test_complete(self):
        xform = xf.comp(xf.random_sample(1), xf.partition_all(2))
        self.assertEqual(list(xf.xiter(xform, [1, 2, 3])), [(1, 2), (3,)])


class TestCompleting(unittest.TestCase):
    def test_default_cf(self):
        rf = xf.completing(xf.multi_arity(lambda: 0, None, lambda x, y: x + y))
        self.assertEqual(rf(), 0)
        self.assertEqual(rf(1, 2), 3)
        self.assertEqual(rf('success'), 'success')

    def test_custom_cf(self):
        rf = xf.completing(xf.multi_arity(lambda: 0, None, lambda x, y: x + y),
                           str)
        self.assertEqual(rf(), 0)
        self.assertEqual(rf(1, 2), 3)
        self.assertEqual(rf(100), '100')


class TestIreduce(unittest.TestCase):
    def test_some_no_init(self):
        result = xf.ireduce(sum_rf, [1, 2, 3, 8])
        self.assertEqual(result, 14)

    def test_empty_no_init(self):
        result = xf.ireduce(sum_rf, [])
        self.assertEqual(result, 0)

    def test_no_init_no_arity_zero(self):
        with self.assertRaises(TypeError):
            result = xf.ireduce(lambda x, y: x + y, [])
            self.assertEqual(result, 0)


class TestItransduce(unittest.TestCase):
    def test_itransduce_some(self):
        result = xf.itransduce(xf.filter(lambda x: x % 2 == 0),
                               sum_rf,
                               1,
                               [2, 3, 8])
        self.assertEqual(result, 11)

    def test_itransduce_some_no_init(self):
        result = xf.itransduce(xf.filter(lambda x: x % 2 == 0),
                               sum_rf,
                               [1, 2, 3, 8])
        self.assertEqual(result, 10)

    def test_itransduce_empty_no_init(self):
        result = xf.itransduce(xf.filter(lambda x: x % 2 == 0), sum_rf, [])
        self.assertEqual(result, 0)

    def test_itransduce_empty_no_init_no_arity_zero(self):
        with self.assertRaises(TypeError):
            xf.itransduce(xf.filter(lambda x: x % 2 == 0),
                          lambda x, y: x + y,
                          [])

    def test_itransduce_init_only(self):
        result = xf.itransduce(xf.filter(None), xf.identity, 1, [])
        self.assertEqual(result, 1)

    def test_itransduce_init_only_complete(self):
        def xform(rf):
            return lambda result: rf(result + 100)

        result = xf.itransduce(xform, xf.identity, 1, [])
        self.assertEqual(result, 101)

    def test_itransduce_reduced(self):
        result = xf.itransduce(xf.take(2), sum_rf, 1, [2, 3, 100])
        self.assertEqual(result, 6)

    def test_complete(self):
        result = xf.itransduce(xf.partition_all(2), append_rf, [], [1, 2, 3])
        self.assertEqual(result, [(1, 2), (3,)])


if __name__ == '__main__':
    unittest.main()
