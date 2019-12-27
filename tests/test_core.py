#!/usr/bin/env python3

# Copyright 2019 Jake Magers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import threading
import time
import unittest
import chanpy as c
from chanpy import chan
from chanpy import transducers as xf
from concurrent.futures import ThreadPoolExecutor


async def a_list(ch):
    return await c.to_list(ch).get()


class TestThreadCall(unittest.TestCase):
    def setUp(self):
        c.set_loop(asyncio.new_event_loop())

    def tearDown(self):
        c.get_loop().close()
        c.set_loop(None)

    def test_non_none_return_value(self):
        def thread():
            return 'success'

        ch = c.thread(thread)
        self.assertEqual(ch.b_get(), 'success')
        self.assertIsNone(ch.b_get())

    def test_none_return_value(self):
        def thread():
            return None

        ch = c.thread(thread)
        self.assertIsNone(ch.b_get())

    def test_executor(self):
        def thread():
            time.sleep(0.1)
            return threading.current_thread().name

        executor = ThreadPoolExecutor(max_workers=1,
                                      thread_name_prefix='executor')
        thread_name = c.thread(thread, executor).b_get()
        self.assertTrue(thread_name.startswith('executor'))


class TestMultAsyncio(unittest.TestCase):
    def test_tap(self):
        async def main():
            src, dest = chan(), chan()
            m = c.mult(src)
            m.tap(dest)
            await src.put('success')
            self.assertEqual(await dest.get(), 'success')
            src.close()

        asyncio.run(main())

    def test_untap(self):
        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            await src.put('item1')
            await dest1.get()
            await dest2.get()
            m.untap(dest2)
            await src.put('item2')
            await dest1.get()
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
            await src.put('item')
            await dest1.get()
            await dest2.get()
            m.untap_all()
            self.assertIs(await src.put('dropMe'), True)
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
            await src.put('item')
            await dest1.get()
            await asyncio.sleep(0.1)
            self.assertIs(src.offer('failure'), False)
            await dest2.get()
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
            self.assertIs(await close_dest.put('closed'), False)
            self.assertIs(await no_close_dest.put('not closed'), True)

        asyncio.run(main())

    def test_tap_closes_when_added_after_mult_closes(self):
        async def main():
            src_ch, tap_ch = chan(), chan()
            m = c.mult(src_ch)
            src_ch.close()
            await asyncio.sleep(0.1)
            m.tap(tap_ch)
            self.assertIsNone(await tap_ch.get())

        asyncio.run(main())


class TestMultThread(unittest.TestCase):
    def test_tap(self):
        def thread(src, dest):
            m = c.mult(src)
            m.tap(dest)
            src.b_put('success')

        async def main():
            src, dest = chan(), chan()
            c.thread(lambda: thread(src, dest))
            self.assertEqual(await dest.get(), 'success')
            src.close()

        asyncio.run(main())

    def test_untap(self):
        def thread(src, dest1, dest2):
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            src.b_put('item1')
            dest1.b_get()
            dest2.b_get()
            m.untap(dest2)
            src.b_put('item2')
            dest1.b_get()

        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            c.thread(lambda: thread(src, dest1, dest2))
            await asyncio.sleep(0.1)
            self.assertIsNone(dest2.poll())
            src.close()

        asyncio.run(main())

    def test_untap_all(self):
        def thread(src, dest1, dest2):
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            src.b_put('item')
            dest1.b_get()
            dest2.b_get()
            m.untap_all()

        async def main():
            src, dest1, dest2 = chan(), chan(), chan()
            c.thread(lambda: thread(src, dest1, dest2))
            await asyncio.sleep(0.1)
            self.assertIs(await src.put('dropMe'), True)
            await asyncio.sleep(0.1)
            self.assertIsNone(dest1.poll())
            self.assertIsNone(dest2.poll())

        asyncio.run(main())

    def test_untap_nonexistent_tap(self):
        def thread(src, complete):
            m = c.mult(src)
            m.untap(chan())
            src.close()
            complete.close()

        async def main():
            src, complete = chan(), chan()
            c.thread(lambda: thread(src, complete))
            self.assertIsNone(await complete.get())

        asyncio.run(main())

    def test_mult_blocks_until_all_taps_accept(self):
        def thread(src, dest1, dest2, complete):
            m = c.mult(src)
            m.tap(dest1)
            m.tap(dest2)
            src.b_put('item')
            dest1.b_get()
            time.sleep(0.1)
            self.assertIs(src.offer('failure'), False)
            dest2.b_get()
            src.close()
            complete.close()

        async def main():
            src, dest1, dest2, complete = chan(), chan(), chan(), chan()
            c.thread(lambda: thread(src, dest1, dest2, complete))
            self.assertIsNone(await complete.get())

        asyncio.run(main())

    def test_only_correct_taps_close(self):
        def thread(src, close_dest, open_dest):
            m = c.mult(src)
            m.tap(close_dest)
            m.tap(open_dest, close=False)
            src.close()

        async def main():
            src, close_dest, open_dest = chan(), chan(1), chan(1)
            c.thread(lambda: thread(src, close_dest, open_dest))
            await asyncio.sleep(0.1)
            self.assertIs(await close_dest.put('closed'), False)
            self.assertIs(await open_dest.put('not closed'), True)

        asyncio.run(main())

    def test_tap_closes_when_added_after_mult_closes(self):
        def thread(src_ch, tap_ch):
            m = c.mult(src_ch)
            src_ch.close()
            time.sleep(0.1)
            m.tap(tap_ch)

        async def main():
            src_ch, tap_ch = chan(), chan()
            c.thread(lambda: thread(src_ch, tap_ch))
            self.assertIsNone(await tap_ch.get())

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

            await from_ch.put('apple')
            self.assertEqual(await a1_ch.get(), 'apple')
            self.assertEqual(await a2_ch.get(), 'apple')
            await from_ch.put('bat')
            self.assertEqual(await b1_ch.get(), 'bat')
            self.assertEqual(await b2_ch.get(), 'bat')

            await from_ch.put('ant')
            self.assertEqual(await a1_ch.get(), 'ant')
            self.assertEqual(await a2_ch.get(), 'ant')
            await from_ch.put('bear')
            self.assertEqual(await b1_ch.get(), 'bear')
            self.assertEqual(await b2_ch.get(), 'bear')

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
            await from_ch.put('apple')
            self.assertEqual(await a1_ch.get(), 'apple')
            await from_ch.put('bat')
            self.assertEqual(await b_ch.get(), 'bat')
            await asyncio.sleep(0.1)
            self.assertIsNone(a2_ch.poll())

            p.sub('a', a2_ch)
            from_ch.put('air')
            self.assertEqual(await a2_ch.get(), 'air')

        asyncio.run(main())

    def test_unsub_nonexistent_topic(self):
        async def main():
            from_ch, to_ch = chan(1), chan()
            p = c.pub(from_ch, xf.identity)
            p.sub('a', to_ch)

            p.unsub('b', to_ch)
            await from_ch.put('a')
            self.assertEqual(await to_ch.get(), 'a')

        asyncio.run(main())

    def test_unsub_nonexistent_ch(self):
        async def main():
            from_ch, to_ch = chan(1), chan()
            p = c.pub(from_ch, xf.identity)
            p.sub('a', to_ch)

            p.unsub('b', chan())
            await from_ch.put('a')
            self.assertEqual(await to_ch.get(), 'a')

        asyncio.run(main())

    def test_unsub_all(self):
        async def main():
            from_ch, a_ch, b_ch = chan(2), chan(), chan()
            p = c.pub(from_ch, lambda x: x[0])
            p.sub('a', a_ch)
            p.sub('b', b_ch)

            p.unsub_all()
            await from_ch.put('apple')
            await from_ch.put('bat')
            await asyncio.sleep(0.1)
            self.assertIsNone(from_ch.poll())
            self.assertIsNone(a_ch.poll())
            self.assertIsNone(b_ch.poll())

            p.sub('a', a_ch)
            await from_ch.put('air')
            self.assertEqual(await a_ch.get(), 'air')

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
            await from_ch.put('apple')
            await from_ch.put('bat')
            await asyncio.sleep(0.1)
            self.assertIsNone(a1_ch.poll())
            self.assertIsNone(a2_ch.poll())
            self.assertEqual(b_ch.poll(), 'bat')
            self.assertIsNone(from_ch.poll())

            p.sub('a', a1_ch)
            await from_ch.put('air')
            self.assertEqual(await a1_ch.get(), 'air')

        asyncio.run(main())

    def test_only_correct_subs_get_closed(self):
        async def main():
            from_ch, close_ch, open_ch = chan(1), chan(1), chan(1)
            p = c.pub(from_ch, xf.identity)
            p.sub('close', close_ch)
            p.sub('open', open_ch, close=False)

            from_ch.close()
            await asyncio.sleep(0.1)
            self.assertIs(await close_ch.put('fail'), False)
            self.assertIs(await open_ch.put('success'), True)

        asyncio.run(main())

    def test_buf_fn(self):
        async def main():
            from_ch = chan()
            a_ch, b_ch = chan(), chan()
            p = c.pub(from_ch, lambda x: x[0],
                      lambda x: None if x == 'a' else 2)

            p.sub('a', a_ch)
            p.sub('b', b_ch)
            await from_ch.put('a1')
            await from_ch.put('a2')
            await asyncio.sleep(0.1)
            self.assertIs(from_ch.offer('a fail'), False)
            self.assertEqual(await a_ch.get(), 'a1')
            self.assertEqual(await a_ch.get(), 'a2')
            await asyncio.sleep(0.1)
            self.assertIsNone(a_ch.poll())

            await from_ch.put('b1')
            await from_ch.put('b2')
            await from_ch.put('b3')
            await from_ch.put('b4')
            await asyncio.sleep(0.1)
            self.assertIs(from_ch.offer('b fail'), False)
            self.assertEqual(await b_ch.get(), 'b1')
            self.assertEqual(await b_ch.get(), 'b2')
            self.assertEqual(await b_ch.get(), 'b3')
            self.assertEqual(await b_ch.get(), 'b4')
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
            await from_ch1.put('from_ch1')
            self.assertEqual(await to_ch.get(), 'from_ch1')
            m.admix(from_ch2)
            await from_ch1.put('from_ch1 again')
            self.assertEqual(await to_ch.get(), 'from_ch1 again')
            await from_ch2.put('from_ch2')
            self.assertEqual(await to_ch.get(), 'from_ch2')

        asyncio.run(main())

    def test_unmix(self):
        async def main():
            from_ch1, from_ch2, to_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch1)
            await from_ch1.put('from_ch1')
            self.assertEqual(await to_ch.get(), 'from_ch1')
            m.admix(from_ch2)
            m.unmix(from_ch1)
            await from_ch2.put('from_ch2')
            self.assertEqual(await to_ch.get(), 'from_ch2')
            await from_ch1.put('remain in from_ch1')
            await asyncio.sleep(0.1)
            self.assertIsNone(to_ch.poll())
            self.assertEqual(await from_ch1.get(), 'remain in from_ch1')

        asyncio.run(main())

    def test_unmix_all(self):
        async def main():
            from_ch1, from_ch2, to_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch1)
            m.admix(from_ch2)
            await from_ch1.put('from_ch1')
            self.assertEqual(await to_ch.get(), 'from_ch1')
            await from_ch2.put('from_ch2')
            self.assertEqual(await to_ch.get(), 'from_ch2')
            m.unmix_all()
            await asyncio.sleep(0.1)
            await from_ch1.put('ignore from_ch1 item')
            await from_ch2.put('ignore from_ch2 item')
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
            await unmuted_ch.put('not muted')
            self.assertEqual(await to_ch.get(), 'not muted')
            await muted_ch.put('mute me')
            self.assertIsNone(to_ch.poll())

            m.toggle({unmuted_ch: {'mute': True},
                      muted_ch: {'mute': False}})
            await muted_ch.put('the mute can now talk')
            self.assertEqual(await to_ch.get(), 'the mute can now talk')
            await unmuted_ch.put('i made a deal with Ursula')
            self.assertIsNone(to_ch.poll())

        asyncio.run(main())

    def test_pause(self):
        async def main():
            unpaused_ch, paused_ch, to_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.toggle({unpaused_ch: {'pause': False},
                      paused_ch: {'pause': True}})
            await unpaused_ch.put('not paused')
            self.assertEqual(await to_ch.get(), 'not paused')
            await paused_ch.put('remain in paused_ch')
            await asyncio.sleep(0.1)
            self.assertEqual(await paused_ch.get(), 'remain in paused_ch')

            m.toggle({unpaused_ch: {'pause': True},
                      paused_ch: {'pause': False}})
            await paused_ch.put('no longer paused')
            self.assertEqual(await to_ch.get(), 'no longer paused')
            await unpaused_ch.put('paused now')
            await asyncio.sleep(0.1)
            self.assertEqual(await unpaused_ch.get(), 'paused now')

        asyncio.run(main())

    def test_pause_dominates_mute(self):
        async def main():
            from_ch, to_ch = chan(1), chan(1)
            m = c.mix(to_ch)
            m.toggle({from_ch: {'pause': True, 'mute': True}})
            await from_ch.put('stay in from_ch')
            await asyncio.sleep(0.1)
            self.assertEqual(await from_ch.get(), 'stay in from_ch')

        asyncio.run(main())

    def test_solo_dominates_pause_and_mute(self):
        async def main():
            from_ch, to_ch = chan(), chan(1)
            m = c.mix(to_ch)
            m.toggle({from_ch: {'solo': True, 'pause': True, 'mute': True}})
            await from_ch.put('success')
            self.assertEqual(await to_ch.get(), 'success')

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
            await solo_ch1.put('solo_ch1 not muted')
            self.assertEqual(await to_ch.get(), 'solo_ch1 not muted')
            await solo_ch2.put('solo_ch2 not muted')
            self.assertEqual(await to_ch.get(), 'solo_ch2 not muted')
            await non_solo_ch.put('drop me')
            await asyncio.sleep(0.1)
            self.assertIsNone(to_ch.poll())

            m.toggle({solo_ch1: {'solo': False},
                      solo_ch2: {'solo': False}})
            await asyncio.sleep(0.1)
            await solo_ch1.put('solo_ch1 still not muted')
            self.assertEqual(await to_ch.get(), 'solo_ch1 still not muted')
            await solo_ch2.put('solo_ch2 still not muted')
            self.assertEqual(await to_ch.get(), 'solo_ch2 still not muted')
            await non_solo_ch.put('non_solo_ch not muted')
            self.assertEqual(await to_ch.get(), 'non_solo_ch not muted')

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
            await solo_ch1.put('solo_ch1 not paused')
            self.assertEqual(await to_ch.get(), 'solo_ch1 not paused')
            await solo_ch2.put('solo_ch2 not paused')
            self.assertEqual(await to_ch.get(), 'solo_ch2 not paused')
            await non_solo_ch.put('stay in non_solo_ch')
            await asyncio.sleep(0.1)
            self.assertEqual(await non_solo_ch.get(), 'stay in non_solo_ch')

            m.toggle({solo_ch1: {'solo': False},
                      solo_ch2: {'solo': False}})
            await asyncio.sleep(0.1)
            await solo_ch1.put('solo_ch1 still not paused')
            self.assertEqual(await to_ch.get(), 'solo_ch1 still not paused')
            await solo_ch2.put('solo_ch2 still not paused')
            self.assertEqual(await to_ch.get(), 'solo_ch2 still not paused')
            await non_solo_ch.put('non_solo_ch not paused')
            self.assertEqual(await to_ch.get(), 'non_solo_ch not paused')

        asyncio.run(main())

    def test_admix_unmix_toggle_do_not_interrupt_put(self):
        async def main():
            to_ch, from_ch = chan(), chan(1)
            admix_ch, unmix_ch, pause_ch = chan(1), chan(1), chan(1)
            m = c.mix(to_ch)
            m.toggle({from_ch: {}, unmix_ch: {}})

            # Start waiting put to to_ch
            await from_ch.put('successful transfer')
            await asyncio.sleep(0.1)

            # Apply operations while mix is waiting on to_ch
            m.admix(admix_ch)
            m.unmix(unmix_ch)
            m.toggle({pause_ch: {'pause': True}})

            # Confirm state is correct
            self.assertEqual(await to_ch.get(), 'successful transfer')

            await admix_ch.put('admix_ch added')
            self.assertEqual(await to_ch.get(), 'admix_ch added')

            await unmix_ch.put('unmix_ch removed')
            await asyncio.sleep(0.1)
            self.assertEqual(await unmix_ch.get(), 'unmix_ch removed')

            await pause_ch.put('pause_ch paused')
            await asyncio.sleep(0.1)
            self.assertEqual(await pause_ch.get(), 'pause_ch paused')

        asyncio.run(main())

    def test_to_ch_does_not_close_when_from_chs_do(self):
        async def main():
            from_ch, to_ch = chan(), chan(1)
            m = c.mix(to_ch)
            m.admix(from_ch)
            from_ch.close()
            await asyncio.sleep(0.1)
            self.assertIs(await to_ch.put('success'), True)

        asyncio.run(main())

    def test_mix_consumes_only_one_after_to_ch_closes(self):
        async def main():
            from_ch, to_ch = chan(1), chan()
            m = c.mix(to_ch)
            m.admix(from_ch)
            await asyncio.sleep(0.1)
            to_ch.close()
            await from_ch.put('mix consumes me')
            await from_ch.put('mix ignores me')
            await asyncio.sleep(0.1)
            self.assertEqual(await from_ch.get(), 'mix ignores me')

        asyncio.run(main())


class TestPipe(unittest.TestCase):
    def test_pipe_copy(self):
        async def main():
            src, dest = chan(), chan()
            c.pipe(src, dest)
            src.f_put(1)
            src.f_put(2)
            src.close()
            self.assertEqual(await a_list(dest), [1, 2])

        asyncio.run(main())

    def test_pipe_close_dest(self):
        async def main():
            src, dest = chan(), chan()
            c.pipe(src, dest)
            src.close()
            self.assertIsNone(await dest.get())

        asyncio.run(main())

    def test_return_value_is_dest(self):
        async def main():
            src, dest = chan(), chan()
            src.close()
            self.assertIs(c.pipe(src, dest), dest)

        asyncio.run(main())

    def test_pipe_no_close_dest(self):
        async def main():
            src, dest = chan(), chan(1)
            c.pipe(src, dest, close=False)
            src.close()
            await asyncio.sleep(0.1)
            dest.put('success')
            self.assertEqual(await dest.get(), 'success')

        asyncio.run(main())

    def test_stop_consuming_when_dest_closes(self):
        async def main():
            src, dest = chan(3), chan(1)
            c.onto_chan(src, ['intoDest1', 'intoDest2', 'dropMe'], close=False)
            c.pipe(src, dest)
            await asyncio.sleep(0.1)
            dest.close()
            self.assertEqual(await dest.get(), 'intoDest1')
            self.assertEqual(await dest.get(), 'intoDest2')
            self.assertIsNone(await dest.get())
            await asyncio.sleep(0.1)
            self.assertIsNone(src.poll())

        asyncio.run(main())


class TestPipeline(unittest.TestCase):
    def _test_output(self, mode):
        def f(x):
            time.sleep(0.2)
            return str(x)

        async def main():
            xform = xf.map(f)
            start_time = time.time()
            to_ch = chan(5)
            finished_ch = c.pipeline(5, to_ch, xform, c.to_chan(range(5)),
                                     mode=mode)
            self.assertIs(await finished_ch.get(), None)
            elapsed_time = time.time() - start_time
            self.assertTrue(0.1 < elapsed_time < 0.3)
            self.assertEqual(await a_list(to_ch), ['0', '1', '2', '3', '4'])

        asyncio.run(main())

    def _test_to_ch_not_closed(self, mode):
        async def main():
            to_ch = chan(5)
            c.pipeline(5, to_ch, xf.map(str), c.to_chan(range(5)),
                       close=False, mode=mode)
            for i in range(5):
                self.assertEqual(await to_ch.get(), str(i))
            self.assertIs(await to_ch.put('success'), True)
            to_ch.close()
            self.assertEqual(await to_ch.get(), 'success')
            self.assertIs(await to_ch.get(), None)

        asyncio.run(main())

    def _test_stop_consuming_from_ch(self, mode):
        async def main():
            to_ch = chan(5, xf.take(5))
            from_ch = c.to_chan(range(20))
            c.pipeline(5, to_ch, xf.identity, from_ch, mode=mode)
            await asyncio.sleep(0.1)
            self.assertEqual(await a_list(to_ch), [0, 1, 2, 3, 4])
            self.assertTrue(len(await a_list(from_ch)) > 5)

        asyncio.run(main())

    def _test_output_with_chunksize(self, mode):
        async def main():
            to_ch = chan(5)
            finished_ch = c.pipeline(5, to_ch, xf.map(str), c.to_chan(range(5)),
                                     mode=mode, chunksize=2)
            self.assertIs(await finished_ch.get(), None)
            self.assertEqual(await a_list(to_ch), ['0', '1', '2', '3', '4'])

        asyncio.run(main())

    def _test_ex_handler(self, mode):
        def f(x):
            if x == 1:
                raise ValueError
            return str(x)

        def ex_handler(e):
            if isinstance(e, ValueError):
                return 'ex_handler value'

        async def main():
            to_ch = chan(2)
            c.pipeline(1, to_ch, xf.map(f), c.to_chan([1, 2]),
                       ex_handler=ex_handler, mode=mode)
            self.assertEqual(await a_list(to_ch), ['ex_handler value', '2'])

        asyncio.run(main())

    def test_invalid_mode(self):
        with self.assertRaises(ValueError):
            c.pipeline(1, chan(), xf.identity, chan(), mode='invalid')

    def test_thread_output(self):
        self._test_output('thread')

    def test_thread_to_ch_not_closed(self):
        self._test_to_ch_not_closed('thread')

    def test_thread_stop_consuming_from_ch(self):
        self._test_stop_consuming_from_ch('thread')

    def test_thread_output_with_chunksize(self):
        self._test_output_with_chunksize('thread')

    def test_thread_ex_handler(self):
        self._test_ex_handler('thread')

    def test_process_output(self):
        self._test_output('process')

    def test_process_to_ch_not_closed(self):
        self._test_to_ch_not_closed('process')

    def test_process_stop_consuming_from_ch(self):
        self._test_stop_consuming_from_ch('process')

    def test_thread_output_with_chunksize(self):
        self._test_output_with_chunksize('process')

    def test_process_ex_handler(self):
        self._test_ex_handler('process')


class TestPipelineAsync(unittest.TestCase):
    def test_pipeline_async(self):
        def thread(val, result_ch):
            result_ch.b_put(val)
            time.sleep(0.2)
            result_ch.b_put(str(val))
            result_ch.close()

        def af(val, result_ch):
            threading.Thread(target=thread, args=[val, result_ch]).start()

        async def main():
            to_ch = chan(8)
            start_time = time.time()
            finished_ch = c.pipeline_async(2, to_ch, af, c.to_chan([1, 2, 3, 4]))
            self.assertIs(await finished_ch.get(), None)
            self.assertTrue(0.3 < time.time() - start_time < 0.5)
            self.assertEqual(await a_list(to_ch),
                             [1, '1', 2, '2', 3, '3', 4, '4'])

        asyncio.run(main())

    def test_pipeline_async_no_close(self):
        def af(_, result_ch):
            result_ch.close()

        async def main():
            to_ch = chan(1)
            finished_ch = c.pipeline_async(2, to_ch, af,
                                           c.to_chan([1, 2, 3, 4]),
                                           close=False)
            self.assertIs(await finished_ch.get(), None)
            await to_ch.put('success')
            to_ch.close()
            self.assertEqual(await to_ch.get(), 'success')
            self.assertIs(await to_ch.get(), None)

        asyncio.run(main())


class TestReduce(unittest.TestCase):
    def test_empty_ch(self):
        async def main():
            ch = chan()
            ch.close()
            result_ch = c.reduce(lambda: None, 'init', ch)
            self.assertEqual(await result_ch.get(), 'init')

        asyncio.run(main())

    def test_non_empty_ch(self):
        async def main():
            in_ch = c.to_chan(range(4))
            result_ch = c.reduce(lambda x, y: x + y, 100, in_ch)
            self.assertEqual(await result_ch.get(), 106)

        asyncio.run(main())

    def test_no_init_non_empty(self):
        async def main():
            in_ch = c.to_chan(range(4))
            result_ch = c.reduce(xf.multi_arity(lambda: 100,
                                                xf.identity,
                                                lambda x, y: x + y),
                                 in_ch)
            self.assertEqual(await result_ch.get(), 106)

        asyncio.run(main())

    def test_no_init_empty(self):
        async def main():
            in_ch = chan()
            in_ch.close()
            result_ch = c.reduce(xf.multi_arity(lambda: 100,
                                                xf.identity,
                                                lambda x, y: x + y),
                                 in_ch)
            self.assertEqual(await result_ch.get(), 100)

        asyncio.run(main())

    def test_no_init_no_zero_arity(self):
        async def main():
            in_ch = c.to_chan(range(4))
            with self.assertRaises(TypeError):
                c.reduce(xf.multi_arity(None,
                                        xf.identity,
                                        lambda x, y: x + y),
                         in_ch)

        asyncio.run(main())

    def test_reduced(self):
        async def main():
            in_ch = c.to_chan(range(4))

            def rf(result, val):
                if val == 2:
                    return xf.reduced(result + 2)
                return result + val

            result_ch = c.reduce(rf, 100, in_ch)
            self.assertEqual(await result_ch.get(), 103)

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

            result_ch = c.transduce(xf.partition_all(2), rf, [], ch)
            self.assertEqual(await result_ch.get(), [(1, 2), (3,)])

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
            self.assertEqual(await result_ch.get(), [1, 2])

        asyncio.run(main())

    def test_no_init_non_empty(self):
        async def main():
            in_ch = c.to_chan(range(4))
            result_ch = c.transduce(xf.filter(lambda x: x % 2 == 0),
                                    xf.multi_arity(lambda: 100,
                                                   xf.identity,
                                                   lambda x, y: x + y),
                                    in_ch)
            self.assertEqual(await result_ch.get(), 102)

        asyncio.run(main())

    def test_no_init_empty(self):
        async def main():
            in_ch = chan()
            in_ch.close()
            result_ch = c.transduce(xf.filter(lambda x: x % 2 == 0),
                                    xf.multi_arity(lambda: 100,
                                                   xf.identity,
                                                   lambda x, y: x + y),
                                    in_ch)
            self.assertEqual(await result_ch.get(), 100)

        asyncio.run(main())

    def test_no_init_no_zero_arity(self):
        async def main():
            in_ch = c.to_chan(range(4))
            with self.assertRaises(TypeError):
                c.transduce(xf.filter(lambda x: x % 2 == 0),
                            xf.multi_arity(None,
                                           xf.identity,
                                           lambda x, y: x + y),
                            in_ch)

        asyncio.run(main())


class TestMerge(unittest.TestCase):
    def test_merge_unbuffered(self):
        async def main():
            src1, src2 = chan(), chan()
            m = c.merge([src1, src2], 2)
            await src1.put('src1')
            await src2.put('src2')
            src1.close()
            src2.close()
            self.assertEqual([x async for x in m], ['src1', 'src2'])

        asyncio.run(main())


class TestMap(unittest.TestCase):
    def test_map_unbuffered(self):
        async def main():
            letter_ch = c.to_chan(['a', 'b', 'c', 'd', 'e'])
            number_ch = c.to_chan(['1', '2', '3'])
            result_ch = c.map(lambda x, y: x + y, [letter_ch, number_ch])
            self.assertEqual(await a_list(result_ch), ['a1', 'b2', 'c3'])
            self.assertEqual(await a_list(letter_ch), ['e'])

        asyncio.run(main())

    def test_map_buffered(self):
        async def main():
            letter_ch = c.to_chan(['a', 'b', 'c', 'd', 'e'])
            number_ch = c.to_chan(['1', '2', '3'])
            result_ch = c.map(lambda x, y: x + y,
                              [letter_ch, number_ch],
                              c.sliding_buffer(2))
            await asyncio.sleep(0.1)
            self.assertEqual(await a_list(result_ch), ['b2', 'c3'])
            self.assertEqual(await a_list(letter_ch), ['e'])

        asyncio.run(main())


class TestSplit(unittest.TestCase):
    def test_chans_close_with_closed_source(self):
        async def main():
            src_ch = chan()
            src_ch.close()
            t_ch, f_ch = c.split(lambda _: True, src_ch)
            self.assertIsNone(await t_ch.get())
            self.assertIsNone(await f_ch.get())

        asyncio.run(main())

    def test_true_false_chans(self):
        async def main():
            t_ch, f_ch = c.split(lambda x: x % 2 == 0,
                                 c.to_chan([1, 2, 3, 4]))
            self.assertEqual(await f_ch.get(), 1)
            self.assertEqual(await t_ch.get(), 2)
            self.assertEqual(await f_ch.get(), 3)
            self.assertEqual(await t_ch.get(), 4)
            self.assertIsNone(await f_ch.get())
            self.assertIsNone(await t_ch.get())

        asyncio.run(main())

    def test_bufs(self):
        async def main():
            t_ch, f_ch = c.split(lambda x: x % 2 == 0,
                                 c.to_chan([1, 2, 3, 4, 5]),
                                 2, 3)
            self.assertEqual(await a_list(t_ch), [2, 4])
            self.assertEqual(await a_list(f_ch), [1, 3, 5])

        asyncio.run(main())


if __name__ == '__main__':
    unittest.main()
