import asyncio
import ast
import gc
from pathlib import Path
import time
from collections import defaultdict
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker, _OrderBookMessageEnvelope
from hummingbot.core.data_type.order_book_tracker_data_source import (
    _UNBOUND_RAW_QUEUE_GENERATION,
    OrderBookTrackerDataSource,
)


class _LifecycleProbeSource(OrderBookTrackerDataSource):
    def __init__(self):
        super().__init__(trading_pairs=[])
        self.remote_pairs = set()
        self.listener_generations = []
        self.unsubscribe_started = asyncio.Event()
        self.unsubscribe_release = asyncio.Event()
        self.block_unsubscribe = False
        self.unsubscribe_result = True
        self.unsubscribe_calls = 0

    async def get_last_traded_prices(self, trading_pairs, domain=None):
        await asyncio.Future()

    async def get_new_order_book(self, trading_pair):
        return OrderBook()

    async def listen_for_subscriptions(self):
        self.listener_generations.append(self._raw_message_queue_context.get())
        for trading_pair in self._trading_pairs:
            self.remote_pairs.add(trading_pair)
        await asyncio.Future()

    async def subscribe_to_trading_pair(self, trading_pair):
        self.add_trading_pair(trading_pair)
        self.remote_pairs.add(trading_pair)
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair):
        self.unsubscribe_calls += 1
        if self.block_unsubscribe:
            self.unsubscribe_started.set()
            await self.unsubscribe_release.wait()
        if self.unsubscribe_result:
            self.remote_pairs.discard(trading_pair)
            self.remove_trading_pair(trading_pair)
        return self.unsubscribe_result

    async def _parse_trade_message(self, raw_message, message_queue):
        return None

    async def _parse_order_book_diff_message(self, raw_message, message_queue):
        return None

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue):
        return None

    async def _order_book_snapshot(self, trading_pair):
        raise NotImplementedError

    async def _connected_websocket_assistant(self):
        raise NotImplementedError

    async def _subscribe_channels(self, ws):
        raise NotImplementedError

    def _channel_originating_message(self, event_message):
        raise NotImplementedError


class OrderBookTrackerGenerationTests(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def _message(message_type: OrderBookMessageType) -> OrderBookMessage:
        content = {"trading_pair": "STALE-USDT"}
        if message_type is OrderBookMessageType.TRADE:
            content.update({"trade_id": 1, "price": "1", "amount": "1", "trade_type": 1.0})
        else:
            content.update({"update_id": 1, "bids": [], "asks": []})
        return OrderBookMessage(message_type=message_type, content=content, timestamp=time.time())

    async def test_stopped_generation_cannot_publish_initial_book_or_readiness(self):
        initialization_started = asyncio.Event()
        old_book = OrderBook()

        async def cancellation_resistant_snapshot(_trading_pair: str):
            initialization_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                return old_book

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.get_new_order_book = AsyncMock(side_effect=cancellation_resistant_snapshot)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=["OLD-USDT"])
        tracker._sleep = AsyncMock()

        tracker.start()
        started_generation = tracker._active_generation
        await initialization_started.wait()
        tracker.stop()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        self.assertFalse(tracker.ready)
        self.assertIsNone(tracker._active_generation)
        self.assertGreater(tracker._generation, started_generation)
        self.assertNotIn("OLD-USDT", tracker.order_books)

    async def test_initial_books_are_published_as_one_generation_commit(self):
        second_snapshot_started = asyncio.Event()
        second_snapshot_release = asyncio.Event()

        async def snapshots(trading_pair: str):
            if trading_pair == "SECOND-USDT":
                second_snapshot_started.set()
                await second_snapshot_release.wait()
            return OrderBook()

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.get_new_order_book = AsyncMock(side_effect=snapshots)
        tracker = OrderBookTracker(
            data_source=data_source, trading_pairs=["FIRST-USDT", "SECOND-USDT"]
        )
        tracker._sleep = AsyncMock()
        tracker._update_last_trade_prices_loop = AsyncMock(side_effect=lambda: asyncio.Future())

        tracker.start()
        await second_snapshot_started.wait()

        self.assertFalse(tracker.ready)
        self.assertEqual({}, tracker.order_books)
        self.assertEqual({}, tracker._tracking_tasks)

        second_snapshot_release.set()
        await tracker.wait_ready()
        self.assertEqual({"FIRST-USDT", "SECOND-USDT"}, set(tracker.order_books))
        tracker.stop()

    async def test_delayed_old_listener_frames_are_rejected_before_routing_state_lookup(self):
        listener_started = [asyncio.Event() for _ in range(3)]
        listener_calls = [0, 0, 0]

        def listener(index: int, message_type: OrderBookMessageType):
            async def emit_after_cancellation(_loop, output):
                call = listener_calls[index]
                listener_calls[index] += 1
                if call == 0:
                    listener_started[index].set()
                    try:
                        await asyncio.Future()
                    except asyncio.CancelledError:
                        output.put_nowait(self._message(message_type))
                        return
                await asyncio.Future()

            return emit_after_cancellation

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.listen_for_order_book_diffs = AsyncMock(
            side_effect=listener(0, OrderBookMessageType.DIFF)
        )
        data_source.listen_for_order_book_snapshots = AsyncMock(
            side_effect=listener(1, OrderBookMessageType.SNAPSHOT)
        )
        data_source.listen_for_trades = AsyncMock(
            side_effect=listener(2, OrderBookMessageType.TRADE)
        )
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])

        tracker.start()
        old_generation = tracker._active_generation
        await asyncio.gather(*(started.wait() for started in listener_started))
        tracker.start()
        await tracker.wait_ready()
        tracker._order_book_diff_stream.put_nowait(
            _OrderBookMessageEnvelope(old_generation, self._message(OrderBookMessageType.DIFF))
        )
        tracker._order_book_snapshot_stream.put_nowait(
            _OrderBookMessageEnvelope(old_generation, self._message(OrderBookMessageType.SNAPSHOT))
        )
        tracker._order_book_trade_stream.put_nowait(
            _OrderBookMessageEnvelope(old_generation, self._message(OrderBookMessageType.TRADE))
        )
        for _ in range(5):
            await asyncio.sleep(0)

        self.assertNotIn("STALE-USDT", tracker._saved_message_queues)
        self.assertEqual(0, tracker.metrics.total_snapshots_rejected)
        self.assertEqual(0, tracker.metrics.total_trades_rejected)
        tracker.stop()

    async def test_stop_immediately_invalidates_and_clears_all_generation_state(self):
        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])
        tracker.start()
        await tracker.wait_ready()

        tracker._order_books["OLD-USDT"] = OrderBook()
        tracker._tracking_message_queues["OLD-USDT"] = asyncio.Queue()
        tracker._saved_message_queues["OLD-USDT"].append(self._message(OrderBookMessageType.DIFF))
        tracker._past_diffs_windows["OLD-USDT"].append(self._message(OrderBookMessageType.DIFF))
        tracker._order_book_diff_stream.put_nowait(self._message(OrderBookMessageType.DIFF))
        tracker._order_book_snapshot_stream.put_nowait(self._message(OrderBookMessageType.SNAPSHOT))
        tracker._order_book_trade_stream.put_nowait(self._message(OrderBookMessageType.TRADE))

        tracker.stop()

        self.assertFalse(tracker.ready)
        self.assertEqual({}, tracker.order_books)
        self.assertEqual({}, tracker._tracking_message_queues)
        self.assertEqual({}, tracker._saved_message_queues)
        self.assertEqual({}, tracker._past_diffs_windows)
        self.assertTrue(tracker._order_book_diff_stream.empty())
        self.assertTrue(tracker._order_book_snapshot_stream.empty())
        self.assertTrue(tracker._order_book_trade_stream.empty())

    async def test_stop_and_wait_drains_a_cancellation_resistant_generation_task(self):
        cleanup_started = asyncio.Event()
        cleanup_release = asyncio.Event()

        async def cancellation_resistant_task():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cleanup_started.set()
                await cleanup_release.wait()

        tracker = OrderBookTracker(
            data_source=MagicMock(spec=OrderBookTrackerDataSource), trading_pairs=[]
        )
        generation_task = asyncio.create_task(cancellation_resistant_task())
        await asyncio.sleep(0)
        tracker._tracking_tasks["OLD-USDT"] = generation_task

        drain_task = None
        try:
            drain_task = asyncio.create_task(tracker.stop_and_wait(timeout=1.0))
            await cleanup_started.wait()
            self.assertFalse(drain_task.done())
            cleanup_release.set()
            self.assertTrue(await drain_task)
            self.assertTrue(generation_task.done())
        finally:
            cleanup_release.set()
            generation_task.cancel()
            await asyncio.gather(generation_task, return_exceptions=True)
            if drain_task is not None:
                drain_task.cancel()
                await asyncio.gather(drain_task, return_exceptions=True)

    async def test_add_trading_pair_cannot_commit_after_generation_stop(self):
        snapshot_started = asyncio.Event()
        snapshot_release = asyncio.Event()

        async def delayed_snapshot(_trading_pair: str):
            snapshot_started.set()
            await snapshot_release.wait()
            return OrderBook()

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.subscribe_to_trading_pair = AsyncMock(return_value=True)
        data_source.get_new_order_book = AsyncMock(side_effect=delayed_snapshot)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])
        tracker.start()
        await tracker.wait_ready()

        add_task = asyncio.create_task(tracker.add_trading_pair("LATE-USDT"))
        await snapshot_started.wait()
        tracker.stop()
        snapshot_release.set()

        self.assertFalse(await add_task)
        self.assertNotIn("LATE-USDT", tracker.order_books)
        self.assertNotIn("LATE-USDT", tracker._tracking_tasks)
        self.assertNotIn("LATE-USDT", tracker._trading_pairs)

    async def test_remove_trading_pair_cannot_delete_replacement_generation_state(self):
        unsubscribe_started = asyncio.Event()
        unsubscribe_release = asyncio.Event()
        replacement_book = OrderBook()

        async def delayed_unsubscribe(_trading_pair: str):
            unsubscribe_started.set()
            await unsubscribe_release.wait()
            return True

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.unsubscribe_from_trading_pair = AsyncMock(side_effect=delayed_unsubscribe)
        data_source.get_new_order_book = AsyncMock(return_value=replacement_book)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])
        tracker._update_last_trade_prices_loop = AsyncMock(side_effect=lambda: asyncio.Future())
        tracker.start()
        await tracker.wait_ready()

        old_task = asyncio.create_task(asyncio.sleep(3600))
        tracker._trading_pairs.append("KEEP-USDT")
        tracker._order_books["KEEP-USDT"] = OrderBook()
        tracker._tracking_message_queues["KEEP-USDT"] = asyncio.Queue()
        tracker._tracking_tasks["KEEP-USDT"] = old_task

        remove_task = asyncio.create_task(tracker.remove_trading_pair("KEEP-USDT"))
        await unsubscribe_started.wait()
        tracker.start()
        await tracker.wait_ready()
        replacement_task = tracker._tracking_tasks["KEEP-USDT"]
        unsubscribe_release.set()

        self.assertFalse(await remove_task)
        self.assertIs(replacement_book, tracker.order_books["KEEP-USDT"])
        self.assertIs(replacement_task, tracker._tracking_tasks["KEEP-USDT"])
        self.assertIn("KEEP-USDT", tracker._trading_pairs)
        tracker.stop()

    async def test_add_cancellation_survives_subscription_cleanup_failure(self):
        snapshot_started = asyncio.Event()

        async def blocked_snapshot(_trading_pair: str):
            snapshot_started.set()
            await asyncio.Future()

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.subscribe_to_trading_pair = AsyncMock(return_value=True)
        data_source.unsubscribe_from_trading_pair = AsyncMock(side_effect=RuntimeError("cleanup failed"))
        data_source.get_new_order_book = AsyncMock(side_effect=blocked_snapshot)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])
        tracker.start()
        await tracker.wait_ready()

        add_task = asyncio.create_task(tracker.add_trading_pair("CANCEL-USDT"))
        await snapshot_started.wait()
        add_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await add_task
        data_source.unsubscribe_from_trading_pair.assert_awaited_once_with("CANCEL-USDT")
        tracker.stop()

    async def test_old_last_price_fetch_cannot_update_replacement_book(self):
        fetch_started = asyncio.Event()
        stale_fetch_release = asyncio.Event()
        fetch_calls = 0
        old_book = OrderBook()
        replacement_book = OrderBook()

        async def cancellation_resistant_last_price_fetch(**_kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                fetch_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    await stale_fetch_release.wait()
                    return {"PRICE-USDT": 999.0}
            await asyncio.Future()

        data_source = MagicMock(spec=OrderBookTrackerDataSource)
        data_source.get_new_order_book = AsyncMock(side_effect=[old_book, replacement_book])
        data_source.get_last_traded_prices = AsyncMock(side_effect=cancellation_resistant_last_price_fetch)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=["PRICE-USDT"])
        tracker._sleep = AsyncMock()

        tracker.start()
        await tracker.wait_ready()
        await fetch_started.wait()
        tracker.start()
        await tracker.wait_ready()
        stale_fetch_release.set()
        for _ in range(3):
            await asyncio.sleep(0)

        self.assertNotEqual(999.0, replacement_book.last_trade_price)
        tracker.stop()

    async def test_start_caller_stays_unbound_and_resolves_replacement_generation(self):
        source = _LifecycleProbeSource()
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        first_started = asyncio.Event()
        replacement_started = asyncio.Event()
        observed = {}

        async def task_a_start_and_access():
            tracker.start()
            await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
            observed["first_generation"] = tracker._active_generation
            observed["caller_context"] = source._raw_message_queue_context.get()
            first_started.set()
            await asyncio.wait_for(replacement_started.wait(), timeout=0.2)
            replacement = defaultdict(asyncio.Queue)
            source._message_queue = replacement
            source._message_queue["task-a"].put_nowait("current")
            observed["replacement"] = replacement
            observed["post_restart_context"] = source._raw_message_queue_context.get()

        caller_task = asyncio.create_task(task_a_start_and_access())
        await asyncio.wait_for(first_started.wait(), timeout=0.2)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        active_generation = tracker._active_generation
        replacement_started.set()
        await asyncio.wait_for(caller_task, timeout=0.2)
        source._message_queue["external"].put_nowait("current")

        self.assertIs(_UNBOUND_RAW_QUEUE_GENERATION, observed["caller_context"])
        self.assertIs(_UNBOUND_RAW_QUEUE_GENERATION, observed["post_restart_context"])
        self.assertNotIn(observed["first_generation"], source._raw_message_queues)
        self.assertIs(observed["replacement"], source._raw_message_queues[active_generation])
        self.assertEqual(1, source._raw_message_queues[active_generation]["task-a"].qsize())
        self.assertEqual(1, source._raw_message_queues[active_generation]["external"].qsize())
        self.assertIn(active_generation, source.listener_generations)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_retired_bound_context_cannot_recreate_mapping_by_assignment_or_access(self):
        source = _LifecycleProbeSource()
        source.activate_raw_message_generation(1)
        token = source.bind_raw_message_generation(1)
        try:
            source.deactivate_raw_message_generation(1)
            source.activate_raw_message_generation(2)
            source._message_queue = defaultdict(asyncio.Queue)
            source._message_queue["stale"].put_nowait("discard")

            self.assertNotIn(1, source._raw_message_queues)
            self.assertEqual(0, source._message_queue["stale"].qsize())
        finally:
            source.reset_raw_message_generation(token)

        source._message_queue["current"].put_nowait("deliver")
        self.assertEqual(1, source._raw_message_queues[2]["current"].qsize())

    async def test_retired_indexed_assignment_uses_non_growing_singleton_sink(self):
        source = _LifecycleProbeSource()
        source.activate_raw_message_generation(1)
        token = source.bind_raw_message_generation(1)
        try:
            source.deactivate_raw_message_generation(1)
            source.activate_raw_message_generation(2)
            injected = asyncio.Queue()
            source._message_queue["retired-key"] = injected
            source._message_queue["retired-key"].put_nowait("stale")
            for index in range(256):
                source._message_queue[f"retired-{index}"] = asyncio.Queue()

            self.assertEqual(0, injected.qsize())
            self.assertEqual(0, len(source._discarded_raw_message_queues))
            self.assertEqual(0, len(source._raw_message_queues[2]))
            self.assertIs(
                source._message_queue["first-arbitrary-key"],
                source._message_queue["second-arbitrary-key"],
            )
        finally:
            source.reset_raw_message_generation(token)

    async def test_add_preserves_exact_cancellation_message_when_child_swallows_cancel(self):
        source = _LifecycleProbeSource()
        child_cancellation_args = None

        async def cancellation_resistant_subscribe(trading_pair):
            nonlocal child_cancellation_args
            try:
                await asyncio.Future()
            except asyncio.CancelledError as cancellation:
                child_cancellation_args = cancellation.args
                source.add_trading_pair(trading_pair)
                source.remote_pairs.add(trading_pair)
                return True

        source.subscribe_to_trading_pair = cancellation_resistant_subscribe
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        add_task = asyncio.create_task(tracker.add_trading_pair("MESSAGE-USDT"))
        await asyncio.sleep(0)
        add_task.cancel("add-message")

        with self.assertRaises(asyncio.CancelledError) as raised:
            await asyncio.wait_for(add_task, timeout=0.2)

        self.assertEqual(("add-message",), child_cancellation_args)
        self.assertEqual(("add-message",), raised.exception.args)
        self.assertEqual(1, source.unsubscribe_calls)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_remove_preserves_exact_cancellation_message_when_child_swallows_cancel(self):
        source = _LifecycleProbeSource()
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertTrue(await tracker.add_trading_pair("MESSAGE-USDT"))
        child_cancellation_args = None

        async def cancellation_resistant_unsubscribe(trading_pair):
            nonlocal child_cancellation_args
            try:
                await asyncio.Future()
            except asyncio.CancelledError as cancellation:
                child_cancellation_args = cancellation.args
                source.remote_pairs.discard(trading_pair)
                source.remove_trading_pair(trading_pair)
                return True

        source.unsubscribe_from_trading_pair = cancellation_resistant_unsubscribe
        remove_task = asyncio.create_task(tracker.remove_trading_pair("MESSAGE-USDT"))
        await asyncio.sleep(0)
        remove_task.cancel("remove-message")

        with self.assertRaises(asyncio.CancelledError) as raised:
            await asyncio.wait_for(remove_task, timeout=0.2)

        self.assertEqual(("remove-message",), child_cancellation_args)
        self.assertEqual(("remove-message",), raised.exception.args)
        self.assertNotIn("MESSAGE-USDT", tracker.order_books)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_tracker_then_caller_cancellation_is_never_converted_to_false(self):
        source = _LifecycleProbeSource()
        subscribe_started = asyncio.Event()
        tracker_cancel_consumed = asyncio.Event()

        async def cancellation_resistant_subscribe(trading_pair):
            subscribe_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                asyncio.current_task().uncancel()
                tracker_cancel_consumed.set()
                await asyncio.Future()
            return True

        source.subscribe_to_trading_pair = cancellation_resistant_subscribe
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        add_task = asyncio.create_task(tracker.add_trading_pair("CANCEL-USDT"))
        await asyncio.wait_for(subscribe_started.wait(), timeout=0.2)
        tracker.stop()
        await asyncio.wait_for(tracker_cancel_consumed.wait(), timeout=0.2)
        add_task.cancel("caller-later")

        with self.assertRaises(asyncio.CancelledError) as raised:
            await asyncio.wait_for(add_task, timeout=0.2)
        self.assertEqual(("caller-later",), raised.exception.args)
        self.assertEqual(1, source.unsubscribe_calls)
        self.assertTrue(await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4))
        self.assertEqual(set(), tracker._tasks_to_drain)

    async def test_direct_restart_fails_closed_while_dynamic_cleanup_is_pending(self):
        for replacement_pair in ("DYNAMIC-USDT", "OTHER-USDT"):
            source = _LifecycleProbeSource()
            tracker = OrderBookTracker(data_source=source, trading_pairs=[])
            tracker._sleep = AsyncMock()
            tracker.start()
            await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
            self.assertTrue(await tracker.add_trading_pair("DYNAMIC-USDT"))
            source.block_unsubscribe = True
            if replacement_pair != "DYNAMIC-USDT":
                tracker._trading_pairs[:] = [replacement_pair]

            try:
                with self.assertRaisesRegex(RuntimeError, "stop_and_wait"):
                    tracker.start()
                await asyncio.wait_for(source.unsubscribe_started.wait(), timeout=0.2)
                self.assertIsNone(tracker._active_generation)
            finally:
                source.unsubscribe_release.set()
                source.block_unsubscribe = False
                await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_clean_drain_allows_restart_but_residual_cleanup_does_not(self):
        source = _LifecycleProbeSource()
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertTrue(await tracker.add_trading_pair("DYNAMIC-USDT"))
        self.assertTrue(await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4))

        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertIsNotNone(tracker._active_generation)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

        source.unsubscribe_result = False
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertTrue(await tracker.add_trading_pair("RESIDUAL-USDT"))
        self.assertFalse(await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4))
        with self.assertRaisesRegex(RuntimeError, "stop_and_wait"):
            tracker.start()
        self.assertIsNone(tracker._active_generation)

        source.unsubscribe_result = True
        self.assertTrue(await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4))

    async def test_generation_scoped_cleanup_cannot_clear_newer_markers(self):
        source = _LifecycleProbeSource()
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._dynamic_subscriptions.add("PAIR-USDT")
        tracker._dynamic_subscription_generations["PAIR-USDT"] = 2

        self.assertTrue(await tracker._unsubscribe_for_cleanup("PAIR-USDT", 1))

        self.assertIn("PAIR-USDT", tracker._dynamic_subscriptions)
        self.assertEqual(2, tracker._dynamic_subscription_generations["PAIR-USDT"])

    async def test_pending_cleanup_blocks_same_pair_replacement_until_new_incarnation(self):
        source = _LifecycleProbeSource()
        first_snapshot_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        cleanup_release = asyncio.Event()
        snapshot_calls = 0

        async def snapshot(_trading_pair):
            nonlocal snapshot_calls
            snapshot_calls += 1
            if snapshot_calls == 1:
                first_snapshot_started.set()
                await asyncio.Future()
            return OrderBook()

        async def unsubscribe(trading_pair):
            source.unsubscribe_calls += 1
            cleanup_started.set()
            await cleanup_release.wait()
            source.remote_pairs.discard(trading_pair)
            source.remove_trading_pair(trading_pair)
            return True

        source.get_new_order_book = snapshot
        source.unsubscribe_from_trading_pair = unsubscribe
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        first = asyncio.create_task(tracker.add_trading_pair("INCARNATION-USDT"))
        await asyncio.wait_for(first_snapshot_started.wait(), timeout=0.2)
        first.cancel("first-cancel")
        await asyncio.wait_for(cleanup_started.wait(), timeout=0.2)

        try:
            self.assertFalse(await tracker.add_trading_pair("INCARNATION-USDT"))
            self.assertEqual(1, source.unsubscribe_calls)
            cleanup_release.set()
            with self.assertRaises(asyncio.CancelledError) as raised:
                await first
            self.assertEqual(("first-cancel",), raised.exception.args)
            self.assertTrue(await tracker.add_trading_pair("INCARNATION-USDT"))
            owner = tracker._dynamic_subscription_owners["INCARNATION-USDT"]
            self.assertIn("INCARNATION-USDT", source.remote_pairs)
            self.assertIn("INCARNATION-USDT", tracker.order_books)
            self.assertIs(owner, tracker._dynamic_subscription_owners["INCARNATION-USDT"])
        finally:
            cleanup_release.set()
            await asyncio.gather(first, return_exceptions=True)
            await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_clean_restart_drops_dynamic_pairs_but_restarts_initial_pairs(self):
        source = _LifecycleProbeSource()
        source._trading_pairs.append("STATIC-USDT")
        tracker = OrderBookTracker(data_source=source, trading_pairs=["STATIC-USDT"])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertTrue(await tracker.add_trading_pair("DYNAMIC-USDT"))

        self.assertTrue(await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4))
        self.assertNotIn("DYNAMIC-USDT", tracker._trading_pairs)
        self.assertNotIn("DYNAMIC-USDT", source._trading_pairs)
        self.assertNotIn("DYNAMIC-USDT", source.remote_pairs)

        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertIn("STATIC-USDT", tracker.order_books)
        self.assertNotIn("DYNAMIC-USDT", tracker.order_books)
        self.assertNotIn("DYNAMIC-USDT", source.remote_pairs)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_false_unsubscribe_retry_keeps_local_remote_source_and_owner_coherent(self):
        source = _LifecycleProbeSource()
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        tracker._sleep = AsyncMock()
        tracker.start()
        await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
        self.assertTrue(await tracker.add_trading_pair("RETRY-USDT"))
        owner = tracker._dynamic_subscription_owners["RETRY-USDT"]
        source.unsubscribe_result = False

        self.assertFalse(await tracker.remove_trading_pair("RETRY-USDT"))
        self.assertIn("RETRY-USDT", tracker.order_books)
        self.assertIn("RETRY-USDT", tracker._trading_pairs)
        self.assertIn("RETRY-USDT", source._trading_pairs)
        self.assertIn("RETRY-USDT", source.remote_pairs)
        self.assertIs(owner, tracker._dynamic_subscription_owners["RETRY-USDT"])

        source.unsubscribe_result = True
        self.assertTrue(await tracker.remove_trading_pair("RETRY-USDT"))
        self.assertNotIn("RETRY-USDT", tracker.order_books)
        self.assertNotIn("RETRY-USDT", tracker._trading_pairs)
        self.assertNotIn("RETRY-USDT", source._trading_pairs)
        self.assertNotIn("RETRY-USDT", source.remote_pairs)
        self.assertNotIn("RETRY-USDT", tracker._dynamic_subscription_owners)
        await asyncio.wait_for(tracker.stop_and_wait(timeout=0.2), timeout=0.4)

    async def test_fast_data_source_failure_is_logged_and_retrieved_immediately(self):
        source = MagicMock(spec=OrderBookTrackerDataSource)
        source.bind_raw_message_generation.return_value = object()

        async def fail():
            raise RuntimeError("listener-failed")

        source.listen_for_subscriptions = AsyncMock(side_effect=fail)
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        observed = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        previous_logger = tracker.__class__._obt_logger
        logger = MagicMock()
        tracker.__class__._obt_logger = logger
        loop.set_exception_handler(lambda _loop, context: observed.append(context))
        try:
            task = tracker._schedule_data_source_task(1, "listen_for_subscriptions")
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            del task
            gc.collect()
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)
            tracker.__class__._obt_logger = previous_logger

        self.assertEqual([], observed)
        logger.error.assert_called_once()

    def test_changed_core_uses_only_python_310_asyncio_task_apis(self):
        forbidden = []
        for relative_path in (
            "hummingbot/connector/exchange_py_base.py",
            "hummingbot/core/data_type/order_book_tracker.py",
            "hummingbot/core/data_type/order_book_tracker_data_source.py",
        ):
            tree = ast.parse(Path(relative_path).read_text(encoding="utf-8"), filename=relative_path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr in {"cancelling", "uncancel", "timeout"}:
                    forbidden.append((relative_path, node.lineno, node.attr))

        self.assertEqual([], forbidden)

    async def test_retired_generation_and_pair_lock_metadata_are_pruned(self):
        source = _LifecycleProbeSource()
        for generation in range(1, 101):
            source.activate_raw_message_generation(generation)
            source.deactivate_raw_message_generation(generation)
        tracker = OrderBookTracker(data_source=source, trading_pairs=[])
        for index in range(100):
            tracker._pair_transaction_lock(f"PAIR-{index}")

        self.assertLessEqual(len(source._retired_raw_message_generations), 1)
        self.assertEqual(0, len(tracker._pair_transaction_locks))
