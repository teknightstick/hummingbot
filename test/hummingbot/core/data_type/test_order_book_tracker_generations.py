import asyncio
import time
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker, _OrderBookMessageEnvelope
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


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
