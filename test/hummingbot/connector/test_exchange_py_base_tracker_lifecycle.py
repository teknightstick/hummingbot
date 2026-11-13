import asyncio
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange_py_base import ExchangePyBase


class ExchangePyBaseTrackerLifecycleTests(IsolatedAsyncioWrapperTestCase):

    @staticmethod
    def _connector(order_book_tracker, **overrides):
        values = dict(
            _last_poll_timestamp=1,
            _last_timestamp=1,
            _poll_notifier=None,
            order_book_tracker=order_book_tracker,
            _status_polling_task=None,
            _trading_rules_polling_task=None,
            _trading_fees_polling_task=None,
            _user_stream_tracker_task=None,
            _user_stream_tracker=None,
            _user_stream_event_listener_task=None,
            _lost_orders_update_task=None,
            is_trading_required=False,
        )
        values.update(overrides)
        connector = SimpleNamespace(**values)
        connector.start_network = MethodType(ExchangePyBase.start_network, connector)
        connector.stop_network = MethodType(ExchangePyBase.stop_network, connector)
        return connector

    async def test_stop_network_awaits_order_book_tracker_drain(self):
        order_book_tracker = MagicMock()
        order_book_tracker.stop_and_wait = AsyncMock(return_value=True)
        connector = self._connector(order_book_tracker)

        await ExchangePyBase.stop_network(connector)

        order_book_tracker.stop_and_wait.assert_awaited_once_with()
        self.assertEqual(set(), connector._network_tasks_to_drain)

    async def test_newer_stop_prevents_older_start_from_activating_late(self):
        first_stop_started = asyncio.Event()
        release_first_stop = asyncio.Event()
        stop_calls = 0

        async def stop_and_wait():
            nonlocal stop_calls
            stop_calls += 1
            if stop_calls == 1:
                first_stop_started.set()
                await release_first_stop.wait()
            return True

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=stop_and_wait)
        connector = self._connector(tracker)
        older_start = asyncio.create_task(ExchangePyBase.start_network(connector))
        await first_stop_started.wait()
        newer_stop = asyncio.create_task(ExchangePyBase.stop_network(connector))

        try:
            await asyncio.wait_for(newer_stop, timeout=0.2)
            self.assertEqual(0, tracker.start.call_count)
            release_first_stop.set()
            await older_start
            self.assertEqual(0, tracker.start.call_count)
        finally:
            release_first_stop.set()
            await asyncio.gather(older_start, newer_stop, return_exceptions=True)

    async def test_duplicate_starts_only_activate_newest_transaction(self):
        first_stop_started = asyncio.Event()
        release_first_stop = asyncio.Event()
        stop_calls = 0

        async def stop_and_wait():
            nonlocal stop_calls
            stop_calls += 1
            if stop_calls == 1:
                first_stop_started.set()
                await release_first_stop.wait()
            return True

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=stop_and_wait)
        connector = self._connector(tracker)
        first = asyncio.create_task(ExchangePyBase.start_network(connector))
        await first_stop_started.wait()
        second = asyncio.create_task(ExchangePyBase.start_network(connector))

        try:
            await asyncio.wait_for(second, timeout=0.2)
            self.assertEqual(1, tracker.start.call_count)
            release_first_stop.set()
            await first
            self.assertEqual(1, tracker.start.call_count)
        finally:
            release_first_stop.set()
            await asyncio.gather(first, second, return_exceptions=True)

    async def test_start_propagates_cancellation_swallowed_by_drain(self):
        drain_started = asyncio.Event()

        async def cancellation_resistant_drain():
            drain_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                return True

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=cancellation_resistant_drain)
        connector = self._connector(tracker)
        start_task = asyncio.create_task(ExchangePyBase.start_network(connector))
        await drain_started.wait()
        start_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await start_task
        tracker.start.assert_not_called()

    async def test_stop_cancellation_invalidates_all_tasks_and_keeps_resistant_work_indexed(self):
        drain_started = asyncio.Event()
        drain_release = asyncio.Event()
        status_cancelled = asyncio.Event()
        status_release = asyncio.Event()

        async def blocked_drain():
            drain_started.set()
            await drain_release.wait()
            return True

        async def cancellation_resistant_status_task():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                status_cancelled.set()
                await status_release.wait()

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=blocked_drain)
        status_task = asyncio.create_task(cancellation_resistant_status_task())
        connector = self._connector(tracker, _status_polling_task=status_task)
        stop_task = asyncio.create_task(ExchangePyBase.stop_network(connector))

        try:
            await drain_started.wait()
            await asyncio.wait_for(status_cancelled.wait(), timeout=0.2)
            stop_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await stop_task

            tracker.stop.assert_called_once_with()
            self.assertIsNone(connector._status_polling_task)
            self.assertIsNone(connector._trading_rules_polling_task)
            self.assertIsNone(connector._trading_fees_polling_task)
            self.assertIsNone(connector._user_stream_tracker_task)
            self.assertIsNone(connector._user_stream_event_listener_task)
            self.assertIsNone(connector._lost_orders_update_task)
            self.assertFalse(status_task.done())
            self.assertIn(status_task, connector._network_tasks_to_drain)
        finally:
            drain_release.set()
            status_release.set()
            status_task.cancel()
            await asyncio.gather(status_task, stop_task, return_exceptions=True)
            cleanup_tasks = tuple(getattr(connector, "_network_tasks_to_drain", ()))
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
