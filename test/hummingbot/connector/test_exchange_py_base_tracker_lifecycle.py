import asyncio
import inspect
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange_py_base import ExchangePyBase, NetworkLifecycleError


class _LifecycleHarnessBase(ExchangePyBase):
    @property
    def is_trading_required(self):
        return False


class _PreSuperLifecycleConnector(_LifecycleHarnessBase):
    async def start_network(self):
        self.pre_super_entered.set()
        await self.pre_super_release.wait()
        await super().start_network()
        self.publications += 1

    async def stop_network(self):
        await super().stop_network()


class _PostSuperLifecycleConnector(_LifecycleHarnessBase):
    async def start_network(self):
        await super().start_network()
        self.post_super_entered.set()
        await self.post_super_release.wait()
        self.publications += 1

    async def stop_network(self):
        await super().stop_network()


class _OmittedStopReturnConnector(_LifecycleHarnessBase):
    async def start_network(self):
        await super().start_network()

    async def stop_network(self):
        await super().stop_network()
        self.subclass_cleanup_calls += 1


for _connector_class in (
    _LifecycleHarnessBase,
    _PreSuperLifecycleConnector,
    _PostSuperLifecycleConnector,
    _OmittedStopReturnConnector,
):
    _connector_class.__abstractmethods__ = frozenset()


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

    @staticmethod
    def _subclass_connector(connector_class, order_book_tracker, **overrides):
        connector = connector_class.__new__(connector_class)
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
            publications=0,
            subclass_cleanup_calls=0,
        )
        values.update(overrides)
        connector.__dict__.update(values)
        connector._set_order_book_tracker(order_book_tracker)
        return connector

    async def test_stop_network_awaits_order_book_tracker_drain(self):
        order_book_tracker = MagicMock()
        order_book_tracker.stop_and_wait = AsyncMock(return_value=True)
        connector = self._connector(order_book_tracker)

        result = await ExchangePyBase.stop_network(connector)

        order_book_tracker.stop_and_wait.assert_awaited_once_with()
        self.assertIsNone(result)
        self.assertEqual(set(), connector._network_tasks_to_drain)

    async def test_newer_stop_waits_for_entire_pre_super_override(self):
        newer_stop_drain_started = asyncio.Event()

        async def drain():
            newer_stop_drain_started.set()
            return True

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=drain)
        connector = self._subclass_connector(
            _PreSuperLifecycleConnector,
            tracker,
            pre_super_entered=asyncio.Event(),
            pre_super_release=asyncio.Event(),
        )
        older_start = asyncio.create_task(connector.start_network())
        await asyncio.wait_for(connector.pre_super_entered.wait(), timeout=0.2)
        newer_stop = asyncio.create_task(connector.stop_network())

        try:
            await asyncio.wait({newer_stop}, timeout=0.05)
            self.assertFalse(newer_stop.done())
            connector.pre_super_release.set()
            await asyncio.wait_for(older_start, timeout=0.2)
            await asyncio.wait_for(newer_stop, timeout=0.2)
            publications_at_stop_return = connector.publications
            await asyncio.sleep(0)
            self.assertEqual(1, publications_at_stop_return)
            self.assertEqual(publications_at_stop_return, connector.publications)
            self.assertGreaterEqual(tracker.stop.call_count, 2)
        finally:
            connector.pre_super_release.set()
            await asyncio.gather(older_start, newer_stop, return_exceptions=True)

    async def test_newer_stop_waits_for_entire_post_super_override(self):
        newer_stop_drain_started = asyncio.Event()
        drain_calls = 0

        async def drain():
            nonlocal drain_calls
            drain_calls += 1
            if drain_calls == 2:
                newer_stop_drain_started.set()
            return True

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=drain)
        connector = self._subclass_connector(
            _PostSuperLifecycleConnector,
            tracker,
            post_super_entered=asyncio.Event(),
            post_super_release=asyncio.Event(),
        )
        older_start = asyncio.create_task(connector.start_network())
        await asyncio.wait_for(connector.post_super_entered.wait(), timeout=0.2)
        newer_stop = asyncio.create_task(connector.stop_network())

        try:
            await asyncio.wait({newer_stop}, timeout=0.05)
            self.assertFalse(newer_stop.done())
            connector.post_super_release.set()
            await asyncio.wait_for(older_start, timeout=0.2)
            await asyncio.wait_for(newer_stop, timeout=0.2)
            publications_at_stop_return = connector.publications
            await asyncio.sleep(0)
            self.assertEqual(1, publications_at_stop_return)
            self.assertEqual(publications_at_stop_return, connector.publications)
        finally:
            connector.post_super_release.set()
            await asyncio.gather(older_start, newer_stop, return_exceptions=True)

    async def test_base_newer_stop_prevents_older_start_from_activating_late(self):
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
            await asyncio.wait({newer_stop}, timeout=0.05)
            self.assertFalse(newer_stop.done())
            self.assertEqual(0, tracker.start.call_count)
            release_first_stop.set()
            await older_start
            await newer_stop
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
            await asyncio.wait({second}, timeout=0.05)
            self.assertFalse(second.done())
            self.assertEqual(0, tracker.start.call_count)
            release_first_stop.set()
            await first
            await second
            self.assertEqual(1, tracker.start.call_count)
        finally:
            release_first_stop.set()
            await asyncio.gather(first, second, return_exceptions=True)

    async def test_subclass_start_cancellation_releases_serializer_and_runs_stop_cleanup(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        connector = self._subclass_connector(
            _PreSuperLifecycleConnector,
            tracker,
            pre_super_entered=asyncio.Event(),
            pre_super_release=asyncio.Event(),
        )
        start_task = asyncio.create_task(connector.start_network())
        await asyncio.wait_for(connector.pre_super_entered.wait(), timeout=0.2)
        start_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(start_task, timeout=0.2)
        await asyncio.wait_for(connector.stop_network(), timeout=0.2)
        self.assertGreaterEqual(tracker.stop.call_count, 1)

    async def test_nested_override_wrappers_preserve_signature_and_do_not_deadlock(self):
        events = []

        class Parent(_OmittedStopReturnConnector):
            async def start_network(self):
                events.append("parent-start-before")
                await super().start_network()
                events.append("parent-start-after")

            async def stop_network(self):
                events.append("parent-stop-before")
                await super().stop_network()
                events.append("parent-stop-after")

        class Child(Parent):
            async def start_network(self):
                events.append("child-start-before")
                await super().start_network()
                events.append("child-start-after")

            async def stop_network(self):
                events.append("child-stop-before")
                await super().stop_network()
                events.append("child-stop-after")

        Parent.__abstractmethods__ = frozenset()
        Child.__abstractmethods__ = frozenset()
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        connector = self._subclass_connector(Child, tracker)

        await asyncio.wait_for(connector.start_network(), timeout=0.2)
        await asyncio.wait_for(connector.stop_network(), timeout=0.2)

        self.assertEqual(["self"], list(inspect.signature(Child.start_network).parameters))
        self.assertIn("child-start-after", events)
        self.assertIn("parent-stop-after", events)

    async def test_false_tracker_drain_and_residual_make_public_stop_false(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=False)
        tracker.residual_subscriptions = frozenset({"PAIR-USDT"})
        connector = self._connector(tracker)

        self.assertIs(await ExchangePyBase.stop_network(connector), False)

        tracker.stop_and_wait = AsyncMock(return_value=True)
        self.assertIs(await ExchangePyBase.stop_network(connector), False)

    async def test_omitted_override_return_propagates_false_stop_truth(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=False)
        tracker.residual_subscriptions = frozenset({"PAIR-USDT"})
        connector = self._subclass_connector(_OmittedStopReturnConnector, tracker)

        self.assertIs(await connector.stop_network(), False)
        self.assertEqual(1, connector.subclass_cleanup_calls)

    async def test_start_raises_stable_lifecycle_error_after_unclean_stop(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=False)
        tracker.residual_subscriptions = frozenset({"PAIR-USDT"})
        connector = self._subclass_connector(_OmittedStopReturnConnector, tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "unclean"):
            await connector.start_network()
        tracker.start.assert_not_called()

    async def test_user_stream_cleanup_false_makes_public_stop_false(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        user_stream_tracker = MagicMock()
        user_stream_tracker.stop = AsyncMock(return_value=False)
        connector = self._connector(tracker, _user_stream_tracker=user_stream_tracker)

        self.assertIs(await ExchangePyBase.stop_network(connector), False)

    async def test_user_stream_cleanup_failure_raises_stable_lifecycle_error(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        user_stream_tracker = MagicMock()
        user_stream_tracker.stop = AsyncMock(side_effect=ValueError("cleanup failed"))
        connector = self._connector(tracker, _user_stream_tracker=user_stream_tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "cleanup task failed"):
            await ExchangePyBase.stop_network(connector)

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

            restart_task = asyncio.create_task(ExchangePyBase.start_network(connector))
            await asyncio.wait({restart_task}, timeout=0.05)
            self.assertFalse(restart_task.done())
            tracker.start.assert_not_called()
            drain_release.set()
            status_release.set()
            await asyncio.wait_for(restart_task, timeout=0.2)
            tracker.start.assert_called_once_with()
        finally:
            drain_release.set()
            status_release.set()
            status_task.cancel()
            await asyncio.gather(status_task, stop_task, return_exceptions=True)
            if "restart_task" in locals():
                restart_task.cancel()
                await asyncio.gather(restart_task, return_exceptions=True)
            cleanup_tasks = tuple(getattr(connector, "_network_tasks_to_drain", ()))
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
