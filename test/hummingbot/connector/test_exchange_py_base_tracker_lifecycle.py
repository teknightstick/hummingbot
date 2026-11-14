import asyncio
import inspect
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange_py_base import ExchangePyBase, NetworkLifecycleError
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker


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


class _QueuedCancellationConnector(_LifecycleHarnessBase):
    async def start_network(self):
        await super().start_network()
        self.publications += 1

    async def stop_network(self):
        await super().stop_network()


class _PendingCancellationConnector(_LifecycleHarnessBase):
    async def start_network(self):
        await super().start_network()

    async def stop_network(self):
        result = await super().stop_network()
        asyncio.current_task().cancel("caller-message")
        return False if result is None else result


class _NoBaseStartConnector(_LifecycleHarnessBase):
    async def start_network(self):
        self.publications += 1


class _NoBaseStopConnector(_LifecycleHarnessBase):
    async def stop_network(self):
        return None


class _FailingStopConnector(_LifecycleHarnessBase):
    async def stop_network(self):
        await super().stop_network()
        raise ValueError("override-cleanup")


class _PostSuperFailingStartConnector(_LifecycleHarnessBase):
    async def start_network(self):
        await super().start_network()
        self._status_polling_task = asyncio.create_task(asyncio.Event().wait())
        self.post_super_task = self._status_polling_task
        raise RuntimeError("post-super-start-failed: secret detail")

    async def stop_network(self):
        await super().stop_network()


class _PreSuperFailingStartConnector(_LifecycleHarnessBase):
    async def start_network(self):
        self._status_polling_task = asyncio.create_task(asyncio.Event().wait())
        self.pre_super_task = self._status_polling_task
        raise RuntimeError("pre-super-start-failed: secret detail")

    async def stop_network(self):
        await super().stop_network()


class _PreSuperCancelledStopConnector(_LifecycleHarnessBase):
    async def start_network(self):
        await super().start_network()

    async def stop_network(self):
        self.pre_super_entered.set()
        await self.pre_super_release.wait()
        await super().stop_network()


for _connector_class in (
    _LifecycleHarnessBase,
    _PreSuperLifecycleConnector,
    _PostSuperLifecycleConnector,
    _OmittedStopReturnConnector,
    _QueuedCancellationConnector,
    _PendingCancellationConnector,
    _NoBaseStartConnector,
    _NoBaseStopConnector,
    _FailingStopConnector,
    _PostSuperFailingStartConnector,
    _PreSuperFailingStartConnector,
    _PreSuperCancelledStopConnector,
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

    async def test_stop_network_stops_real_order_book_tracker_exactly_once_before_drain(self):
        tracker = OrderBookTracker(data_source=MagicMock(), trading_pairs=[])
        connector = self._connector(tracker)
        stop = MagicMock(wraps=tracker.stop)
        stop_and_wait = AsyncMock(wraps=tracker.stop_and_wait)
        tracker.stop = stop
        tracker.stop_and_wait = stop_and_wait

        result = await ExchangePyBase.stop_network(connector)

        self.assertIsNone(result)
        stop.assert_called_once_with()
        stop_and_wait.assert_awaited_once_with()
        self.assertEqual(set(), connector._network_tasks_to_drain)

    async def test_stop_network_dispatches_through_real_tracker_subclass_public_override(self):
        class OverridingTracker(OrderBookTracker):
            def __init__(self):
                super().__init__(data_source=MagicMock(), trading_pairs=[])
                self.public_stop_and_wait_calls = 0

            async def stop_and_wait(self):
                self.public_stop_and_wait_calls += 1
                return await super().stop_and_wait()

        tracker = OverridingTracker()
        stop = MagicMock(wraps=tracker.stop)
        tracker.stop = stop
        connector = self._connector(tracker)

        result = await ExchangePyBase.stop_network(connector)

        self.assertIsNone(result)
        self.assertEqual(1, tracker.public_stop_and_wait_calls)
        stop.assert_called_once_with()

    async def test_stop_network_ignores_colliding_protected_name_on_unrelated_tracker(self):
        class UnrelatedTracker:
            def __init__(self):
                self.stop_calls = 0
                self.public_stop_and_wait_calls = 0
                self.protected_drain_calls = 0

            def stop(self):
                self.stop_calls += 1

            async def stop_and_wait(self):
                self.public_stop_and_wait_calls += 1
                return True

            async def _drain_stopped_tasks(self, required_argument):
                self.protected_drain_calls += 1
                return required_argument

        tracker = UnrelatedTracker()
        connector = self._connector(tracker)

        result = await ExchangePyBase.stop_network(connector)

        self.assertIsNone(result)
        self.assertEqual(1, tracker.stop_calls)
        self.assertEqual(1, tracker.public_stop_and_wait_calls)
        self.assertEqual(0, tracker.protected_drain_calls)

    async def test_unused_already_stopped_token_does_not_suppress_later_direct_stop(self):
        class NonDelegatingTracker(OrderBookTracker):
            def __init__(self):
                super().__init__(data_source=MagicMock(), trading_pairs=[])
                self.public_stop_and_wait_calls = 0

            async def stop_and_wait(self):
                self.public_stop_and_wait_calls += 1
                return True

        tracker = NonDelegatingTracker()
        stop = MagicMock(wraps=tracker.stop)
        tracker.stop = stop
        connector = self._connector(tracker)

        self.assertIsNone(await ExchangePyBase.stop_network(connector))
        self.assertEqual(1, tracker.public_stop_and_wait_calls)
        stop.assert_called_once_with()

        self.assertTrue(await OrderBookTracker.stop_and_wait(tracker, timeout=0.2))
        self.assertEqual(2, stop.call_count)

    async def test_public_override_error_clears_token_and_allows_clean_retry(self):
        class FailingOnceTracker(OrderBookTracker):
            def __init__(self):
                super().__init__(data_source=MagicMock(), trading_pairs=[])
                self.public_stop_and_wait_calls = 0

            async def stop_and_wait(self):
                self.public_stop_and_wait_calls += 1
                if self.public_stop_and_wait_calls == 1:
                    raise RuntimeError("override-drain-failed")
                return await super().stop_and_wait()

        tracker = FailingOnceTracker()
        stop = MagicMock(wraps=tracker.stop)
        tracker.stop = stop
        connector = self._connector(tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "cleanup task failed") as raised:
            await ExchangePyBase.stop_network(connector)

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)
        self.assertIsNone(await ExchangePyBase.stop_network(connector))
        self.assertEqual(2, tracker.public_stop_and_wait_calls)
        self.assertEqual(2, stop.call_count)

        self.assertTrue(await OrderBookTracker.stop_and_wait(tracker, timeout=0.2))
        self.assertEqual(3, stop.call_count)

    async def test_cancelled_connector_stop_does_not_leak_token_to_direct_call(self):
        class BlockingTracker(OrderBookTracker):
            def __init__(self):
                super().__init__(data_source=MagicMock(), trading_pairs=[])
                self.public_stop_and_wait_calls = 0
                self.public_stop_and_wait_entered = asyncio.Event()
                self.public_stop_and_wait_release = asyncio.Event()

            async def stop_and_wait(self):
                self.public_stop_and_wait_calls += 1
                self.public_stop_and_wait_entered.set()
                await self.public_stop_and_wait_release.wait()
                return await super().stop_and_wait()

        tracker = BlockingTracker()
        stop = MagicMock(wraps=tracker.stop)
        tracker.stop = stop
        connector = self._connector(tracker)
        connector_stop = asyncio.create_task(ExchangePyBase.stop_network(connector))

        try:
            await asyncio.wait_for(tracker.public_stop_and_wait_entered.wait(), timeout=0.2)
            connector_stop.cancel("connector-stop-cancelled")
            with self.assertRaises(asyncio.CancelledError) as raised:
                await connector_stop
            self.assertEqual(("connector-stop-cancelled",), raised.exception.args)

            self.assertTrue(await OrderBookTracker.stop_and_wait(tracker, timeout=0.2))
            self.assertEqual(2, stop.call_count)

            tracker.public_stop_and_wait_release.set()
            pending_cleanup = tuple(connector._network_tasks_to_drain)
            await asyncio.gather(*pending_cleanup)
            self.assertEqual(2, stop.call_count)
        finally:
            tracker.public_stop_and_wait_release.set()
            connector_stop.cancel()
            await asyncio.gather(connector_stop, return_exceptions=True)
            pending_cleanup = tuple(getattr(connector, "_network_tasks_to_drain", ()))
            await asyncio.gather(*pending_cleanup, return_exceptions=True)

    async def test_concurrent_connector_stops_each_use_one_public_one_shot_dispatch(self):
        class CountingTracker(OrderBookTracker):
            def __init__(self):
                super().__init__(data_source=MagicMock(), trading_pairs=[])
                self.public_stop_and_wait_calls = 0

            async def stop_and_wait(self):
                self.public_stop_and_wait_calls += 1
                await asyncio.sleep(0)
                return await super().stop_and_wait()

        tracker = CountingTracker()
        stop = MagicMock(wraps=tracker.stop)
        tracker.stop = stop
        connector = self._connector(tracker)

        results = await asyncio.gather(
            *(ExchangePyBase.stop_network(connector) for _ in range(4))
        )

        self.assertEqual([None, None, None, None], results)
        self.assertEqual(4, tracker.public_stop_and_wait_calls)
        self.assertEqual(4, stop.call_count)

    async def test_cancelled_queued_stop_does_not_allocate_or_invalidate_generation(self):
        first_drain_entered = asyncio.Event()
        first_drain_release = asyncio.Event()
        drain_calls = 0

        async def drain():
            nonlocal drain_calls
            drain_calls += 1
            if drain_calls == 1:
                first_drain_entered.set()
                await first_drain_release.wait()
            return True

        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(side_effect=drain)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_QueuedCancellationConnector, tracker)
        older_start = asyncio.create_task(connector.start_network())
        await asyncio.wait_for(first_drain_entered.wait(), timeout=0.2)
        queued_stop = asyncio.create_task(connector.stop_network())
        await asyncio.sleep(0)
        queued_stop.cancel("queued-stop")

        try:
            with self.assertRaises(asyncio.CancelledError) as raised:
                await queued_stop
            self.assertEqual(("queued-stop",), raised.exception.args)
            first_drain_release.set()
            await asyncio.wait_for(older_start, timeout=0.2)
            self.assertEqual(1, tracker.start.call_count)
            self.assertEqual(1, connector.publications)
            self.assertEqual(1, connector._network_lifecycle_generation)
            self.assertEqual({}, connector._network_lifecycle_transactions)
        finally:
            first_drain_release.set()
            await asyncio.gather(older_start, queued_stop, return_exceptions=True)

    async def test_pending_cancellation_wins_over_unclean_start_error_with_message(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_PendingCancellationConnector, tracker)
        task = asyncio.create_task(connector.start_network())

        with self.assertRaises(asyncio.CancelledError) as raised:
            await task

        self.assertEqual(("caller-message",), raised.exception.args)
        self.assertTrue(task.cancelled())
        tracker.start.assert_not_called()

    async def test_start_override_omitting_super_fails_closed_and_cleans_base_work(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_NoBaseStartConnector, tracker)
        pending = asyncio.create_task(asyncio.Event().wait())
        connector._network_tasks_to_drain = {pending}

        with self.assertRaisesRegex(NetworkLifecycleError, "must call super"):
            await connector.start_network()

        self.assertTrue(pending.done())
        self.assertGreaterEqual(tracker.stop.call_count, 1)
        tracker.start.assert_not_called()

    async def test_stop_override_omitting_super_fails_closed_after_base_cleanup(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_NoBaseStopConnector, tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "must call super"):
            await connector.stop_network()

        tracker.stop.assert_called_once_with()
        tracker.stop_and_wait.assert_awaited_once_with()

    async def test_stop_override_exception_is_stable_lifecycle_error(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_FailingStopConnector, tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "override cleanup") as raised:
            await connector.stop_network()

        self.assertIsInstance(raised.exception.__cause__, ValueError)
        tracker.stop.assert_called_once_with()

    async def test_post_super_start_failure_fails_closed_with_stable_error(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_PostSuperFailingStartConnector, tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "start_network override failed") as raised:
            await connector.start_network()

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)
        self.assertNotIn("secret detail", str(raised.exception))
        self.assertGreaterEqual(tracker.stop.call_count, 2)
        self.assertIsNone(connector._status_polling_task)
        self.assertTrue(connector.post_super_task.done())
        self.assertEqual(set(), connector._network_tasks_to_drain)

    async def test_pre_super_start_failure_fails_closed_with_stable_error(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_PreSuperFailingStartConnector, tracker)

        with self.assertRaisesRegex(NetworkLifecycleError, "start_network override failed") as raised:
            await connector.start_network()

        self.assertIsInstance(raised.exception.__cause__, RuntimeError)
        self.assertNotIn("secret detail", str(raised.exception))
        tracker.stop.assert_called_once_with()
        self.assertIsNone(connector._status_polling_task)
        self.assertTrue(connector.pre_super_task.done())
        self.assertEqual(set(), connector._network_tasks_to_drain)

    async def test_pre_super_stop_cancellation_preserves_message_and_fails_closed(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        entered = asyncio.Event()
        release = asyncio.Event()
        connector = self._subclass_connector(
            _PreSuperCancelledStopConnector,
            tracker,
            pre_super_entered=entered,
            pre_super_release=release,
        )
        pending = asyncio.create_task(asyncio.Event().wait())
        connector._status_polling_task = pending
        stop_task = asyncio.create_task(connector.stop_network())
        await asyncio.wait_for(entered.wait(), timeout=0.2)
        stop_task.cancel("stop-caller-message")

        with self.assertRaises(asyncio.CancelledError) as raised:
            await asyncio.wait_for(stop_task, timeout=0.2)

        self.assertEqual(("stop-caller-message",), raised.exception.args)
        tracker.stop.assert_called_once_with()
        tracker.stop_and_wait.assert_awaited_once_with()
        self.assertIsNone(connector._status_polling_task)
        self.assertTrue(pending.done())
        release.set()

    async def test_public_stop_cancels_and_clears_registered_override_tasks(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        task_attributes = (
            "_funding_info_listener_task",
            "_funding_fee_polling_task",
            "_rate_limits_polling_task",
            "_queued_orders_task",
            "_orders_transactions_check_task",
            "_trade_ws_maintenance_task",
            "_trade_ws_listener_task",
            "_balance_update_task",
            "_position_update_task",
            "_balance_refresh_task",
        )
        tasks = {attribute: asyncio.create_task(asyncio.Event().wait()) for attribute in task_attributes}
        connector = self._connector(tracker, **tasks)

        self.assertIsNone(await ExchangePyBase.stop_network(connector))

        for attribute, task in tasks.items():
            self.assertIsNone(getattr(connector, attribute))
            self.assertTrue(task.done(), attribute)
        self.assertEqual(set(), connector._network_tasks_to_drain)

    async def test_stop_rescans_multiple_late_registered_task_generations(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_LifecycleHarnessBase, tracker)
        generation_count = 25
        tasks = []

        async def generation(index):
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                if index + 1 < generation_count:
                    replacement = asyncio.create_task(generation(index + 1))
                    tasks.append(replacement)
                    await asyncio.sleep(0)
                    connector._status_polling_task = replacement

        initial = asyncio.create_task(generation(0))
        tasks.append(initial)
        connector._status_polling_task = initial
        await asyncio.sleep(0)

        try:
            self.assertIsNone(await asyncio.wait_for(connector.stop_network(), timeout=0.2))
            self.assertEqual(generation_count, len(tasks))
            self.assertIsNone(connector._status_polling_task)
            self.assertTrue(all(task.done() for task in tasks))
            self.assertEqual(set(), connector._network_tasks_to_drain)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_stop_timeout_indexes_every_late_registered_task_generation(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(_LifecycleHarnessBase, tracker)
        generation_count = 11
        releases = [asyncio.Event() for _ in range(generation_count)]
        tasks = []

        async def generation(index):
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                if index + 1 < generation_count:
                    replacement = asyncio.create_task(generation(index + 1))
                    tasks.append(replacement)
                    await asyncio.sleep(0)
                    connector._status_polling_task = replacement
                await releases[index].wait()

        async def immediate_timeout(waited_tasks, timeout):
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return set(), {task for task in waited_tasks if not task.done()}

        initial = asyncio.create_task(generation(0))
        tasks.append(initial)
        connector._status_polling_task = initial
        await asyncio.sleep(0)

        try:
            with patch(
                "hummingbot.connector.exchange_py_base.asyncio.wait",
                new=AsyncMock(side_effect=immediate_timeout),
            ):
                self.assertIs(await asyncio.wait_for(connector.stop_network(), timeout=0.2), False)

            self.assertEqual(generation_count, len(tasks))
            live_tasks = [task for task in tasks if not task.done()]
            self.assertTrue(
                all(
                    task is connector._status_polling_task
                    or task in connector._network_tasks_to_drain
                    for task in live_tasks
                )
            )
            await asyncio.sleep(0)
            later_published = connector._status_polling_task
            if later_published is not None and not later_published.done():
                self.assertIn(later_published, connector._network_tasks_to_drain)

            for release in releases:
                release.set()
            self.assertIsNone(await asyncio.wait_for(connector.stop_network(), timeout=0.2))
            self.assertTrue(all(task.done() for task in tasks))
            self.assertIsNone(connector._status_polling_task)
            self.assertEqual(set(), connector._network_tasks_to_drain)
        finally:
            for release in releases:
                release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def test_completed_network_task_is_pruned_when_its_loop_stops_before_callbacks(self):
        connector = MagicMock()
        connector._network_tasks_to_drain = set()
        loop = asyncio.new_event_loop()

        async def finish_and_stop():
            loop.stop()
            return 1

        try:
            task = loop.create_task(finish_and_stop())
            ExchangePyBase._track_network_task(connector, task)
            loop.run_forever()

            self.assertTrue(task.done())
            self.assertEqual(0, len(connector._network_tasks_to_drain))
            self.assertEqual(1, task.result())
        finally:
            loop.close()

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
            self.assertEqual(1, tracker.start.call_count)
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
            self.assertEqual(2, tracker.start.call_count)
        finally:
            release_first_stop.set()
            await asyncio.gather(first, second, return_exceptions=True)

    async def test_subclass_start_cancellation_releases_serializer_and_runs_stop_cleanup(self):
        tracker = MagicMock()
        tracker.stop_and_wait = AsyncMock(return_value=True)
        tracker.residual_subscriptions = frozenset()
        connector = self._subclass_connector(
            _PreSuperLifecycleConnector,
            tracker,
            pre_super_entered=asyncio.Event(),
            pre_super_release=asyncio.Event(),
        )
        pending = asyncio.create_task(asyncio.Event().wait())
        connector._status_polling_task = pending
        start_task = asyncio.create_task(connector.start_network())
        await asyncio.wait_for(connector.pre_super_entered.wait(), timeout=0.2)
        start_task.cancel("start-caller-message")

        with self.assertRaises(asyncio.CancelledError) as raised:
            await asyncio.wait_for(start_task, timeout=0.2)
        self.assertEqual(("start-caller-message",), raised.exception.args)
        self.assertIsNone(connector._status_polling_task)
        self.assertTrue(pending.done())
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
