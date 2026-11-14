import asyncio
import gc
import logging
import weakref

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.core.utils.async_utils import GenerationOwnedTaskOwner, await_primary_then_cleanup


class _Generation:
    pass


class _ExceptionCountingTask(asyncio.Task):
    def __init__(self, coroutine, *, loop):
        super().__init__(coroutine, loop=loop)
        self.exception_calls = 0

    def exception(self):
        self.exception_calls += 1
        return super().exception()


class _CallbackRegistrationFailingTask(asyncio.Task):
    def add_done_callback(self, callback, *args, **kwargs):
        raise RuntimeError("callback registration failed")


class _CallbackRegistrationFailingExceptionCountingTask(_ExceptionCountingTask):
    def add_done_callback(self, callback, *args, **kwargs):
        raise RuntimeError("callback registration failed")


class GenerationOwnedTaskOwnerTests(IsolatedAsyncioWrapperTestCase):
    @staticmethod
    async def _cancellation_suppressing_task(started, cancellation_seen, release):
        started.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancellation_seen.set()

    @staticmethod
    def _owner(logger, callback):
        return GenerationOwnedTaskOwner(
            logger=logger,
            cleanup_error_message="cleanup failed",
            residual_warning_message="residual retained",
            on_task_done=callback,
        )

    async def test_create_task_rolls_back_when_factory_raises(self):
        owner = self._owner(MagicMock(), MagicMock())
        generation = _Generation()
        factory_error = RuntimeError("factory failed")

        with self.assertRaises(RuntimeError) as context:
            owner.create_task(generation, MagicMock(side_effect=factory_error))

        self.assertIs(factory_error, context.exception)
        self.assertIsNone(owner._current_generation)
        self.assertIsNone(owner._current_task)
        self.assertEqual({}, owner._tasks_by_generation)

    async def test_create_task_rejects_invalid_return_without_publishing(self):
        owner = self._owner(MagicMock(), MagicMock())
        generation = _Generation()

        with self.assertRaisesRegex(TypeError, "asyncio.Task"):
            owner.create_task(generation, object)

        self.assertIsNone(owner._current_generation)
        self.assertIsNone(owner._current_task)
        self.assertEqual({}, owner._tasks_by_generation)

    async def test_create_task_rolls_back_and_cancels_when_callback_registration_fails(self):
        owner = self._owner(MagicMock(), MagicMock())
        generation = _Generation()
        task = _CallbackRegistrationFailingTask(
            asyncio.sleep(10),
            loop=asyncio.get_running_loop(),
        )

        with self.assertRaisesRegex(RuntimeError, "callback registration failed"):
            owner.create_task(generation, lambda: task)
        await owner.wait_until_released()

        self.assertTrue(task.cancelled())
        self.assertIsNone(owner._current_generation)
        self.assertIsNone(owner._current_task)
        self.assertEqual({}, owner._tasks_by_generation)

    async def test_callback_registration_failure_retains_cancellation_suppressor_until_terminal(self):
        logger = MagicMock()
        owner = self._owner(logger, MagicMock())
        generation = _Generation()
        started = asyncio.Event()
        cancellation_seen = asyncio.Event()
        release = asyncio.Event()

        async def fail_after_cancellation():
            await self._cancellation_suppressing_task(started, cancellation_seen, release)
            raise ValueError("failed task terminal error")

        task = _CallbackRegistrationFailingExceptionCountingTask(
            fail_after_cancellation(),
            loop=asyncio.get_running_loop(),
        )
        await started.wait()

        with self.assertRaisesRegex(RuntimeError, "callback registration failed"):
            owner.create_task(generation, lambda: task)
        await cancellation_seen.wait()

        waiter = asyncio.create_task(owner.wait_until_released())
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())
        self.assertIs(generation, owner._current_generation)
        self.assertIs(task, owner.task_for(generation))
        self.assertIsNone(owner._current_task)
        self.assertEqual({task}, owner._residual_tasks)

        owner_ref = weakref.ref(owner)
        owner = None
        gc.collect()
        self.assertIsNotNone(owner_ref())

        release.set()
        await waiter
        with self.assertRaisesRegex(ValueError, "failed task terminal error"):
            task.result()
        await asyncio.sleep(0)

        self.assertEqual(1, task.exception_calls)
        logger.error.assert_called_once()
        for _ in range(3):
            gc.collect()
            await asyncio.sleep(0)
        self.assertIsNone(owner_ref())

    async def test_create_task_rejects_reentrant_factory_without_inner_or_outer_orphan(self):
        owner = self._owner(MagicMock(), MagicMock())
        generation = _Generation()
        created_tasks = []

        def inner_factory():
            task = asyncio.create_task(asyncio.sleep(10))
            created_tasks.append(task)
            return task

        def outer_factory():
            try:
                owner.create_task(generation, inner_factory)
            except RuntimeError:
                pass
            task = asyncio.create_task(asyncio.sleep(10))
            created_tasks.append(task)
            return task

        try:
            with self.assertRaisesRegex(RuntimeError, "Reentrant task creation"):
                owner.create_task(generation, outer_factory)
            await owner.wait_until_released()

            self.assertEqual(1, len(created_tasks))
            self.assertTrue(created_tasks[0].cancelled())
            self.assertIsNone(owner._current_generation)
            self.assertIsNone(owner._current_task)
            self.assertEqual({}, owner._tasks_by_generation)
        finally:
            for task in created_tasks:
                task.cancel()
            await asyncio.gather(*created_tasks, return_exceptions=True)

    async def test_create_task_invokes_one_factory_for_repeated_callers(self):
        owner = self._owner(MagicMock(), MagicMock())
        generation = _Generation()
        factory_calls = 0

        def factory():
            nonlocal factory_calls
            factory_calls += 1
            return asyncio.create_task(asyncio.sleep(10))

        task = owner.create_task(generation, factory)
        try:
            with self.assertRaisesRegex(RuntimeError, "generation already owns a live task"):
                owner.create_task(generation, factory)

            self.assertEqual(1, factory_calls)
            self.assertIs(task, owner.current_task)
            self.assertIs(task, owner.task_for(generation))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            owner.end_generation(generation)

    async def test_wait_until_released_waits_for_open_generation_without_live_task(self):
        owner = self._owner(MagicMock(), MagicMock())
        generation = _Generation()
        owner.begin_generation(generation)
        waiter = asyncio.create_task(owner.wait_until_released())

        await asyncio.sleep(0)
        self.assertFalse(waiter.done())

        owner.end_generation(generation)
        await asyncio.wait_for(waiter, timeout=0.1)

    def test_residual_tasks_is_an_immutable_snapshot(self):
        owner = self._owner(MagicMock(), MagicMock())

        snapshot = owner.residual_tasks

        self.assertIsInstance(snapshot, frozenset)
        self.assertIsNot(snapshot, owner._residual_tasks)

    async def test_ended_residual_finalizes_once_and_releases_generation_and_task(self):
        callback_counts = {}

        def on_task_done(done_task):
            task_id = id(done_task)
            callback_counts[task_id] = callback_counts.get(task_id, 0) + 1

        owner = self._owner(logging.getLogger(__name__), on_task_done)
        generation = _Generation()
        generation_ref = weakref.ref(generation)
        started = asyncio.Event()
        cancellation_seen = asyncio.Event()
        release = asyncio.Event()
        task = asyncio.create_task(
            self._cancellation_suppressing_task(started, cancellation_seen, release)
        )
        task_id = id(task)
        task_ref = weakref.ref(task)

        owner.bind_task(generation, task)
        await started.wait()
        await owner.cleanup(task, timeout=0.01)
        await cancellation_seen.wait()
        owner.end_generation(generation)

        self.assertIs(generation, owner._current_generation)
        self.assertIs(generation, generation_ref())

        release.set()
        await task
        await asyncio.sleep(0)
        await asyncio.gather(*(owner.cleanup(task, timeout=0.01) for _ in range(3)))
        await owner.cleanup(task, timeout=0.01)

        self.assertEqual(1, callback_counts[task_id])
        self.assertIsNone(owner._current_generation)
        self.assertIsNone(owner._current_task)
        self.assertEqual({}, owner._tasks_by_generation)
        self.assertEqual(set(), owner._residual_tasks)

        generation = None
        task = None
        for _ in range(3):
            await asyncio.sleep(0)
            gc.collect()

        self.assertIsNone(generation_ref())
        self.assertIsNone(task_ref())

    async def test_concurrent_and_repeated_finalization_retrieves_and_logs_failure_once(self):
        callback_counts = {}

        def on_task_done(done_task):
            task_id = id(done_task)
            callback_counts[task_id] = callback_counts.get(task_id, 0) + 1

        async def fail():
            raise RuntimeError("task failed")

        logger = MagicMock()
        owner = self._owner(logger, on_task_done)
        generation = _Generation()
        task = _ExceptionCountingTask(fail(), loop=asyncio.get_running_loop())
        task_id = id(task)
        owner.bind_task(generation, task)
        owner.end_generation(generation)

        await asyncio.wait({task})
        await asyncio.sleep(0)
        await asyncio.gather(*(owner.cleanup(task, timeout=0.01) for _ in range(3)))
        await owner.cleanup(task, timeout=0.01)

        self.assertEqual(1, callback_counts[task_id])
        self.assertEqual(1, task.exception_calls)
        logger.error.assert_called_once()
        self.assertIsNone(owner._current_generation)
        self.assertEqual({}, owner._tasks_by_generation)

    async def test_repeated_old_task_finalization_does_not_clear_replacement_generation(self):
        callback_counts = {}

        def on_task_done(done_task):
            task_id = id(done_task)
            callback_counts[task_id] = callback_counts.get(task_id, 0) + 1

        owner = self._owner(logging.getLogger(__name__), on_task_done)
        old_generation = _Generation()
        old_task = asyncio.create_task(asyncio.sleep(0))
        old_task_id = id(old_task)
        owner.bind_task(old_generation, old_task)
        owner.end_generation(old_generation)
        await old_task
        await asyncio.sleep(0)

        replacement_generation = _Generation()
        replacement_task = asyncio.create_task(asyncio.sleep(10))
        replacement_task_id = id(replacement_task)
        owner.bind_task(replacement_generation, replacement_task)

        await asyncio.gather(*(owner.cleanup(old_task, timeout=0.01) for _ in range(3)))
        await owner.cleanup(old_task, timeout=0.01)

        self.assertEqual(1, callback_counts[old_task_id])
        self.assertIs(replacement_generation, owner._current_generation)
        self.assertIs(replacement_task, owner._current_task)
        self.assertIs(replacement_task, owner.task_for(replacement_generation))

        replacement_task.cancel()
        await asyncio.gather(replacement_task, return_exceptions=True)
        await asyncio.sleep(0)
        self.assertEqual(1, callback_counts[replacement_task_id])
        owner.end_generation(replacement_generation)

    async def test_throwing_completion_callback_is_isolated_and_child_failure_is_finalized_once(self):
        callback_count = 0

        def on_task_done(done_task):
            nonlocal callback_count
            callback_count += 1
            raise RuntimeError("completion callback failed")

        async def fail():
            raise ValueError("child failed")

        logger = MagicMock()
        owner = self._owner(logger, on_task_done)
        generation = _Generation()
        task = _ExceptionCountingTask(fail(), loop=asyncio.get_running_loop())
        owner.bind_task(generation, task)
        owner.end_generation(generation)

        loop = asyncio.get_running_loop()
        loop_callback_failures = []
        previous_exception_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _, context: loop_callback_failures.append(context))
        primary_error = LookupError("sentinel")

        async def primary():
            raise primary_error

        try:
            await asyncio.wait({task})
            await asyncio.sleep(0)
            with self.assertRaises(LookupError) as primary_context:
                await await_primary_then_cleanup(
                    primary=primary(),
                    cleanup=lambda: owner.cleanup(task, timeout=0.01),
                    logger=logger,
                    cleanup_error_message="owner cleanup failed",
                )
            await asyncio.gather(*(owner.cleanup(task, timeout=0.01) for _ in range(32)))
            await owner.cleanup(task, timeout=0.01)
        finally:
            loop.set_exception_handler(previous_exception_handler)

        self.assertIs(primary_error, primary_context.exception)
        self.assertEqual(1, callback_count)
        self.assertEqual(1, task.exception_calls)
        self.assertEqual([], loop_callback_failures)
        self.assertEqual(2, logger.error.call_count)
        logged_messages = [call.args[0] for call in logger.error.call_args_list]
        self.assertIn("cleanup failed", logged_messages)
        self.assertIn("Generation-owned task completion callback failed", logged_messages)
        self.assertIsNone(owner._current_generation)
        self.assertIsNone(owner._current_task)
        self.assertEqual({}, owner._tasks_by_generation)
        self.assertEqual(set(), owner._residual_tasks)
