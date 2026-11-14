import asyncio
import gc
import logging
import weakref

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.core.utils.async_utils import GenerationOwnedTaskOwner


class _Generation:
    pass


class _ExceptionCountingTask(asyncio.Task):
    def __init__(self, coroutine, *, loop):
        super().__init__(coroutine, loop=loop)
        self.exception_calls = 0

    def exception(self):
        self.exception_calls += 1
        return super().exception()


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
