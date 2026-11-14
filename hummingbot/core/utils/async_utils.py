import asyncio
import inspect
import logging
import time
import weakref
from typing import Any, Awaitable, Callable, Dict, Optional, Set, Tuple, TypeVar


_Result = TypeVar("_Result")


class GenerationOwnedTaskOwner:
    """Owns at most one live task and binds it to the generation that created it."""

    def __init__(
        self,
        logger: Any,
        cleanup_error_message: str,
        residual_warning_message: str,
        on_task_done: Callable[[asyncio.Task], None],
    ):
        self._logger = logger
        self._cleanup_error_message = cleanup_error_message
        self._residual_warning_message = residual_warning_message
        self._on_task_done = on_task_done
        self._tasks_by_generation: Dict[int, Tuple[object, asyncio.Task]] = {}
        self._current_generation: Optional[object] = None
        self._current_task: Optional[asyncio.Task] = None
        self._residual_tasks: Set[asyncio.Task] = set()
        self._cancellation_requested_tasks: weakref.WeakSet = weakref.WeakSet()
        self._handled_tasks: weakref.WeakSet = weakref.WeakSet()

    @property
    def current_task(self) -> Optional[asyncio.Task]:
        self._prune_finished_tasks()
        return self._current_task

    @property
    def residual_tasks(self) -> Set[asyncio.Task]:
        self._prune_finished_tasks()
        return self._residual_tasks

    def begin_generation(self, generation: object):
        self._prune_finished_tasks()
        live_task = self._live_task()
        if live_task is not None:
            existing = self.task_for(generation)
            if existing is live_task:
                return
            raise RuntimeError("Cannot start a task generation while its predecessor is still owned")
        self._current_generation = generation

    def bind_task(self, generation: object, task: asyncio.Task):
        self.begin_generation(generation)
        existing = self.task_for(generation)
        if existing is task:
            return
        if existing is not None and not existing.done():
            raise RuntimeError("The generation already owns a live task")
        self._tasks_by_generation[id(generation)] = (generation, task)
        self._current_generation = generation
        self._current_task = task
        task.add_done_callback(self._task_done)

    def task_for(self, generation: object) -> Optional[asyncio.Task]:
        entry = self._tasks_by_generation.get(id(generation))
        if entry is None or entry[0] is not generation:
            return None
        return entry[1]

    def is_current_generation(self, generation: object) -> bool:
        return self._current_generation is generation

    def end_generation(self, generation: object):
        if self._current_generation is not generation:
            return
        if self._current_task is None or self._current_task.done():
            self._current_generation = None

    async def wait_until_released(self):
        while True:
            live_task = self._live_task()
            if live_task is None:
                return
            await asyncio.wait({live_task})
            self._finalize_task(live_task)

    async def cleanup(self, task: asyncio.Task, timeout: float):
        if not task.done() and task not in self._cancellation_requested_tasks:
            self._cancellation_requested_tasks.add(task)
            task.cancel()

        owner_cancellation = None
        deadline = asyncio.get_running_loop().time() + timeout
        while not task.done():
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait({task}, timeout=remaining)
            except asyncio.CancelledError as cancellation:
                if owner_cancellation is None:
                    owner_cancellation = cancellation

        if task.done():
            self._finalize_task(task)
        else:
            live_residuals = [residual for residual in self._residual_tasks if not residual.done()]
            if live_residuals and task not in live_residuals:
                raise RuntimeError("More than one residual generation-owned task would be retained")
            if task not in self._residual_tasks:
                self._residual_tasks.add(task)
                self._logger.warning(self._residual_warning_message)

        if owner_cancellation is not None:
            raise owner_cancellation

    def _live_task(self) -> Optional[asyncio.Task]:
        self._prune_finished_tasks()
        for residual in self._residual_tasks:
            if not residual.done():
                return residual
        if self._current_task is not None and not self._current_task.done():
            return self._current_task
        return None

    def _prune_finished_tasks(self):
        tasks = list(self._residual_tasks)
        if self._current_task is not None:
            tasks.append(self._current_task)
        for task in tasks:
            if task.done():
                self._finalize_task(task)

    def _task_done(self, task: asyncio.Task):
        self._finalize_task(task)

    def _finalize_task(self, task: asyncio.Task):
        self._residual_tasks.discard(task)
        for generation_id, (_, generation_task) in list(self._tasks_by_generation.items()):
            if generation_task is task:
                self._tasks_by_generation.pop(generation_id, None)
        if self._current_task is task:
            self._current_task = None
        self._on_task_done(task)

        if task in self._handled_tasks:
            return
        self._handled_tasks.add(task)
        if task.cancelled():
            return
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return
        if exception is not None:
            self._logger.error(
                self._cleanup_error_message,
                exc_info=(type(exception), exception, exception.__traceback__),
            )


async def await_primary_then_cleanup(
    primary: Optional[Awaitable[_Result]],
    cleanup: Optional[Callable[[], Awaitable[None]]],
    logger: Any,
    cleanup_error_message: str,
) -> Optional[_Result]:
    """Run cleanup without allowing it to replace the primary awaitable's outcome."""

    result = None
    primary_error = None
    try:
        if primary is not None:
            result = await primary
    except BaseException as error:
        primary_error = error

    cleanup_cancellation = None
    if cleanup is not None:
        try:
            await cleanup()
        except asyncio.CancelledError as cancellation:
            cleanup_cancellation = cancellation
        except Exception as cleanup_error:
            logger.error(
                cleanup_error_message,
                exc_info=(type(cleanup_error), cleanup_error, cleanup_error.__traceback__),
            )

    if primary_error is not None:
        raise primary_error
    if cleanup_cancellation is not None:
        raise cleanup_cancellation
    return result


async def safe_wrapper(c):
    try:
        return await c
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logging.getLogger(__name__).error(f"Unhandled error in background task: {str(e)}", exc_info=True)


def safe_ensure_future(coro, *args, **kwargs):
    return asyncio.ensure_future(safe_wrapper(coro), *args, **kwargs)


async def safe_gather(*args, **kwargs):
    try:
        return await asyncio.gather(*args, **kwargs)
    except Exception as e:
        logging.getLogger(__name__).debug(f"Unhandled error in background task: {str(e)}", exc_info=True)
        raise


async def wait_til(condition_func, timeout=10):
    start_time = time.perf_counter()
    while True:
        if condition_func():
            return
        elif time.perf_counter() - start_time > timeout:
            raise Exception(f"{inspect.getsource(condition_func).strip()} condition is never met. Time out reached.")
        else:
            await asyncio.sleep(0.1)


async def run_command(*args):
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    return stdout.decode().strip()


def call_sync(coro,
              loop: asyncio.AbstractEventLoop,
              timeout: float = 30.0):
    import threading
    if threading.current_thread() != threading.main_thread():  # pragma: no cover
        fut = asyncio.run_coroutine_threadsafe(
            asyncio.wait_for(coro, timeout),
            loop
        )
        return fut.result()
    elif not loop.is_running():
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            logging.getLogger(__name__).debug(
                "Runtime error in call_sync - Using new event loop to exec coro",
                exc_info=True
            )
            loop = asyncio.new_event_loop()
    return loop.run_until_complete(asyncio.wait_for(coro, timeout))
