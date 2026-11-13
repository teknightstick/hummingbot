import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Deque, Dict, List, Optional, Tuple
from weakref import WeakValueDictionary

import pandas as pd

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.event.events import OrderBookTradeEvent
from hummingbot.logger import HummingbotLogger


_TRACKER_OWNED_CANCELLATION = object()


class OrderBookTrackerLifecycleError(RuntimeError):
    """Raised when a synchronous tracker start cannot safely cross pending cleanup."""


class OrderBookTrackerDataSourceType(Enum):
    REMOTE_API = 2
    EXCHANGE_API = 3


@dataclass(frozen=True)
class _OrderBookMessageEnvelope:
    generation: int
    message: OrderBookMessage


class _GenerationBoundQueue:
    """Output-only queue view that binds listener messages to one tracker generation."""

    def __init__(self, queue: asyncio.Queue, generation: int):
        self._queue = queue
        self._generation = generation

    async def put(self, message: OrderBookMessage):
        await self._queue.put(_OrderBookMessageEnvelope(self._generation, message))

    def put_nowait(self, message: OrderBookMessage):
        return self._queue.put_nowait(_OrderBookMessageEnvelope(self._generation, message))

    def __getattr__(self, name: str):
        return getattr(self._queue, name)


class _CancellationPreservingFuture(asyncio.Future):
    """Forwards cancellation to a child without losing caller cancellation."""

    def __init__(self, awaitable):
        super().__init__()
        self._child_started = False
        self._cancellation_args = None
        self._child = asyncio.ensure_future(self._run_child(awaitable))
        self._child.add_done_callback(self._child_done)

    async def _run_child(self, awaitable):
        self._child_started = True
        if self._cancellation_args is not None:
            asyncio.get_running_loop().call_soon(self._forward_cancellation)
        return await awaitable

    def _forward_cancellation(self):
        if self._child.done() or self._cancellation_args is None:
            return
        if self._cancellation_args:
            self._child.cancel(self._cancellation_args[0])
        else:
            self._child.cancel()

    @property
    def child(self) -> asyncio.Task:
        return self._child

    @staticmethod
    def _is_tracker_cancellation(cancellation_args: tuple) -> bool:
        return bool(cancellation_args) and cancellation_args[0] is _TRACKER_OWNED_CANCELLATION

    def cancel(self, msg=None):
        if self.done():
            return False
        cancellation_args = () if msg is None else (msg,)
        if (
            self._cancellation_args is None
            or not self._is_tracker_cancellation(cancellation_args)
            or self._is_tracker_cancellation(self._cancellation_args)
        ):
            self._cancellation_args = cancellation_args
        if self._child_started:
            self._forward_cancellation()
        # Remain pending until the child has terminated. This lets a later
        # caller cancellation replace an earlier tracker-owned sentinel.
        return False

    def _child_done(self, child: asyncio.Task):
        if self.done():
            return
        if self._cancellation_args is not None:
            if not child.cancelled():
                try:
                    child.exception()
                except asyncio.CancelledError:
                    pass
            self.set_exception(asyncio.CancelledError(*self._cancellation_args))
        elif child.cancelled():
            try:
                child.result()
            except asyncio.CancelledError as cancellation:
                self.set_exception(asyncio.CancelledError(*cancellation.args))
        else:
            try:
                self.set_result(child.result())
            except Exception as exception:
                self.set_exception(exception)


@dataclass(eq=False)
class _SubscriptionIncarnation:
    generation: int
    activation: Optional[_CancellationPreservingFuture] = None


@dataclass
class LatencyStats:
    """
    Tracks latency statistics with rolling window for recent samples.
    All times are in milliseconds.

    Supports sampling to reduce overhead on high-frequency message streams.
    """
    ROLLING_WINDOW_SIZE: int = 100  # Keep last 100 samples for recent average
    SAMPLE_RATE: int = 10  # Record 1 out of every N messages for latency (set to 1 to record all)

    count: int = 0
    total_ms: float = 0.0
    min_ms: float = float('inf')
    max_ms: float = 0.0
    _recent_samples: Deque = field(default_factory=lambda: deque(maxlen=100))
    _sample_counter: int = 0  # Internal counter for sampling

    def record(self, latency_ms: float):
        """
        Record a new latency sample.

        Uses sampling to reduce overhead - only records latency details
        for every SAMPLE_RATE messages, but always updates count.
        """
        self.count += 1
        self._sample_counter += 1

        # Always track min/max (cheap operations)
        if latency_ms < self.min_ms:
            self.min_ms = latency_ms
        if latency_ms > self.max_ms:
            self.max_ms = latency_ms

        # Only record full stats every SAMPLE_RATE messages to reduce overhead
        if self._sample_counter >= self.SAMPLE_RATE:
            self._sample_counter = 0
            self.total_ms += latency_ms * self.SAMPLE_RATE  # Approximate total
            self._recent_samples.append(latency_ms)

    @property
    def avg_ms(self) -> float:
        """All-time average latency."""
        return self.total_ms / self.count if self.count > 0 else 0.0

    @property
    def recent_avg_ms(self) -> float:
        """Average latency over recent samples."""
        return sum(self._recent_samples) / len(self._recent_samples) if self._recent_samples else 0.0

    @property
    def recent_samples_count(self) -> int:
        """Number of samples in the rolling window."""
        return len(self._recent_samples)

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "count": self.count,
            "total_ms": self.total_ms,
            "min_ms": self.min_ms if self.min_ms != float('inf') else 0.0,
            "max_ms": self.max_ms,
            "avg_ms": self.avg_ms,
            "recent_avg_ms": self.recent_avg_ms,
            "recent_samples_count": self.recent_samples_count,
        }


@dataclass
class OrderBookPairMetrics:
    """Metrics for a single trading pair."""
    trading_pair: str

    # Message counts
    diffs_processed: int = 0
    diffs_rejected: int = 0
    snapshots_processed: int = 0
    trades_processed: int = 0
    trades_rejected: int = 0

    # Timestamps (perf_counter for internal timing)
    last_diff_timestamp: float = 0.0
    last_snapshot_timestamp: float = 0.0
    last_trade_timestamp: float = 0.0
    tracking_start_time: float = 0.0

    # Latency tracking
    diff_processing_latency: LatencyStats = field(default_factory=LatencyStats)
    snapshot_processing_latency: LatencyStats = field(default_factory=LatencyStats)
    trade_processing_latency: LatencyStats = field(default_factory=LatencyStats)

    def messages_per_minute(self, current_time: float) -> Dict[str, float]:
        """Calculate messages per minute rates."""
        elapsed_minutes = (current_time - self.tracking_start_time) / 60.0 if self.tracking_start_time > 0 else 0
        if elapsed_minutes <= 0:
            return {"diffs": 0.0, "snapshots": 0.0, "trades": 0.0, "total": 0.0}

        diffs_per_min = self.diffs_processed / elapsed_minutes
        snapshots_per_min = self.snapshots_processed / elapsed_minutes
        trades_per_min = self.trades_processed / elapsed_minutes

        return {
            "diffs": diffs_per_min,
            "snapshots": snapshots_per_min,
            "trades": trades_per_min,
            "total": diffs_per_min + snapshots_per_min + trades_per_min,
        }

    def to_dict(self, current_time: float) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "trading_pair": self.trading_pair,
            "diffs_processed": self.diffs_processed,
            "diffs_rejected": self.diffs_rejected,
            "snapshots_processed": self.snapshots_processed,
            "trades_processed": self.trades_processed,
            "trades_rejected": self.trades_rejected,
            "last_diff_timestamp": self.last_diff_timestamp,
            "last_snapshot_timestamp": self.last_snapshot_timestamp,
            "last_trade_timestamp": self.last_trade_timestamp,
            "tracking_start_time": self.tracking_start_time,
            "messages_per_minute": self.messages_per_minute(current_time),
            "diff_latency": self.diff_processing_latency.to_dict(),
            "snapshot_latency": self.snapshot_processing_latency.to_dict(),
            "trade_latency": self.trade_processing_latency.to_dict(),
        }


@dataclass
class OrderBookTrackerMetrics:
    """Aggregate metrics for the entire order book tracker."""

    # Global message counts
    total_diffs_processed: int = 0
    total_diffs_rejected: int = 0
    total_diffs_queued: int = 0  # Messages queued before tracking ready
    total_snapshots_processed: int = 0
    total_snapshots_rejected: int = 0
    total_trades_processed: int = 0
    total_trades_rejected: int = 0

    # Timing
    tracker_start_time: float = 0.0

    # Global latency stats
    diff_processing_latency: LatencyStats = field(default_factory=LatencyStats)
    snapshot_processing_latency: LatencyStats = field(default_factory=LatencyStats)
    trade_processing_latency: LatencyStats = field(default_factory=LatencyStats)

    # Per-pair metrics
    per_pair_metrics: Dict[str, OrderBookPairMetrics] = field(default_factory=dict)

    def get_or_create_pair_metrics(self, trading_pair: str) -> OrderBookPairMetrics:
        """Get or create metrics for a trading pair."""
        if trading_pair not in self.per_pair_metrics:
            self.per_pair_metrics[trading_pair] = OrderBookPairMetrics(
                trading_pair=trading_pair,
                tracking_start_time=time.perf_counter(),
            )
        return self.per_pair_metrics[trading_pair]

    def remove_pair_metrics(self, trading_pair: str):
        """Remove metrics for a trading pair."""
        self.per_pair_metrics.pop(trading_pair, None)

    def messages_per_minute(self, current_time: float) -> Dict[str, float]:
        """Calculate global messages per minute rates."""
        elapsed_minutes = (current_time - self.tracker_start_time) / 60.0 if self.tracker_start_time > 0 else 0
        if elapsed_minutes <= 0:
            return {"diffs": 0.0, "snapshots": 0.0, "trades": 0.0, "total": 0.0}

        diffs_per_min = self.total_diffs_processed / elapsed_minutes
        snapshots_per_min = self.total_snapshots_processed / elapsed_minutes
        trades_per_min = self.total_trades_processed / elapsed_minutes

        return {
            "diffs": diffs_per_min,
            "snapshots": snapshots_per_min,
            "trades": trades_per_min,
            "total": diffs_per_min + snapshots_per_min + trades_per_min,
        }

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        current_time = time.perf_counter()
        return {
            "total_diffs_processed": self.total_diffs_processed,
            "total_diffs_rejected": self.total_diffs_rejected,
            "total_diffs_queued": self.total_diffs_queued,
            "total_snapshots_processed": self.total_snapshots_processed,
            "total_snapshots_rejected": self.total_snapshots_rejected,
            "total_trades_processed": self.total_trades_processed,
            "total_trades_rejected": self.total_trades_rejected,
            "tracker_start_time": self.tracker_start_time,
            "uptime_seconds": current_time - self.tracker_start_time if self.tracker_start_time > 0 else 0,
            "messages_per_minute": self.messages_per_minute(current_time),
            "diff_latency": self.diff_processing_latency.to_dict(),
            "snapshot_latency": self.snapshot_processing_latency.to_dict(),
            "trade_latency": self.trade_processing_latency.to_dict(),
            "per_pair_metrics": {
                pair: metrics.to_dict(current_time)
                for pair, metrics in self.per_pair_metrics.items()
            },
        }


class OrderBookTracker:
    PAST_DIFF_WINDOW_SIZE: int = 32
    _obt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._obt_logger is None:
            cls._obt_logger = logging.getLogger(__name__)
        return cls._obt_logger

    def __init__(self, data_source: OrderBookTrackerDataSource, trading_pairs: List[str], domain: Optional[str] = None):
        self._domain: Optional[str] = domain
        self._data_source: OrderBookTrackerDataSource = data_source
        self._trading_pairs: List[str] = trading_pairs
        self._initial_trading_pairs: Tuple[str, ...] = tuple(trading_pairs)
        self._order_books_initialized: asyncio.Event = asyncio.Event()
        self._tracking_tasks: Dict[str, asyncio.Task] = {}
        self._order_books: Dict[str, OrderBook] = {}
        self._tracking_message_queues: Dict[str, asyncio.Queue] = {}
        self._past_diffs_windows: Dict[str, Deque] = defaultdict(lambda: deque(maxlen=self.PAST_DIFF_WINDOW_SIZE))
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._saved_message_queues: Dict[str, Deque[OrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))
        self._generation: int = 0
        self._active_generation: Optional[int] = None
        self._tasks_to_drain: set[asyncio.Task] = set()
        self._in_flight_transactions: set[asyncio.Task] = set()
        self._pair_transaction_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self._dynamic_subscriptions: set[str] = set()
        self._dynamic_subscription_generations: Dict[str, int] = {}
        self._dynamic_subscription_owners: Dict[str, object] = {}
        self._residual_subscriptions: set[str] = set()
        self._residual_subscription_generations: Dict[str, int] = {}
        self._residual_subscription_owners: Dict[str, object] = {}
        self._shutdown_unsubscribe_tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_unsubscribe_generations: Dict[str, int] = {}
        self._shutdown_unsubscribe_owners: Dict[str, object] = {}

        self._emit_trade_event_task: Optional[asyncio.Task] = None
        self._init_order_books_task: Optional[asyncio.Task] = None
        self._order_book_diff_listener_task: Optional[asyncio.Task] = None
        self._order_book_trade_listener_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_listener_task: Optional[asyncio.Task] = None
        self._order_book_diff_router_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_router_task: Optional[asyncio.Task] = None
        self._update_last_trade_prices_task: Optional[asyncio.Task] = None
        self._order_book_stream_listener_task: Optional[asyncio.Task] = None

        # Metrics tracking
        self._metrics: OrderBookTrackerMetrics = OrderBookTrackerMetrics()

    @property
    def metrics(self) -> OrderBookTrackerMetrics:
        """Access order book tracker metrics."""
        return self._metrics

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        return self._data_source

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_books

    @property
    def ready(self) -> bool:
        return self._order_books_initialized.is_set()

    @property
    def snapshot(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        return {
            trading_pair: order_book.snapshot
            for trading_pair, order_book in self._order_books.items()
        }

    @property
    def residual_subscriptions(self) -> frozenset[str]:
        """Remote subscriptions whose last cleanup attempt did not succeed."""
        return frozenset(self._residual_subscriptions)

    async def _run_tracker_task(self, method_name: str, *args):
        try:
            return await getattr(self, method_name)(*args)
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().error(
                f"Unhandled error in order book tracker task {method_name}: {exception}",
                exc_info=True,
            )

    async def _run_data_source_task(self, generation: int, method_name: str, *args):
        token = self._data_source.bind_raw_message_generation(generation)
        try:
            return await getattr(self._data_source, method_name)(*args)
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().error(
                f"Unhandled error in order book data source task {method_name}: {exception}",
                exc_info=True,
            )
        finally:
            self._data_source.reset_raw_message_generation(token)

    def _schedule_tracker_task(self, method_name: str, *args) -> asyncio.Task:
        return asyncio.ensure_future(self._run_tracker_task(method_name, *args))

    def _schedule_data_source_task(self, generation: int, method_name: str, *args) -> asyncio.Task:
        return asyncio.ensure_future(self._run_data_source_task(generation, method_name, *args))

    def start(self):
        self.stop()
        self._prune_completed_subscription_cleanups()
        if self._dynamic_subscriptions or self._residual_subscriptions or any(
            not task.done() for task in self._shutdown_unsubscribe_tasks.values()
        ):
            raise OrderBookTrackerLifecycleError(
                "Order book tracker cleanup is pending; await stop_and_wait() before start()."
            )
        generation = self._generation
        self._active_generation = generation
        self._data_source.activate_raw_message_generation(generation)
        self._metrics.tracker_start_time = time.perf_counter()
        self._init_order_books_task = self._schedule_tracker_task("_init_order_books", generation)
        self._emit_trade_event_task = self._schedule_tracker_task("_emit_trade_event_loop", generation)
        self._order_book_diff_listener_task = self._schedule_data_source_task(
            generation,
            "listen_for_order_book_diffs",
            self._ev_loop,
            _GenerationBoundQueue(self._order_book_diff_stream, generation),
        )
        self._order_book_trade_listener_task = self._schedule_data_source_task(
            generation,
            "listen_for_trades",
            self._ev_loop,
            _GenerationBoundQueue(self._order_book_trade_stream, generation),
        )
        self._order_book_snapshot_listener_task = self._schedule_data_source_task(
            generation,
            "listen_for_order_book_snapshots",
            self._ev_loop,
            _GenerationBoundQueue(self._order_book_snapshot_stream, generation),
        )
        self._order_book_stream_listener_task = self._schedule_data_source_task(
            generation,
            "listen_for_subscriptions",
        )
        self._order_book_diff_router_task = self._schedule_tracker_task("_order_book_diff_router", generation)
        self._order_book_snapshot_router_task = self._schedule_tracker_task(
            "_order_book_snapshot_router", generation
        )
        self._update_last_trade_prices_task = self._schedule_tracker_task(
            "_update_last_trade_prices_loop", generation
        )

    def stop(self):
        active_generation = self._active_generation
        tasks_to_drain = [
            task for task in (
                self._init_order_books_task,
                self._emit_trade_event_task,
                self._order_book_diff_listener_task,
                self._order_book_trade_listener_task,
                self._order_book_snapshot_listener_task,
                self._order_book_diff_router_task,
                self._order_book_snapshot_router_task,
                self._update_last_trade_prices_task,
                self._order_book_stream_listener_task,
                *self._tracking_tasks.values(),
                *self._in_flight_transactions,
            ) if task is not None
        ]
        self._generation += 1
        self._active_generation = None
        self._data_source.deactivate_raw_message_generation(active_generation)
        self._order_books_initialized.clear()
        self._order_book_diff_stream = asyncio.Queue()
        self._order_book_snapshot_stream = asyncio.Queue()
        self._order_book_trade_stream = asyncio.Queue()
        if self._init_order_books_task is not None:
            self._init_order_books_task.cancel()
            self._init_order_books_task = None
        if self._emit_trade_event_task is not None:
            self._emit_trade_event_task.cancel()
            self._emit_trade_event_task = None
        if self._order_book_diff_listener_task is not None:
            self._order_book_diff_listener_task.cancel()
            self._order_book_diff_listener_task = None
        if self._order_book_snapshot_listener_task is not None:
            self._order_book_snapshot_listener_task.cancel()
            self._order_book_snapshot_listener_task = None
        if self._order_book_trade_listener_task is not None:
            self._order_book_trade_listener_task.cancel()
            self._order_book_trade_listener_task = None

        if self._order_book_diff_router_task is not None:
            self._order_book_diff_router_task.cancel()
            self._order_book_diff_router_task = None
        if self._order_book_snapshot_router_task is not None:
            self._order_book_snapshot_router_task.cancel()
            self._order_book_snapshot_router_task = None
        if self._update_last_trade_prices_task is not None:
            self._update_last_trade_prices_task.cancel()
            self._update_last_trade_prices_task = None
        if self._order_book_stream_listener_task is not None:
            self._order_book_stream_listener_task.cancel()
            self._order_book_stream_listener_task = None
        if len(self._tracking_tasks) > 0:
            for _, task in self._tracking_tasks.items():
                task.cancel()
            self._tracking_tasks.clear()
        for task in tuple(self._in_flight_transactions):
            task.cancel(_TRACKER_OWNED_CANCELLATION)
        self._order_books.clear()
        self._tracking_message_queues.clear()
        self._saved_message_queues.clear()
        self._past_diffs_windows.clear()
        for task in tasks_to_drain:
            self._track_task_for_drain(task)
        for trading_pair in self._dynamic_subscriptions | self._residual_subscriptions:
            owner_generation = self._dynamic_subscription_generations.get(
                trading_pair,
                self._residual_subscription_generations.get(trading_pair, active_generation),
            )
            owner = self._dynamic_subscription_owners.get(
                trading_pair,
                self._residual_subscription_owners.get(trading_pair),
            )
            if owner_generation is not None:
                self._schedule_subscription_cleanup(trading_pair, owner_generation, owner)

    def _track_task_for_drain(self, task: Optional[asyncio.Task]):
        if task is None:
            return
        if task.done():
            self._generation_task_done(task)
        elif task not in self._tasks_to_drain:
            self._tasks_to_drain.add(task)
            task.add_done_callback(self._generation_task_done)

    def _generation_task_done(self, task: asyncio.Task):
        self._tasks_to_drain.discard(task)
        if task.cancelled():
            return
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return
        if exception is not None:
            self.logger().error(
                "Error while draining an order book tracker generation.",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    async def stop_and_wait(self, timeout: float = 10.0) -> bool:
        """Stop immediately, then wait up to ``timeout`` seconds for owned tasks to drain."""
        self.stop()
        deadline = self._ev_loop.time() + timeout
        while self._tasks_to_drain:
            for task in tuple(self._tasks_to_drain):
                if task.done():
                    self._generation_task_done(task)
            if not self._tasks_to_drain:
                break
            remaining = deadline - self._ev_loop.time()
            if remaining <= 0:
                self.logger().warning(
                    f"Timed out draining {len(self._tasks_to_drain)} order book tracker task(s) "
                    f"after {timeout:.1f} seconds."
                )
                return False
            _, pending = await asyncio.wait(set(self._tasks_to_drain), timeout=remaining)
            if pending:
                self.logger().warning(
                    f"Timed out draining {len(pending)} order book tracker task(s) after {timeout:.1f} seconds."
                )
                return False
            await asyncio.sleep(0)
        self._prune_completed_subscription_cleanups()
        return not self._residual_subscriptions

    def _pair_transaction_lock(self, trading_pair: str) -> asyncio.Lock:
        lock = self._pair_transaction_locks.get(trading_pair)
        if lock is None:
            lock = asyncio.Lock()
            self._pair_transaction_locks[trading_pair] = lock
        return lock

    def _register_current_transaction(self) -> Optional[asyncio.Task]:
        task = asyncio.current_task()
        if task is not None:
            self._in_flight_transactions.add(task)
        return task

    @staticmethod
    def _cancelled_by_tracker(cancellation: asyncio.CancelledError) -> bool:
        return bool(cancellation.args) and cancellation.args[0] is _TRACKER_OWNED_CANCELLATION

    @classmethod
    def _preferred_cancellation(
        cls,
        earlier: asyncio.CancelledError,
        later: asyncio.CancelledError,
    ) -> asyncio.CancelledError:
        if not cls._cancelled_by_tracker(later):
            return later
        if not cls._cancelled_by_tracker(earlier):
            return earlier
        return later

    async def _await_owned_operation(self, awaitable):
        """Drain a child operation while preserving the newest explicit cancellation."""
        return await _CancellationPreservingFuture(awaitable)

    def _subscription_owner_matches(self, trading_pair: str, generation: int, owner: object) -> bool:
        current_generation = self._dynamic_subscription_generations.get(trading_pair)
        if owner is None:
            return current_generation == generation
        return (
            current_generation == generation
            and self._dynamic_subscription_owners.get(trading_pair) is owner
        )

    def _clear_subscription_ownership(self, trading_pair: str, generation: int, owner: object) -> bool:
        if not self._subscription_owner_matches(trading_pair, generation, owner):
            return False
        self._dynamic_subscriptions.discard(trading_pair)
        self._dynamic_subscription_generations.pop(trading_pair, None)
        self._dynamic_subscription_owners.pop(trading_pair, None)
        if (
            self._residual_subscription_generations.get(trading_pair) == generation
            and self._residual_subscription_owners.get(trading_pair) is owner
        ):
            self._residual_subscriptions.discard(trading_pair)
            self._residual_subscription_generations.pop(trading_pair, None)
            self._residual_subscription_owners.pop(trading_pair, None)
        return True

    def _prune_completed_subscription_cleanups(self):
        for trading_pair, task in tuple(self._shutdown_unsubscribe_tasks.items()):
            if task.done():
                generation = self._shutdown_unsubscribe_generations.get(trading_pair)
                owner = self._shutdown_unsubscribe_owners.get(trading_pair)
                if generation is not None:
                    self._subscription_cleanup_done(trading_pair, generation, owner, task)
                self._generation_task_done(task)

    def _schedule_subscription_cleanup(
        self, trading_pair: str, generation: int, owner: object = None
    ) -> asyncio.Task:
        existing = self._shutdown_unsubscribe_tasks.get(trading_pair)
        if existing is not None:
            if not existing.done():
                return existing
            existing_generation = self._shutdown_unsubscribe_generations.get(trading_pair)
            existing_owner = self._shutdown_unsubscribe_owners.get(trading_pair)
            if existing_generation is not None:
                self._subscription_cleanup_done(
                    trading_pair, existing_generation, existing_owner, existing
                )
            self._generation_task_done(existing)
        task = asyncio.ensure_future(self._unsubscribe_for_cleanup(trading_pair, generation, owner))
        self._shutdown_unsubscribe_tasks[trading_pair] = task
        self._shutdown_unsubscribe_generations[trading_pair] = generation
        self._shutdown_unsubscribe_owners[trading_pair] = owner
        self._track_task_for_drain(task)
        task.add_done_callback(
            lambda completed, pair=trading_pair, generation=generation, owner=owner: self._subscription_cleanup_done(
                pair, generation, owner, completed
            )
        )
        return task

    def _subscription_cleanup_done(
        self, trading_pair: str, generation: int, owner: object, task: asyncio.Task
    ):
        if (
            self._shutdown_unsubscribe_tasks.get(trading_pair) is task
            and self._shutdown_unsubscribe_generations.get(trading_pair) == generation
            and self._shutdown_unsubscribe_owners.get(trading_pair) is owner
        ):
            self._shutdown_unsubscribe_tasks.pop(trading_pair, None)
            self._shutdown_unsubscribe_generations.pop(trading_pair, None)
            self._shutdown_unsubscribe_owners.pop(trading_pair, None)

    def _record_residual_subscription(
        self, trading_pair: str, generation: int, owner: object = None
    ):
        if self._subscription_owner_matches(trading_pair, generation, owner):
            self._residual_subscriptions.add(trading_pair)
            self._residual_subscription_generations[trading_pair] = generation
            self._residual_subscription_owners[trading_pair] = owner

    async def _unsubscribe_for_cleanup(
        self, trading_pair: str, generation: int, owner: object = None
    ) -> bool:
        if isinstance(owner, _SubscriptionIncarnation) and owner.activation is not None:
            activation = owner.activation
            if not activation.done():
                activation.cancel(_TRACKER_OWNED_CANCELLATION)
                try:
                    await asyncio.shield(activation)
                except asyncio.CancelledError:
                    if not activation.done():
                        self._record_residual_subscription(trading_pair, generation, owner)
                        raise
                except Exception:
                    pass

            activation_child = activation.child
            if not activation_child.done():
                # Cleanup must never race an activation that can still finish
                # remotely. Keep the incarnation owned and indexed until the
                # child reaches a terminal state.
                try:
                    await asyncio.shield(activation_child)
                except asyncio.CancelledError:
                    if not activation_child.done():
                        self._record_residual_subscription(trading_pair, generation, owner)
                        raise
                except Exception:
                    pass

            if not activation_child.done():
                self._record_residual_subscription(trading_pair, generation, owner)
                return False

            if not activation_child.cancelled():
                try:
                    activation_succeeded = bool(activation_child.result())
                except (asyncio.CancelledError, Exception):
                    activation_succeeded = True
                if not activation_succeeded:
                    self._clear_subscription_ownership(trading_pair, generation, owner)
                    return True

        try:
            success = await self._data_source.unsubscribe_from_trading_pair(trading_pair)
        except asyncio.CancelledError:
            self._record_residual_subscription(trading_pair, generation, owner)
            raise
        except Exception:
            self._record_residual_subscription(trading_pair, generation, owner)
            self.logger().exception(f"Error cleaning up subscription for {trading_pair}")
            return False
        if success:
            if self._clear_subscription_ownership(trading_pair, generation, owner):
                self._remove_pair_locally(trading_pair)
        else:
            self._record_residual_subscription(trading_pair, generation, owner)
            self.logger().warning(f"Failed to clean up subscription for {trading_pair}")
        return success

    async def _rollback_added_subscription(self, trading_pair: str, generation: int, owner: object):
        cleanup_task = self._shutdown_unsubscribe_tasks.get(trading_pair)
        if cleanup_task is None:
            if not self._subscription_owner_matches(trading_pair, generation, owner):
                return
            cleanup_task = self._schedule_subscription_cleanup(trading_pair, generation, owner)
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The cleanup coroutine records and logs its residual state. It must
            # never replace a primary lifecycle exception or cancellation.
            pass

    def _remove_pair_locally(self, trading_pair: str) -> Optional[asyncio.Task]:
        tracking_task = self._tracking_tasks.pop(trading_pair, None)
        if tracking_task is not None:
            tracking_task.cancel()
            self._track_task_for_drain(tracking_task)
        self._order_books.pop(trading_pair, None)
        self._tracking_message_queues.pop(trading_pair, None)
        self._past_diffs_windows.pop(trading_pair, None)
        self._saved_message_queues.pop(trading_pair, None)
        self._metrics.remove_pair_metrics(trading_pair)
        if trading_pair in self._trading_pairs:
            self._trading_pairs.remove(trading_pair)
        return tracking_task

    async def wait_ready(self):
        await self._order_books_initialized.wait()

    async def _update_last_trade_prices_loop(self, generation: Optional[int] = None):
        '''
        Updates last trade price for all order books through REST API, it is to initiate last_trade_price and as
        fall-back mechanism for when the web socket update channel fails.
        '''
        await self._order_books_initialized.wait()
        if not self._is_generation_current(generation):
            return
        while True:
            try:
                if not self._is_generation_current(generation):
                    return
                outdateds = [t_pair for t_pair, o_book in self._order_books.items()
                             if o_book.last_applied_trade < time.perf_counter() - (60. * 3)
                             and o_book.last_trade_price_rest_updated < time.perf_counter() - 5]
                if outdateds:
                    args = {"trading_pairs": outdateds}
                    if self._domain is not None:
                        args["domain"] = self._domain
                    last_prices = await self._data_source.get_last_traded_prices(**args)
                    if not self._is_generation_current(generation):
                        return
                    for trading_pair, last_price in last_prices.items():
                        order_book = self._order_books.get(trading_pair)
                        if order_book is not None:
                            order_book.last_trade_price = last_price
                            order_book.last_trade_price_rest_updated = time.perf_counter()
                else:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching last trade price.", exc_info=True)
                await asyncio.sleep(30)

    async def _initial_order_book_for_trading_pair(self, trading_pair: str) -> OrderBook:
        return await self._data_source.get_new_order_book(trading_pair)

    async def _init_order_books(self, generation: Optional[int] = None):
        """
        Initialize order books
        """
        trading_pairs = tuple(self._trading_pairs)
        initialized_order_books: Dict[str, OrderBook] = {}
        for index, trading_pair in enumerate(trading_pairs):
            order_book = await self._initial_order_book_for_trading_pair(trading_pair)
            if generation is not None and generation != self._active_generation:
                return
            initialized_order_books[trading_pair] = order_book
            self.logger().info(f"Initialized order book for {trading_pair}. "
                               f"{index + 1}/{len(trading_pairs)} completed.")
            await self._sleep(delay=1)
            if generation is not None and generation != self._active_generation:
                return
        for trading_pair, order_book in initialized_order_books.items():
            self._order_books[trading_pair] = order_book
            self._tracking_message_queues[trading_pair] = asyncio.Queue()
            self._tracking_tasks[trading_pair] = asyncio.ensure_future(
                self._track_single_book(trading_pair, generation)
            )
        self._order_books_initialized.set()

    async def add_trading_pair(self, trading_pair: str) -> bool:
        """
        Dynamically adds a new trading pair to the order book tracker.

        This method:
        1. Subscribes to the trading pair on the existing WebSocket connection
        2. Fetches the initial order book snapshot
        3. Creates the tracking queue and starts the tracking task
        4. Any messages received before the snapshot (stored in _saved_message_queues)
           will be automatically processed by _track_single_book

        :param trading_pair: the trading pair to add (e.g., "BTC-USDT")
        :return: True if successfully added, False otherwise
        """
        generation = self._active_generation
        transaction_task = self._register_current_transaction()
        raw_generation_token = self._data_source.bind_raw_message_generation(generation)
        subscription_may_be_active = False
        owner = None
        try:
            async with self._pair_transaction_lock(trading_pair):
                self._prune_completed_subscription_cleanups()
                cleanup_task = self._shutdown_unsubscribe_tasks.get(trading_pair)
                if cleanup_task is not None and not cleanup_task.done():
                    self.logger().warning(
                        f"Trading pair {trading_pair} cleanup is still pending"
                    )
                    return False
                if not self._is_generation_current(generation):
                    return False
                if (
                    trading_pair in self._order_books
                    or trading_pair in self._dynamic_subscriptions
                    or trading_pair in self._residual_subscriptions
                ):
                    self.logger().warning(f"Trading pair {trading_pair} is already being tracked")
                    return False

                await self._order_books_initialized.wait()
                if not self._is_generation_current(generation):
                    return False

                self.logger().info(f"Adding trading pair {trading_pair} to order book tracker...")
                owner = _SubscriptionIncarnation(generation=generation)
                self._dynamic_subscriptions.add(trading_pair)
                self._dynamic_subscription_generations[trading_pair] = generation
                self._dynamic_subscription_owners[trading_pair] = owner
                subscription_may_be_active = True
                owner.activation = _CancellationPreservingFuture(
                    self._data_source.subscribe_to_trading_pair(trading_pair)
                )
                subscribe_success = await owner.activation
                if not subscribe_success:
                    subscription_may_be_active = False
                    self._clear_subscription_ownership(trading_pair, generation, owner)
                if not subscribe_success:
                    self.logger().error(f"Failed to subscribe to {trading_pair} WebSocket channels")
                    return False
                if not self._is_generation_current(generation):
                    await self._rollback_added_subscription(trading_pair, generation, owner)
                    subscription_may_be_active = False
                    return False

                order_book = await self._await_owned_operation(
                    self._initial_order_book_for_trading_pair(trading_pair)
                )
                if not self._is_generation_current(generation):
                    await self._rollback_added_subscription(trading_pair, generation, owner)
                    subscription_may_be_active = False
                    return False

                if trading_pair not in self._trading_pairs:
                    self._trading_pairs.append(trading_pair)
                self._order_books[trading_pair] = order_book
                self._tracking_message_queues[trading_pair] = asyncio.Queue()
                self._tracking_tasks[trading_pair] = asyncio.ensure_future(
                    self._track_single_book(trading_pair, generation)
                )
                self._residual_subscriptions.discard(trading_pair)
                self._residual_subscription_generations.pop(trading_pair, None)
                self._residual_subscription_owners.pop(trading_pair, None)
                subscription_may_be_active = False
                self.logger().info(f"Successfully added trading pair {trading_pair} to order book tracker")
                return True
        except asyncio.CancelledError as cancellation:
            cancellation_to_raise = cancellation
            if subscription_may_be_active:
                try:
                    await self._rollback_added_subscription(trading_pair, generation, owner)
                except asyncio.CancelledError as cleanup_cancellation:
                    cancellation_to_raise = self._preferred_cancellation(
                        cancellation_to_raise, cleanup_cancellation
                    )
                except Exception:
                    self.logger().exception(
                        f"Error cleaning up subscription for cancelled trading pair add {trading_pair}"
                    )
            if self._cancelled_by_tracker(cancellation_to_raise):
                return False
            raise cancellation_to_raise
        except Exception:
            self.logger().exception(f"Error adding trading pair {trading_pair}")
            if subscription_may_be_active:
                await self._rollback_added_subscription(trading_pair, generation, owner)
            if self._is_generation_current(generation):
                self._remove_pair_locally(trading_pair)
            return False
        finally:
            self._data_source.reset_raw_message_generation(raw_generation_token)
            if transaction_task is not None:
                self._in_flight_transactions.discard(transaction_task)

    async def remove_trading_pair(self, trading_pair: str) -> bool:
        """
        Dynamically removes a trading pair from the order book tracker.

        This method:
        1. Cancels the tracking task for the pair
        2. Unsubscribes from WebSocket channels
        3. Cleans up all data structures

        :param trading_pair: the trading pair to remove (e.g., "SOL-USDT")
        :return: True if successfully removed, False otherwise
        """
        generation = self._active_generation
        transaction_task = self._register_current_transaction()
        raw_generation_token = self._data_source.bind_raw_message_generation(generation)
        owner = None
        try:
            async with self._pair_transaction_lock(trading_pair):
                self._prune_completed_subscription_cleanups()
                cleanup_task = self._shutdown_unsubscribe_tasks.get(trading_pair)
                if cleanup_task is not None and not cleanup_task.done():
                    return False
                if not self._is_generation_current(generation):
                    return False
                if trading_pair not in self._order_books:
                    self.logger().warning(f"Trading pair {trading_pair} is not being tracked")
                    return False

                self.logger().info(f"Removing trading pair {trading_pair} from order book tracker...")
                owner = self._dynamic_subscription_owners.get(trading_pair)

                async def unsubscribe_and_commit():
                    success = await self._data_source.unsubscribe_from_trading_pair(trading_pair)
                    tracking_task = None
                    if success:
                        if owner is not None:
                            if self._clear_subscription_ownership(trading_pair, generation, owner):
                                tracking_task = self._remove_pair_locally(trading_pair)
                        elif self._is_generation_current(generation):
                            tracking_task = self._remove_pair_locally(trading_pair)
                    return success, tracking_task

                try:
                    unsubscribe_success, tracking_task = await self._await_owned_operation(
                        unsubscribe_and_commit()
                    )
                except asyncio.CancelledError as cancellation:
                    if owner is not None:
                        self._record_residual_subscription(trading_pair, generation, owner)
                    raise cancellation
                except Exception:
                    self.logger().exception(f"Error unsubscribing from trading pair {trading_pair}")
                    return False

                if not unsubscribe_success:
                    self.logger().warning(f"Failed to unsubscribe from {trading_pair} WebSocket channels")
                    return False

                generation_is_current = self._is_generation_current(generation)

                if tracking_task is not None:
                    try:
                        await asyncio.shield(
                            asyncio.gather(tracking_task, return_exceptions=True)
                        )
                    except asyncio.CancelledError:
                        raise

                if not generation_is_current:
                    return False
                self.logger().info(f"Successfully removed trading pair {trading_pair} from order book tracker")
                return True
        except asyncio.CancelledError as cancellation:
            if self._cancelled_by_tracker(cancellation):
                return False
            raise cancellation
        finally:
            self._data_source.reset_raw_message_generation(raw_generation_token)
            if transaction_task is not None:
                self._in_flight_transactions.discard(transaction_task)

    def _message_for_generation(
        self, stream_item, generation: Optional[int]
    ) -> Optional[OrderBookMessage]:
        if generation is None:
            if isinstance(stream_item, _OrderBookMessageEnvelope):
                if stream_item.generation != self._active_generation:
                    return None
                return stream_item.message
            return stream_item
        if generation != self._active_generation:
            return None
        if not isinstance(stream_item, _OrderBookMessageEnvelope):
            return None
        if stream_item.generation != generation:
            return None
        return stream_item.message

    async def _order_book_diff_router(self, generation: Optional[int] = None):
        """
        Routes the real-time order book diff messages to the correct order book.
        """
        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0

        # Cache pair_metrics references to avoid repeated dict lookups
        pair_metrics_cache: Dict[str, OrderBookPairMetrics] = {}

        while True:
            try:
                stream_item = await self._order_book_diff_stream.get()
                ob_message = self._message_for_generation(stream_item, generation)
                if ob_message is None:
                    continue
                process_start = time.perf_counter()
                trading_pair: str = ob_message.trading_pair

                if trading_pair not in self._tracking_message_queues:
                    messages_queued += 1
                    self._metrics.total_diffs_queued += 1
                    # Save diff messages received before snapshots are ready
                    self._saved_message_queues[trading_pair].append(ob_message)
                    continue

                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                order_book: OrderBook = self._order_books[trading_pair]

                # Get or cache pair metrics (single lookup per pair)
                if trading_pair not in pair_metrics_cache:
                    pair_metrics_cache[trading_pair] = self._metrics.get_or_create_pair_metrics(trading_pair)
                pair_metrics = pair_metrics_cache[trading_pair]

                # Check the order book's initial update ID. If it's larger, don't bother.
                if order_book.snapshot_uid > ob_message.update_id:
                    messages_rejected += 1
                    self._metrics.total_diffs_rejected += 1
                    pair_metrics.diffs_rejected += 1
                    continue

                await message_queue.put(ob_message)
                messages_accepted += 1

                # Record metrics (latency uses sampling internally to reduce overhead)
                process_time_ms = (time.perf_counter() - process_start) * 1000
                self._metrics.total_diffs_processed += 1
                self._metrics.diff_processing_latency.record(process_time_ms)
                pair_metrics.diffs_processed += 1
                pair_metrics.last_diff_timestamp = process_start
                pair_metrics.diff_processing_latency.record(process_time_ms)

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(f"Diff messages processed: {messages_accepted}, "
                                        f"rejected: {messages_rejected}, queued: {messages_queued}")
                    messages_accepted = 0
                    messages_rejected = 0
                    messages_queued = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    async def _order_book_snapshot_router(self, generation: Optional[int] = None):
        """
        Route the real-time order book snapshot messages to the correct order book.
        """
        await self._order_books_initialized.wait()
        if generation is not None and generation != self._active_generation:
            return

        # Cache pair_metrics references
        pair_metrics_cache: Dict[str, OrderBookPairMetrics] = {}

        while True:
            try:
                stream_item = await self._order_book_snapshot_stream.get()
                ob_message = self._message_for_generation(stream_item, generation)
                if ob_message is None:
                    continue
                process_start = time.perf_counter()
                trading_pair: str = ob_message.trading_pair

                if trading_pair not in self._tracking_message_queues:
                    self._metrics.total_snapshots_rejected += 1
                    continue

                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                await message_queue.put(ob_message)

                # Record metrics
                process_time_ms = (time.perf_counter() - process_start) * 1000
                self._metrics.total_snapshots_processed += 1
                self._metrics.snapshot_processing_latency.record(process_time_ms)

                # Get or cache pair metrics
                if trading_pair not in pair_metrics_cache:
                    pair_metrics_cache[trading_pair] = self._metrics.get_or_create_pair_metrics(trading_pair)
                pair_metrics = pair_metrics_cache[trading_pair]
                pair_metrics.snapshots_processed += 1
                pair_metrics.last_snapshot_timestamp = process_start
                pair_metrics.snapshot_processing_latency.record(process_time_ms)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 5 seconds.", exc_info=True)
                await asyncio.sleep(5.0)

    def _is_generation_current(self, generation: Optional[int]) -> bool:
        return generation == self._active_generation

    async def _track_single_book(self, trading_pair: str, generation: Optional[int] = None):
        if not self._is_generation_current(generation):
            return
        past_diffs_window = self._past_diffs_windows[trading_pair]

        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                if not self._is_generation_current(generation):
                    return
                saved_messages: Deque[OrderBookMessage] = self._saved_message_queues[trading_pair]

                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()
                    if not self._is_generation_current(generation):
                        return

                if message.type is OrderBookMessageType.DIFF:
                    order_book.apply_diffs(message.bids, message.asks, message.update_id)
                    past_diffs_window.append(message)
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug(f"Processed {diff_messages_accepted} order book diffs for {trading_pair}.")
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[OrderBookMessage] = list(past_diffs_window)
                    order_book.restore_from_snapshot_and_diffs(message, past_diffs)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    async def _emit_trade_event_loop(self, generation: Optional[int] = None):
        last_message_timestamp: float = time.time()
        messages_accepted: int = 0
        messages_rejected: int = 0
        await self._order_books_initialized.wait()
        if generation is not None and generation != self._active_generation:
            return

        # Cache pair_metrics references
        pair_metrics_cache: Dict[str, OrderBookPairMetrics] = {}

        while True:
            try:
                stream_item = await self._order_book_trade_stream.get()
                trade_message = self._message_for_generation(stream_item, generation)
                if trade_message is None:
                    continue
                process_start = time.perf_counter()
                trading_pair: str = trade_message.trading_pair

                if trading_pair not in self._order_books:
                    messages_rejected += 1
                    self._metrics.total_trades_rejected += 1
                    continue

                order_book: OrderBook = self._order_books[trading_pair]
                order_book.apply_trade(OrderBookTradeEvent(
                    trading_pair=trade_message.trading_pair,
                    timestamp=trade_message.timestamp,
                    price=float(trade_message.content["price"]),
                    amount=float(trade_message.content["amount"]),
                    trade_id=trade_message.trade_id,
                    type=TradeType.SELL if
                    trade_message.content["trade_type"] == float(TradeType.SELL.value) else TradeType.BUY
                ))

                messages_accepted += 1

                # Record metrics
                process_time_ms = (time.perf_counter() - process_start) * 1000
                self._metrics.total_trades_processed += 1
                self._metrics.trade_processing_latency.record(process_time_ms)

                # Get or cache pair metrics
                if trading_pair not in pair_metrics_cache:
                    pair_metrics_cache[trading_pair] = self._metrics.get_or_create_pair_metrics(trading_pair)
                pair_metrics = pair_metrics_cache[trading_pair]
                pair_metrics.trades_processed += 1
                pair_metrics.last_trade_timestamp = process_start
                pair_metrics.trade_processing_latency.record(process_time_ms)

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(f"Trade messages processed: {messages_accepted}, rejected: {messages_rejected}")
                    messages_accepted = 0
                    messages_rejected = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    @staticmethod
    async def _sleep(delay: float):
        await asyncio.sleep(delay=delay)
