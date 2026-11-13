import asyncio
import logging
import time
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from collections.abc import Iterator, MutableMapping
from contextvars import ContextVar, Token
from typing import Any, Callable, Dict, List, Optional
from weakref import WeakValueDictionary

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


_UNBOUND_RAW_QUEUE_GENERATION = object()


class _DiscardingQueue:
    """Queue-shaped sink used by producers from retired websocket sessions."""

    async def put(self, item: Any):
        return None

    def put_nowait(self, item: Any):
        return None

    async def get(self):
        await asyncio.Future()

    def empty(self) -> bool:
        return True

    def qsize(self) -> int:
        return 0


class _DiscardingQueueMapping(MutableMapping[str, asyncio.Queue]):
    """Non-growing mapping sink for writes inherited from retired producers."""

    def __init__(self):
        self._sink = _DiscardingQueue()

    def __getitem__(self, key: str) -> asyncio.Queue:
        return self._sink

    def __setitem__(self, key: str, value: asyncio.Queue):
        return None

    def __delitem__(self, key: str):
        return None

    def __iter__(self) -> Iterator[str]:
        return iter(())

    def __len__(self) -> int:
        return 0


class _ActiveGenerationQueue:
    """Stable queue view for connector-lifetime consumers outside a generation context."""

    def __init__(self, data_source: "OrderBookTrackerDataSource", key: str):
        self._data_source = data_source
        self._key = key
        self._rotation = asyncio.Event()

    def generation_rotated(self):
        previous_rotation = self._rotation
        self._rotation = asyncio.Event()
        previous_rotation.set()

    def _queue(self) -> asyncio.Queue:
        return self._data_source._active_raw_message_queue(self._key)

    async def put(self, item: Any):
        await self._queue().put(item)

    def put_nowait(self, item: Any):
        return self._queue().put_nowait(item)

    async def get(self):
        while True:
            generation = self._data_source._active_raw_message_generation
            queue = self._queue()
            rotation = self._rotation
            get_task = asyncio.ensure_future(queue.get())
            rotation_task = asyncio.ensure_future(rotation.wait())
            try:
                done, _ = await asyncio.wait(
                    {get_task, rotation_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if (
                    get_task in done
                    and generation == self._data_source._active_raw_message_generation
                    and not self._data_source._raw_message_generation_is_retired(generation)
                ):
                    return get_task.result()
            finally:
                for task in (get_task, rotation_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(get_task, rotation_task, return_exceptions=True)

    def get_nowait(self):
        return self._queue().get_nowait()

    def empty(self) -> bool:
        return self._queue().empty()

    def qsize(self) -> int:
        return self._queue().qsize()

    def __getattr__(self, name: str):
        return getattr(self._queue(), name)


class _GenerationBoundMessageQueues(MutableMapping[str, asyncio.Queue]):
    """Mapping view that resolves raw queues from the producer task's session generation."""

    def __init__(self, data_source: "OrderBookTrackerDataSource"):
        self._data_source = data_source

    def _mapping(self) -> MutableMapping[str, asyncio.Queue]:
        return self._data_source._raw_message_mapping_for_context()

    def __getitem__(self, key: str) -> asyncio.Queue:
        if self._data_source._raw_message_queue_context.get() is _UNBOUND_RAW_QUEUE_GENERATION:
            return self._data_source._unbound_raw_message_queue(key)
        return self._mapping()[key]

    def __setitem__(self, key: str, value: asyncio.Queue):
        self._mapping()[key] = value

    def __delitem__(self, key: str):
        del self._mapping()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping())

    def __len__(self) -> int:
        return len(self._mapping())


class OrderBookTrackerDataSource(metaclass=ABCMeta):
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str]):
        self._trade_messages_queue_key = "trade"
        self._diff_messages_queue_key = "order_book_diff"
        self._snapshot_messages_queue_key = "order_book_snapshot"

        self._trading_pairs: List[str] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()
        self._raw_message_queue_context: ContextVar = ContextVar(
            f"raw_message_queue_generation_{id(self)}",
            default=_UNBOUND_RAW_QUEUE_GENERATION,
        )
        self._active_raw_message_generation: Optional[int] = None
        self._retired_raw_message_generation_floor: int = -1
        self._retired_raw_message_generations: set[int] = set()
        self._raw_message_queues: Dict[Optional[int], MutableMapping[str, asyncio.Queue]] = {
            None: defaultdict(asyncio.Queue)
        }
        self._discarded_raw_message_queues: MutableMapping[str, asyncio.Queue] = _DiscardingQueueMapping()
        self._unbound_raw_message_queues = WeakValueDictionary()
        self._message_queue_view = _GenerationBoundMessageQueues(self)
        self._ws_assistant: Optional[WSAssistant] = None

    @property
    def _message_queue(self) -> MutableMapping[str, asyncio.Queue]:
        return self._message_queue_view

    @_message_queue.setter
    def _message_queue(self, value: MutableMapping[str, asyncio.Queue]):
        generation = self._raw_message_queue_context.get()
        if generation is _UNBOUND_RAW_QUEUE_GENERATION:
            generation = self._active_raw_message_generation
        if self._raw_message_generation_is_retired(generation):
            return
        self._raw_message_queues[generation] = value
        if generation == self._active_raw_message_generation:
            self._wake_unbound_raw_message_queues()

    def activate_raw_message_generation(self, generation: int):
        """Create a fresh raw-frame queue set without changing the caller task's context."""
        if self._raw_message_generation_is_retired(generation):
            raise RuntimeError(f"Raw message generation {generation} has already been retired.")
        self._raw_message_queues[generation] = defaultdict(asyncio.Queue)
        self._active_raw_message_generation = generation
        self._wake_unbound_raw_message_queues()

    def deactivate_raw_message_generation(self, generation: Optional[int]):
        """Retire raw queues before cancelling their producer and parser tasks."""
        if generation is None:
            return
        self._retired_raw_message_generation_floor = max(
            self._retired_raw_message_generation_floor,
            generation,
        )
        # Keep one compatibility marker while the high-water mark rejects every
        # older inherited context without retaining one entry per generation.
        self._retired_raw_message_generations.clear()
        self._retired_raw_message_generations.add(self._retired_raw_message_generation_floor)
        queues = self._raw_message_queues.pop(generation, None)
        if queues is not None:
            queues.clear()
        if self._active_raw_message_generation == generation:
            self._active_raw_message_generation = None
        self._wake_unbound_raw_message_queues()

    def bind_raw_message_generation(self, generation: Optional[int]) -> Token:
        """Bind raw writes spawned by the current lifecycle transaction."""
        return self._raw_message_queue_context.set(generation)

    def reset_raw_message_generation(self, token: Token):
        self._raw_message_queue_context.reset(token)

    def _raw_message_generation_is_retired(self, generation) -> bool:
        return (
            isinstance(generation, int)
            and generation <= self._retired_raw_message_generation_floor
            and generation != self._active_raw_message_generation
        )

    def _raw_message_mapping_for_context(self) -> MutableMapping[str, asyncio.Queue]:
        generation = self._raw_message_queue_context.get()
        if generation is _UNBOUND_RAW_QUEUE_GENERATION:
            generation = self._active_raw_message_generation
        if self._raw_message_generation_is_retired(generation):
            return self._discarded_raw_message_queues
        if generation not in self._raw_message_queues:
            self._raw_message_queues[generation] = defaultdict(asyncio.Queue)
        return self._raw_message_queues[generation]

    def _active_raw_message_queue(self, key: str) -> asyncio.Queue:
        generation = self._active_raw_message_generation
        if generation not in self._raw_message_queues:
            self._raw_message_queues[generation] = defaultdict(asyncio.Queue)
        return self._raw_message_queues[generation][key]

    def _unbound_raw_message_queue(self, key: str) -> _ActiveGenerationQueue:
        queue = self._unbound_raw_message_queues.get(key)
        if queue is None:
            queue = _ActiveGenerationQueue(self, key)
            self._unbound_raw_message_queues[key] = queue
        return queue

    def _wake_unbound_raw_message_queues(self):
        for queue in tuple(self._unbound_raw_message_queues.values()):
            queue.generation_rotated()

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @property
    def order_book_create_function(self) -> Callable[[], OrderBook]:
        return self._order_book_create_function

    @order_book_create_function.setter
    def order_book_create_function(self, func: Callable[[], OrderBook]):
        self._order_book_create_function = func

    @abstractmethod
    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """
        Return a dictionary the trading_pair as key and the current price as value for each trading pair passed as
        parameter.
        This method is required by the order book tracker, to get the last traded prices when no new public trades
        are notified by the exchange.

        :param trading_pairs: list of trading pairs to get the prices for
        :param domain: which domain we are connecting to

        :return: Dictionary of associations between token pair and its latest price
        """
        raise NotImplementedError

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Creates a local instance of the exchange order book for a particular trading pair

        :param trading_pair: the trading pair for which the order book has to be retrieved

        :return: a local copy of the current order book in the exchange
        """
        snapshot_msg: OrderBookMessage = await self._order_book_snapshot(trading_pair=trading_pair)
        order_book: OrderBook = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                self._ws_assistant = ws  # Store reference for incremental subscriptions
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await self._sleep(1.0)
            finally:
                await self._on_order_stream_interruption(websocket_assistant=ws)
                if self._ws_assistant is ws:
                    self._ws_assistant = None

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order diffs events queue. For each event creates a diff message instance and adds it to the
        output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                diff_event = await message_queue.get()
                await self._parse_order_book_diff_message(raw_message=diff_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order snapshot events queue. For each event it creates a snapshot message instance and adds it to the
        output queue.
        This method also request the full order book content from the exchange using HTTP requests if it does not
        receive events during one hour.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        message_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                try:
                    snapshot_event = await asyncio.wait_for(message_queue.get(),
                                                            timeout=self.FULL_ORDER_BOOK_RESET_DELTA_SECONDS)
                    await self._parse_order_book_snapshot_message(raw_message=snapshot_event, message_queue=output)
                except asyncio.TimeoutError:
                    await self._request_order_book_snapshots(output=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book snapshots from exchange")
                await self._sleep(1.0)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the trade events queue. For each event creates a trade message instance and adds it to the output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created trade messages
        """
        message_queue = self._message_queue[self._trade_messages_queue_key]
        while True:
            try:
                trade_event = await message_queue.get()
                await self._parse_trade_message(raw_message=trade_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange")

    async def _request_order_book_snapshots(self, output: asyncio.Queue):
        for trading_pair in self._trading_pairs:
            try:
                snapshot = await self._order_book_snapshot(trading_pair=trading_pair)
                output.put_nowait(snapshot)
            except Exception:
                self.logger().exception(f"Unexpected error fetching order book snapshot for {trading_pair}.")
                raise

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.TRADE

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        raise NotImplementedError

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.DIFF

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        raise NotImplementedError

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.SNAPSHOT

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        raise NotImplementedError

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange

        :return: an instance of WSAssistant connected to the exchange
        """
        raise NotImplementedError

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        raise NotImplementedError

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Identifies the channel for a particular event message. Used to find the correct queue to add the message in

        :param event_message: the event received through the websocket connection

        :return: the message channel
        """
        raise NotImplementedError

    async def _process_message_for_unknown_channel(
        self, event_message: Dict[str, Any], websocket_assistant: WSAssistant
    ):
        """
        Processes a message coming from a not identified channel.
        Does nothing by default but allows subclasses to reimplement

        :param event_message: the event received through the websocket connection
        :param websocket_assistant: the websocket connection to use to interact with the exchange
        """
        pass

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = ws_response.data
            if data is not None:  # data will be None when the websocket is disconnected
                channel: str = self._channel_originating_message(event_message=data)
                valid_channels = self._get_messages_queue_keys()
                if channel in valid_channels:
                    self._message_queue[channel].put_nowait(data)
                else:
                    await self._process_message_for_unknown_channel(
                        event_message=data, websocket_assistant=websocket_assistant
                    )

    def _get_messages_queue_keys(self) -> List[str]:
        return [self._snapshot_messages_queue_key, self._diff_messages_queue_key, self._trade_messages_queue_key]

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        websocket_assistant and await websocket_assistant.disconnect()

    async def _sleep(self, delay):
        """
        Function added only to facilitate patching the sleep in unit tests without affecting the asyncio module
        """
        await asyncio.sleep(delay)

    def _time(self):
        return time.time()

    def add_trading_pair(self, trading_pair: str):
        """
        Adds a trading pair to the internal list of tracked pairs.

        :param trading_pair: the trading pair to add
        """
        if trading_pair not in self._trading_pairs:
            self._trading_pairs.append(trading_pair)

    def remove_trading_pair(self, trading_pair: str):
        """
        Removes a trading pair from the internal list of tracked pairs.

        :param trading_pair: the trading pair to remove
        """
        if trading_pair in self._trading_pairs:
            self._trading_pairs.remove(trading_pair)

    @abstractmethod
    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """
        Subscribes to order book and trade channels for a single trading pair on an
        existing WebSocket connection.

        :param trading_pair: the trading pair to subscribe to
        :return: True if subscription was successful, False otherwise
        """
        raise NotImplementedError

    @abstractmethod
    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """
        Unsubscribes from order book and trade channels for a single trading pair on an
        existing WebSocket connection.

        :param trading_pair: the trading pair to unsubscribe from
        :return: True if unsubscription was successful, False otherwise
        """
        raise NotImplementedError
