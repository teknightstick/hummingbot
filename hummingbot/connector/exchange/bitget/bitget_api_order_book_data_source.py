import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, NoReturn, Optional

from hummingbot.connector.exchange.bitget import bitget_constants as CONSTANTS, bitget_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import GenerationOwnedTaskOwner, await_primary_then_cleanup
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitget.bitget_exchange import BitgetExchange


class BitgetAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Data source for retrieving order book data from the Bitget exchange via REST and WebSocket APIs.
    """
    _DYNAMIC_SUBSCRIBE_ID_START = 100
    _next_subscribe_id: int = _DYNAMIC_SUBSCRIBE_ID_START
    _PING_TASK_CLEANUP_TIMEOUT = 1.0

    def __init__(
        self,
        trading_pairs: List[str],
        connector: 'BitgetExchange',
        api_factory: WebAssistantsFactory,
    ) -> None:
        super().__init__(trading_pairs)
        self._connector: 'BitgetExchange' = connector
        self._api_factory: WebAssistantsFactory = api_factory
        self._ping_task: Optional[asyncio.Task] = None
        self._ping_task_owner = GenerationOwnedTaskOwner(
            logger=self.logger(),
            cleanup_error_message="Error while cleaning up the Bitget WebSocket ping task",
            residual_warning_message=(
                "Bitget WebSocket ping task did not stop before the cleanup deadline; "
                "retaining residual ownership."
            ),
            on_task_done=self._release_ping_task,
        )

    async def get_last_traded_prices(
        self,
        trading_pairs: List[str],
        domain: Optional[str] = None
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _parse_pong_message(self) -> None:
        self.logger().debug("PING-PONG message for order book completed")

    async def _process_message_for_unknown_channel(
        self,
        event_message: Dict[str, Any],
        websocket_assistant: WSAssistant,
    ) -> None:
        if event_message == CONSTANTS.PUBLIC_WS_PONG_RESPONSE:
            await self._parse_pong_message()
        elif "event" in event_message:
            if event_message["event"] == "error":
                message = event_message.get("msg", "Unknown error")
                error_code = event_message.get("code", "Unknown code")
                raise IOError(f"Failed to subscribe to public channels: {message} ({error_code})")

            if event_message["event"] == "subscribe":
                channel: str = event_message["arg"]["channel"]
                self.logger().info(f"Subscribed to public channel: {channel.upper()}")
        else:
            self.logger().info(f"Message for unknown channel received: {event_message}")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> Optional[str]:
        channel: Optional[str] = None

        if "arg" in event_message and "action" in event_message:
            arg: Dict[str, Any] = event_message["arg"]
            response_channel: Optional[str] = arg.get("channel")

            if response_channel == CONSTANTS.PUBLIC_WS_BOOKS:
                action: Optional[str] = event_message.get("action")
                channels = {
                    "snapshot": self._snapshot_messages_queue_key,
                    "update": self._diff_messages_queue_key
                }
                channel = channels.get(action)
            elif response_channel == CONSTANTS.PUBLIC_WS_TRADE:
                channel = self._trade_messages_queue_key

        return channel

    async def _parse_any_order_book_message(
        self,
        data: Dict[str, Any],
        symbol: str,
        message_type: OrderBookMessageType,
    ) -> OrderBookMessage:
        """
        Parse a WebSocket message into an OrderBookMessage for snapshots or diffs.

        :param raw_message: The raw WebSocket message.
        :param message_type: The type of order book message (SNAPSHOT or DIFF).

        :return: The parsed order book message.
        """
        trading_pair: str = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        update_id: int = int(data["ts"])
        timestamp: float = update_id * 1e-3

        order_book_message_content: Dict[str, Any] = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": data["bids"],
            "asks": data["asks"],
        }

        return OrderBookMessage(
            message_type=message_type,
            content=order_book_message_content,
            timestamp=timestamp
        )

    async def _parse_order_book_diff_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ) -> None:
        diffs_data: Dict[str, Any] = raw_message["data"]
        symbol: str = raw_message["arg"]["instId"]

        for diff in diffs_data:
            diff_message: OrderBookMessage = await self._parse_any_order_book_message(
                data=diff,
                symbol=symbol,
                message_type=OrderBookMessageType.DIFF
            )

            message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ) -> None:
        snapshot_data: Dict[str, Any] = raw_message["data"]
        symbol: str = raw_message["arg"]["instId"]

        for snapshot in snapshot_data:
            snapshot_message: OrderBookMessage = await self._parse_any_order_book_message(
                data=snapshot,
                symbol=symbol,
                message_type=OrderBookMessageType.SNAPSHOT
            )

            message_queue.put_nowait(snapshot_message)

    async def _parse_trade_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ) -> None:
        data: List[Dict[str, Any]] = raw_message["data"]
        symbol: str = raw_message["arg"]["instId"]
        trading_pair: str = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

        for trade_data in data:
            trade_type: float = float(TradeType.BUY.value) \
                if trade_data["side"] == "buy" else float(TradeType.SELL.value)
            message_content: Dict[str, Any] = {
                "trade_id": int(trade_data["tradeId"]),
                "trading_pair": trading_pair,
                "trade_type": trade_type,
                "amount": trade_data["size"],
                "price": trade_data["price"],
            }
            trade_message = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=int(trade_data["ts"]) * 1e-3,
            )
            message_queue.put_nowait(trade_message)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._ping_task_owner.wait_until_released()
        websocket_assistant: WSAssistant = await self._api_factory.get_ws_assistant()
        try:
            await websocket_assistant.connect(
                ws_url=web_utils.public_ws_url(),
                message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE,
            )
        except BaseException:
            try:
                await websocket_assistant.disconnect()
            except BaseException as cleanup_error:
                self.logger().error(
                    "Error while disconnecting an incomplete Bitget WebSocket setup",
                    exc_info=(type(cleanup_error), cleanup_error, cleanup_error.__traceback__),
                )
            raise

        return websocket_assistant

    def _create_ping_task(self, websocket_assistant: WSAssistant) -> asyncio.Task:
        ping_task = self._ping_task_owner.create_task(
            websocket_assistant,
            lambda: asyncio.create_task(self.send_interval_ping(websocket_assistant)),
        )
        self._ping_task = (
            ping_task if self._ping_task_owner.task_for(websocket_assistant) is ping_task else None
        )
        return ping_task

    def _release_ping_task(self, ping_task: asyncio.Task):
        if self._ping_task is ping_task:
            self._ping_task = None

    async def _cleanup_ping_task(self, ping_task: asyncio.Task):
        await self._ping_task_owner.cleanup(ping_task, timeout=self._PING_TASK_CLEANUP_TIMEOUT)

    async def _cleanup_ping_generation(self, websocket_assistant: Optional[WSAssistant]):
        ping_task = self._ping_task_owner.task_for(websocket_assistant)
        if ping_task is not None:
            await self._cleanup_ping_task(ping_task)

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        try:
            return await await_primary_then_cleanup(
                primary=super()._on_order_stream_interruption(websocket_assistant),
                cleanup=lambda: self._cleanup_ping_generation(websocket_assistant),
                logger=self.logger(),
                cleanup_error_message="Error while cleaning up the Bitget ping task owner",
            )
        finally:
            self._ping_task_owner.end_generation(websocket_assistant)

    async def _subscribe_channels(self, ws: WSAssistant) -> None:
        try:
            subscription_topics: List[Dict[str, str]] = []

            for trading_pair in self._trading_pairs:
                symbol: str = await self._connector.exchange_symbol_associated_to_pair(
                    trading_pair
                )
                for channel in [CONSTANTS.PUBLIC_WS_BOOKS, CONSTANTS.PUBLIC_WS_TRADE]:
                    subscription_topics.append({
                        "instType": "SPOT",
                        "channel": channel,
                        "instId": symbol
                    })

            await ws.send(
                WSJSONRequest({
                    "op": "subscribe",
                    "args": subscription_topics,
                })
            )

            self.logger().info("Subscribed to public channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to public channels...")
            raise

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol: str = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        rest_assistant: RESTAssistant = await self._api_factory.get_rest_assistant()

        data: Dict[str, Any] = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.PUBLIC_ORDERBOOK_ENDPOINT),
            params={
                "symbol": symbol,
                "limit": "100",
            },
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.PUBLIC_ORDERBOOK_ENDPOINT,
        )

        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_data: Dict[str, Any] = snapshot_response["data"]
        update_id: int = int(snapshot_data["ts"])
        timestamp: float = update_id * 1e-3

        order_book_message_content: Dict[str, Any] = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot_data["bids"],
            "asks": snapshot_data["asks"],
        }

        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            timestamp
        )

    async def _send_ping(self, websocket_assistant: WSAssistant) -> None:
        ping_request = WSPlainTextRequest(CONSTANTS.PUBLIC_WS_PING_REQUEST)

        await websocket_assistant.send(ping_request)

    async def send_interval_ping(self, websocket_assistant: WSAssistant) -> None:
        """
        Coroutine to send PING messages periodically.

        :param websocket_assistant: The websocket assistant to use to send the PING message.
        """
        try:
            while True:
                await self._send_ping(websocket_assistant)
                await asyncio.sleep(CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        except asyncio.CancelledError:
            self.logger().info("Interval PING task cancelled")
            raise
        except Exception:
            self.logger().exception("Error sending interval PING")

    async def listen_for_subscriptions(self) -> NoReturn:
        while True:
            ws: Optional[WSAssistant] = None

            async def listen_on_websocket():
                nonlocal ws
                ws = await self._connected_websocket_assistant()
                self._ws_assistant = ws  # Store for dynamic subscriptions
                await self._subscribe_channels(ws)
                self._create_ping_task(ws)
                return await self._process_websocket_messages(websocket_assistant=ws)

            try:
                try:
                    await await_primary_then_cleanup(
                        primary=listen_on_websocket(),
                        cleanup=lambda: self._on_order_stream_interruption(websocket_assistant=ws),
                        logger=self.logger(),
                        cleanup_error_message="Error while interrupting the Bitget order book stream",
                    )
                finally:
                    if self._ws_assistant is ws:
                        self._ws_assistant = None
            except ConnectionError as connection_exception:
                self.logger().warning(
                    f"The websocket connection was closed ({connection_exception})"
                )
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. "
                    "Retrying in 5 seconds...",
                )
                await self._sleep(1.0)

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """
        Subscribes to order book and trade channels for a single trading pair
        on the existing WebSocket connection.

        :param trading_pair: the trading pair to subscribe to
        :return: True if subscription was successful, False otherwise
        """
        if self._ws_assistant is None:
            self.logger().warning(
                f"Cannot subscribe to {trading_pair}: WebSocket not connected"
            )
            return False

        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)

            subscription_topics = []
            for channel in [CONSTANTS.PUBLIC_WS_BOOKS, CONSTANTS.PUBLIC_WS_TRADE]:
                subscription_topics.append({
                    "instType": "SPOT",
                    "channel": channel,
                    "instId": symbol
                })

            await self._ws_assistant.send(
                WSJSONRequest({
                    "op": "subscribe",
                    "args": subscription_topics,
                })
            )

            self.add_trading_pair(trading_pair)
            self.logger().info(f"Subscribed to {trading_pair} order book and trade channels")
            return True

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error subscribing to {trading_pair}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """
        Unsubscribes from order book and trade channels for a single trading pair
        on the existing WebSocket connection.

        :param trading_pair: the trading pair to unsubscribe from
        :return: True if unsubscription was successful, False otherwise
        """
        if self._ws_assistant is None:
            self.logger().warning(
                f"Cannot unsubscribe from {trading_pair}: WebSocket not connected"
            )
            return False

        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)

            unsubscription_topics = []
            for channel in [CONSTANTS.PUBLIC_WS_BOOKS, CONSTANTS.PUBLIC_WS_TRADE]:
                unsubscription_topics.append({
                    "instType": "SPOT",
                    "channel": channel,
                    "instId": symbol
                })

            await self._ws_assistant.send(
                WSJSONRequest({
                    "op": "unsubscribe",
                    "args": unsubscription_topics,
                })
            )

            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Unsubscribed from {trading_pair} order book and trade channels")
            return True

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error unsubscribing from {trading_pair}")
            return False

    @classmethod
    def _get_next_subscribe_id(cls) -> int:
        """Returns the next subscription ID and increments the counter."""
        current_id = cls._next_subscribe_id
        cls._next_subscribe_id += 1
        return current_id
