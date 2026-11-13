import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses.core import aioresponses
from bidict import bidict

import hummingbot.connector.derivative.bitmart_perpetual.bitmart_perpetual_constants as CONSTANTS
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.bitmart_perpetual import bitmart_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.bitmart_perpetual.bitmart_perpetual_api_order_book_data_source import (
    BitmartPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.bitmart_perpetual.bitmart_perpetual_derivative import BitmartPerpetualDerivative
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class _DeterministicClosingConnection:
    _MAX_MSG_SIZE = 4 * 1024 * 1024

    def __init__(self):
        self.connected = False
        self.closing = False
        self.receive_started = asyncio.Event()
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()
        self.sent_payloads = []

    async def connect(self, **kwargs):
        self.connected = True
        self.closing = False

    async def disconnect(self):
        self.closing = True
        self.close_started.set()
        await self.close_release.wait()
        self.connected = False
        self.closing = False

    async def send(self, request):
        if self.closing:
            raise ConnectionError("WebSocket close is pending")
        await request.send_with_connection(self)

    async def receive(self):
        self.receive_started.set()
        await asyncio.Future()

    async def _send_json(self, payload):
        self.sent_payloads.append(payload)


class BitmartPerpetualAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}{cls.quote_asset}"
        cls.domain = "bitmart_perpetual"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.listening_task = None
        self.async_tasks: List[asyncio.Task] = []

        self.time_synchronizer = TimeSynchronizer()
        self.time_synchronizer.add_time_offset_ms_sample(0)
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = BitmartPerpetualDerivative(
            client_config_map,
            bitmart_perpetual_api_key="",
            bitmart_perpetual_api_secret="",
            bitmart_perpetual_memo="",
            trading_pairs=[self.trading_pair],
            trading_required=False,
            domain=self.domain,
        )
        self.data_source = BitmartPerpetualAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            domain=self.domain,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        BitmartPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {
            self.domain: bidict({self.ex_trading_pair: self.trading_pair})
        }

        self.connector._set_trading_pair_symbol_map(
            bidict({f"{self.base_asset}{self.quote_asset}": self.trading_pair}))

    async def asyncSetUp(self) -> None:
        self.mocking_assistant = NetworkMockingAssistant()
        self.resume_test_event = asyncio.Event()

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        for task in self.async_tasks:
            task.cancel()
        BitmartPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def resume_test_callback(self, *_, **__):
        self.resume_test_event.set()
        return None

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def _raise_exception(self, exception_class):
        raise exception_class

    def _raise_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def _order_book_snapshot_rest_data(self):
        resp = {
            "code": 1000,
            "message": "Ok",
            "trace": "b9bff62d-9ac8-4815-8808-8f745673c096",
            "data": {
                "asks": [["23935.4", "65", "65"]],
                "bids": [["23935.4", "65", "65"]],
                "timestamp": 1660285421287,
                "symbol": self.ex_trading_pair
            }
        }
        return resp

    def _funding_info_rest_data(self):
        resp = {
            "code": 1000,
            "message": "Ok",
            "data": {
                "timestamp": 1662518172178,
                "symbol": self.ex_trading_pair,
                "rate_value": "0.000164",
                "expected_rate": "0.000164",
                "funding_time": 1709971200000,
                "funding_upper_limit": "0.0375",
                "funding_lower_limit": "-0.0375"
            },
            "trace": "13f7fda9-9543-4e11-a0ba-cbe117989988"
        }
        return resp

    def _exchange_info_rest_data(self):
        resp = {
            "code": 1000,
            "message": "Ok",
            "trace": "9b92a999-9463-4c96-91a4-93ad1cad0d72",
            "data": {
                "symbols": [
                    {
                        "symbol": self.ex_trading_pair,
                        "product_type": 1,
                        "open_timestamp": 1594080000123,
                        "expire_timestamp": 0,
                        "settle_timestamp": 0,
                        "base_currency": "BTC",
                        "quote_currency": "USDT",
                        "last_price": "23920",
                        "volume_24h": "18969368",
                        "turnover_24h": "458933659.7858",
                        "index_price": "23945.25191635",
                        "index_name": self.ex_trading_pair,
                        "contract_size": "0.001",
                        "min_leverage": "1",
                        "max_leverage": "100",
                        "price_precision": "0.1",
                        "vol_precision": "1",
                        "max_volume": "500000",
                        "market_max_volume": "500000",
                        "min_volume": "1",
                        "funding_rate": "0.0001",
                        "expected_funding_rate": "0.00011",
                        "open_interest": "4134180870",
                        "open_interest_value": "94100888927.0433258",
                        "high_24h": "23900",
                        "low_24h": "23100",
                        "change_24h": "0.004",
                        "funding_interval_hours": 8
                    },
                ]
            }
        }
        return resp

    def _exchange_info_with_non_initialized_trading_pair_rest_data(self):
        resp = {
            "code": 1000,
            "message": "Ok",
            "trace": "9b92a999-9463-4c96-91a4-93ad1cad0d72",
            "data": {
                "symbols": [
                    {
                        "symbol": "NEWSYMBOLUSDT",
                        "product_type": 1,
                        "open_timestamp": 1594080000123,
                        "expire_timestamp": 0,
                        "settle_timestamp": 0,
                        "base_currency": "NEWSYMBOL",
                        "quote_currency": "USDT",
                        "last_price": "23920",
                        "volume_24h": "18969368",
                        "turnover_24h": "458933659.7858",
                        "index_price": "23945.25191635",
                        "index_name": self.ex_trading_pair,
                        "contract_size": "0.001",
                        "min_leverage": "1",
                        "max_leverage": "100",
                        "price_precision": "0.1",
                        "vol_precision": "1",
                        "max_volume": "500000",
                        "market_max_volume": "500000",
                        "min_volume": "1",
                        "funding_rate": "0.0001",
                        "expected_funding_rate": "0.00011",
                        "open_interest": "4134180870",
                        "open_interest_value": "94100888927.0433258",
                        "high_24h": "23900",
                        "low_24h": "23100",
                        "change_24h": "0.004",
                        "funding_interval_hours": 8
                    },
                ]
            }
        }
        return resp

    def _orderbook_update_event(self, update_type: str = "update"):
        resp = {
            "data": {
                "symbol": self.ex_trading_pair,
                "asks": [
                    {
                        "price": "70391.6",
                        "vol": "3550"
                    }
                ],
                "bids": [
                    {
                        "price": "70391.2",
                        "vol": "1335"
                    }
                ],
                "ms_t": 1730400086184,
                "version": 980361,
                "type": update_type
            },
            "group": "futures/depthIncrease50:BTCUSDT@200ms"
        }
        return resp

    def _trade_event(self):
        resp = {
            "group": f"futures/trade:{self.ex_trading_pair}",
            "data": [
                {
                    "trade_id": 1409495322,
                    "symbol": self.ex_trading_pair,
                    "deal_price": "117387.58",
                    "way": 1,
                    "deal_vol": "1445",
                    "created_at": "2023-02-24T07:54:11.124940968Z"
                }
            ]
        }

        return resp

    def _funding_info_event(self):
        resp = {
            "data": {
                "symbol": self.ex_trading_pair,
                "fundingRate": "0.000098800809",
                "fundingTime": 1732525864000,
                "nextFundingRate": "0.0000947",
                "nextFundingTime": 1732550400000,
                "funding_upper_limit": "0.0375",
                "funding_lower_limit": "-0.0375",
                "ts": 1732525864601
            },
            "group": "futures/fundingRate:BTCUSDT"
        }
        return resp

    def _ticker_event(self):
        resp = {
            "group": "futures/ticker",
            "data": {
                "symbol": self.ex_trading_pair,
                "volume_24": "117387.58",
                "mark_price": "146.24",
                "last_price": "146.24",
                "index_price": "146.28",
                "range": "147.17",
                "ask_price": "147.11",
                "ask_vol": "1",
                "bid_price": "142.11",
                "bid_vol": "1"
            }
        }
        return resp

    @aioresponses()
    async def test_get_snapshot_exception_raised(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.SNAPSHOT_REST_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=400, body=json.dumps(["ERROR"]))

        with self.assertRaises(IOError) as context:
            await self.data_source._order_book_snapshot(trading_pair=self.trading_pair)

        self.assertIn("HTTP status is 400. Error: [\"ERROR\"]",
                      str(context.exception))

    @aioresponses()
    async def test_get_snapshot_successful(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.SNAPSHOT_REST_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_response = self._order_book_snapshot_rest_data()
        mock_api.get(regex_url, status=200, body=json.dumps(mock_response))

        result: Dict[str, Any] = await self.data_source._request_order_book_snapshot(trading_pair=self.trading_pair)
        self.assertEqual(mock_response, result)

    @aioresponses()
    async def test_get_new_order_book(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.SNAPSHOT_REST_URL, domain=self.domain)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=200, body=json.dumps(self._order_book_snapshot_rest_data()))
        result = await self.data_source.get_new_order_book(trading_pair=self.trading_pair)
        self.assertIsInstance(result, OrderBook)
        self.assertEqual(1, result.snapshot_uid)

    @aioresponses()
    async def test_get_funding_info_from_exchange_successful(self, mock_api):
        funding_url = web_utils.public_rest_url(CONSTANTS.FUNDING_INFO_URL, self.domain)
        funding_regex_url = re.compile(f"^{funding_url}".replace(".", r"\.").replace("?", r"\?"))
        funding_info_resp = self._funding_info_rest_data()
        mock_api.get(funding_regex_url, body=json.dumps(funding_info_resp))

        exchange_info_url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL, self.domain)
        exchange_info_regex_url = re.compile(f"^{exchange_info_url}".replace(".", r"\.").replace("?", r"\?"))
        exchange_info_resp = self._exchange_info_rest_data()
        mock_api.get(exchange_info_regex_url, body=json.dumps(exchange_info_resp))

        funding_info: FundingInfo = await self.data_source.get_funding_info(self.trading_pair)

        self.assertEqual(self.trading_pair, funding_info.trading_pair)
        self.assertEqual(Decimal(exchange_info_resp["data"]["symbols"][0]["index_price"]), funding_info.index_price)
        self.assertEqual(Decimal(exchange_info_resp["data"]["symbols"][0]["last_price"]), funding_info.mark_price)
        self.assertEqual(int(float(funding_info_resp["data"]["funding_time"]) * 1e-3), funding_info.next_funding_utc_timestamp)
        self.assertEqual(Decimal(funding_info_resp["data"]["expected_rate"]), funding_info.rate)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    async def test_listen_for_subscriptions_cancelled_when_connecting(self, _, mock_ws):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()
        self.assertEqual(msg_queue.qsize(), 0)

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        sleep_mock.side_effect = asyncio.CancelledError
        mock_ws.side_effect = Exception("TEST ERROR.")

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...")
        )

    async def test_subscribe_to_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_to_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error occurred subscribing to order book trading and delta streams...")
        )

    async def test_channel_originating_message_returns_correct(self):
        event_type = self._orderbook_update_event(update_type="snapshot")
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._snapshot_messages_queue_key, event_message)

        event_type = self._orderbook_update_event(update_type="update")
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._diff_messages_queue_key, event_message)

        event_type = self._funding_info_event()
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._funding_info_messages_queue_key, event_message)

        event_type = self._trade_event()
        event_message = self.data_source._channel_originating_message(event_type)
        self.assertEqual(self.data_source._trade_messages_queue_key, event_message)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_successful(self, mock_ws):
        msg_queue_diffs: asyncio.Queue = asyncio.Queue()
        msg_queue_snapshots: asyncio.Queue = asyncio.Queue()
        msg_queue_trades: asyncio.Queue = asyncio.Queue()
        msg_queue_funding: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.close.return_value = None

        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value, json.dumps(self._orderbook_update_event(update_type="snapshot"))
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value, json.dumps(self._orderbook_update_event(update_type="update"))
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value, json.dumps(self._trade_event())
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value, json.dumps(self._funding_info_event())
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            mock_ws.return_value, json.dumps(self._exchange_info_rest_data())
        )

        self.listening_task = self.local_event_loop.create_task(self.data_source.listen_for_subscriptions())
        self.listening_task_diffs = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue_diffs)
        )
        self.listening_task_snapshots = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue_snapshots)
        )
        self.listening_task_trades = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue_trades)
        )
        self.listening_task_funding_info = self.local_event_loop.create_task(
            self.data_source.listen_for_funding_info(msg_queue_funding)
        )
        self.listening_task_exchange_info = self.local_event_loop.create_task(
            self.data_source.listen_for_exchange_info()
        )

        result: OrderBookMessage = await msg_queue_diffs.get()
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.DIFF, result.type)
        self.assertTrue(result.has_update_id)
        self.assertEqual(result.update_id, 980361)
        self.assertEqual(self.trading_pair, result.content["trading_pair"])
        self.assertEqual(1, len(result.content["bids"]))
        self.assertEqual(1, len(result.content["asks"]))

        result: OrderBookMessage = await msg_queue_snapshots.get()
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, result.type)
        self.assertTrue(result.has_update_id)
        self.assertEqual(result.update_id, 980361)
        self.assertEqual(self.trading_pair, result.content["trading_pair"])
        self.assertEqual(1, len(result.content["bids"]))
        self.assertEqual(1, len(result.content["asks"]))

        result: OrderBookMessage = await msg_queue_trades.get()
        self.assertIsInstance(result, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.TRADE, result.type)
        self.assertTrue(result.has_trade_id)
        self.assertEqual(result.trade_id, 1409495322)
        self.assertEqual(self.trading_pair, result.content["trading_pair"])

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

    async def test_parse_exchange_info_message_receives_not_initialized_trading_pair(self):
        mock_msg = self._exchange_info_with_non_initialized_trading_pair_rest_data()
        await self.data_source._parse_exchange_info_message(mock_msg)
        self.assertIsNone(self.data_source._last_mark_prices.get("NEWSYMBOL"))
        self.assertIsNone(self.data_source._last_index_prices.get("NEWSYMBOL"))

    async def test_parse_exchange_info_message_success(self):
        mock_msg = self._ticker_event()
        await self.data_source._parse_exchange_info_message(mock_msg)
        self.assertIn(self.trading_pair, self.data_source._last_mark_prices.keys())
        self.assertEqual(Decimal("146.24"), self.data_source._last_mark_prices[self.trading_pair])
        self.assertIn(self.trading_pair, self.data_source._last_index_prices.keys())
        self.assertEqual(Decimal("146.28"), self.data_source._last_index_prices[self.trading_pair])

    async def test_persistent_exchange_info_listener_follows_tracker_generation_restart(self):
        processed = asyncio.Event()
        original_parser = self.data_source._parse_exchange_info_message

        async def parse_and_signal(raw_message):
            await original_parser(raw_message)
            processed.set()

        listener_task = self.data_source._exchange_info_listener_task
        await asyncio.sleep(0)
        self.assertFalse(listener_task.done())

        with patch.object(
            self.data_source,
            "_parse_exchange_info_message",
            side_effect=parse_and_signal,
        ):
            self.data_source.activate_raw_message_generation(1)
            cached_queue = self.data_source._message_queue[self.data_source._tickers_messages_queue_key]
            self.data_source.deactivate_raw_message_generation(1)
            self.data_source.activate_raw_message_generation(2)
            current_queue = self.data_source._message_queue[self.data_source._tickers_messages_queue_key]

            self.assertIs(cached_queue, current_queue)
            current_queue.put_nowait(self._ticker_event())
            await asyncio.wait_for(processed.wait(), timeout=0.2)

        self.assertIs(listener_task, self.data_source._exchange_info_listener_task)
        self.assertFalse(listener_task.done())
        self.assertEqual(Decimal("146.24"), self.data_source._last_mark_prices[self.trading_pair])
        self.assertEqual(Decimal("146.28"), self.data_source._last_index_prices[self.trading_pair])
        listener_task.cancel()
        await asyncio.gather(listener_task, return_exceptions=True)

    # Dynamic subscription tests for subscribe_to_trading_pair and unsubscribe_from_trading_pair

    async def test_subscribe_to_trading_pair_successful(self):
        """Test successful subscription to a new trading pair."""
        new_pair = "ETH-USDT"
        ex_new_pair = "ETHUSDT"

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, ex_new_pair: new_pair})
        )

        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertTrue(result)
        self.assertIn(new_pair, self.data_source._trading_pairs)
        self.assertTrue(
            self._is_logged("INFO", f"Successfully subscribed to {new_pair}")
        )

    async def test_subscribe_to_trading_pair_websocket_not_connected(self):
        """Test subscription fails when WebSocket is not connected."""
        new_pair = "ETH-USDT"
        self.data_source._ws_assistant = None

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", f"Cannot subscribe to {new_pair}: WebSocket connection not established.")
        )

    async def test_subscribe_to_trading_pair_raises_cancel_exception(self):
        """Test that CancelledError is properly raised during subscription."""
        new_pair = "ETH-USDT"
        ex_new_pair = "ETHUSDT"

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, ex_new_pair: new_pair})
        )

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.subscribe_to_trading_pair(new_pair)

    async def test_subscribe_to_trading_pair_raises_exception_and_logs_error(self):
        """Test that exceptions during subscription are logged and return False."""
        new_pair = "ETH-USDT"
        ex_new_pair = "ETHUSDT"

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, ex_new_pair: new_pair})
        )

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertFalse(result)

    async def test_unsubscribe_from_trading_pair_successful(self):
        """Test successful unsubscription from a trading pair."""
        self.assertIn(self.trading_pair, self.data_source._trading_pairs)

        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertTrue(result)
        self.assertNotIn(self.trading_pair, self.data_source._trading_pairs)
        self.assertTrue(
            self._is_logged("INFO", f"Successfully unsubscribed from {self.trading_pair}")
        )

    async def test_unsubscribe_from_trading_pair_websocket_not_connected(self):
        """A disconnected WebSocket has no remote subscription left to remove."""
        self.data_source._ws_assistant = None

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertTrue(result)
        self.assertNotIn(self.trading_pair, self.data_source._trading_pairs)
        self.assertTrue(
            self._is_logged(
                "INFO",
                f"WebSocket session is closed; cleared local subscription for {self.trading_pair}",
            )
        )

    async def test_dynamic_subscription_disconnect_stop_retry_and_restart(self):
        dynamic_pair = "ETH-USDT"
        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, "ETHUSDT": dynamic_pair})
        )
        self.data_source.get_new_order_book = AsyncMock(return_value=OrderBook())
        connection = _DeterministicClosingConnection()
        assistant = WSAssistant(connection=connection)
        self.data_source._api_factory.get_ws_assistant = AsyncMock(return_value=assistant)
        tracker = OrderBookTracker(
            data_source=self.data_source,
            trading_pairs=[self.trading_pair],
            domain=self.domain,
        )
        tracker._sleep = AsyncMock()
        tracker.start()

        try:
            await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
            await asyncio.wait_for(connection.receive_started.wait(), timeout=0.2)
            self.assertTrue(await tracker.add_trading_pair(dynamic_pair))
            self.assertIn(dynamic_pair, self.data_source._trading_pairs)
            sends_before_stop = len(connection.sent_payloads)

            stop_task = asyncio.create_task(tracker.stop_and_wait(timeout=0.02))
            await asyncio.wait_for(connection.close_started.wait(), timeout=0.2)
            self.assertFalse(await stop_task)
            self.assertTrue(connection.connected)
            self.assertIs(assistant, self.data_source._ws_assistant)
            self.assertIn(dynamic_pair, tracker._dynamic_subscriptions)
            self.assertIn(dynamic_pair, tracker.residual_subscriptions)
            self.assertIn(dynamic_pair, tracker._trading_pairs)
            self.assertIn(dynamic_pair, self.data_source._trading_pairs)

            connection.close_release.set()
            for _ in range(10):
                if self.data_source._ws_assistant is None:
                    break
                await asyncio.sleep(0)
            self.assertIsNone(self.data_source._ws_assistant)
            self.assertTrue(await tracker.stop_and_wait(timeout=0.2))
            self.assertEqual(sends_before_stop, len(connection.sent_payloads))
            self.assertNotIn(dynamic_pair, tracker.residual_subscriptions)
            self.assertNotIn(dynamic_pair, tracker._trading_pairs)
            self.assertNotIn(dynamic_pair, self.data_source._trading_pairs)

            tracker.start()
            await asyncio.wait_for(tracker.wait_ready(), timeout=0.2)
            self.assertNotIn(dynamic_pair, tracker._trading_pairs)
        finally:
            connection.close_release.set()
            await tracker.stop_and_wait(timeout=0.2)

    async def test_unsubscribe_from_trading_pair_raises_cancel_exception(self):
        """Test that CancelledError is properly raised during unsubscription."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

    async def test_unsubscribe_from_trading_pair_raises_exception_and_logs_error(self):
        """Test that exceptions during unsubscription are logged and return False."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertFalse(result)
