#!/usr/bin/env python
"""
Tests for OrderBookTrackerDataSource base class.

This module tests:
- add_trading_pair: Adds a trading pair to the internal list
- remove_trading_pair: Removes a trading pair from the internal list
"""
import asyncio
import unittest
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSResponse
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


_END_OF_STREAM = object()


class _ControllableConnection:
    def __init__(
        self,
        disconnect_release: Optional[asyncio.Event] = None,
        disconnect_error: Optional[Exception] = None,
    ):
        self.connected = True
        self.disconnect_calls = 0
        self.disconnect_started = asyncio.Event()
        self.disconnect_release = disconnect_release
        self.disconnect_error = disconnect_error
        self.incoming_messages = asyncio.Queue()

    async def disconnect(self):
        self.disconnect_calls += 1
        self.disconnect_started.set()
        if self.disconnect_release is not None:
            await self.disconnect_release.wait()
        if self.disconnect_error is not None:
            raise self.disconnect_error
        self.connected = False

    async def receive(self):
        message = await self.incoming_messages.get()
        if message is _END_OF_STREAM:
            self.connected = False
            return None
        return WSResponse(message)


class ListenerTestOrderBookTrackerDataSource(OrderBookTrackerDataSource):
    """Concrete base data source with deterministic WebSocket attempts."""

    def __init__(self):
        super().__init__(["BTC-USDT"])
        self.assistants = asyncio.Queue()
        self.connected_assistants = []
        self.subscribed_assistants = asyncio.Queue()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return {pair: 100.0 for pair in trading_pairs}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        assistant = await self.assistants.get()
        self.connected_assistants.append(assistant)
        return assistant

    async def _subscribe_channels(self, ws: WSAssistant):
        self.subscribed_assistants.put_nowait(ws)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return event_message["channel"]

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True


class MockOrderBookTrackerDataSource(OrderBookTrackerDataSource):
    """Concrete implementation of OrderBookTrackerDataSource for testing."""

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return {pair: 100.0 for pair in trading_pairs}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError

    async def _subscribe_channels(self, ws: WSAssistant):
        raise NotImplementedError

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True


class OrderBookTrackerDataSourceTests(unittest.TestCase):
    """Tests for the OrderBookTrackerDataSource base class methods."""

    def setUp(self):
        self.trading_pairs = ["BTC-USDT", "ETH-USDT"]
        self.data_source = MockOrderBookTrackerDataSource(trading_pairs=self.trading_pairs.copy())

    def test_initial_trading_pairs(self):
        """Test that trading pairs are correctly initialized."""
        self.assertEqual(self.trading_pairs, self.data_source._trading_pairs)

    def test_add_trading_pair_new_pair(self):
        """Test adding a new trading pair."""
        new_pair = "SOL-USDT"

        self.assertNotIn(new_pair, self.data_source._trading_pairs)

        self.data_source.add_trading_pair(new_pair)

        self.assertIn(new_pair, self.data_source._trading_pairs)
        self.assertEqual(3, len(self.data_source._trading_pairs))

    def test_add_trading_pair_existing_pair(self):
        """Test that adding an existing pair doesn't create duplicates."""
        existing_pair = "BTC-USDT"

        self.assertIn(existing_pair, self.data_source._trading_pairs)
        initial_count = len(self.data_source._trading_pairs)

        self.data_source.add_trading_pair(existing_pair)

        # Should not create duplicate
        self.assertEqual(initial_count, len(self.data_source._trading_pairs))

    def test_remove_trading_pair_existing_pair(self):
        """Test removing an existing trading pair."""
        pair_to_remove = "BTC-USDT"

        self.assertIn(pair_to_remove, self.data_source._trading_pairs)

        self.data_source.remove_trading_pair(pair_to_remove)

        self.assertNotIn(pair_to_remove, self.data_source._trading_pairs)
        self.assertEqual(1, len(self.data_source._trading_pairs))

    def test_remove_trading_pair_nonexistent_pair(self):
        """Test that removing a nonexistent pair doesn't raise an error."""
        nonexistent_pair = "NONEXISTENT-PAIR"

        self.assertNotIn(nonexistent_pair, self.data_source._trading_pairs)
        initial_count = len(self.data_source._trading_pairs)

        # Should not raise
        self.data_source.remove_trading_pair(nonexistent_pair)

        # Count should remain the same
        self.assertEqual(initial_count, len(self.data_source._trading_pairs))

    def test_add_and_remove_trading_pair_sequence(self):
        """Test adding and removing trading pairs in sequence."""
        new_pair = "SOL-USDT"

        # Add new pair
        self.data_source.add_trading_pair(new_pair)
        self.assertIn(new_pair, self.data_source._trading_pairs)

        # Remove it
        self.data_source.remove_trading_pair(new_pair)
        self.assertNotIn(new_pair, self.data_source._trading_pairs)

        # Remove an original pair
        self.data_source.remove_trading_pair("BTC-USDT")
        self.assertNotIn("BTC-USDT", self.data_source._trading_pairs)

        # Only ETH-USDT should remain
        self.assertEqual(["ETH-USDT"], self.data_source._trading_pairs)

    def test_ws_assistant_initialization(self):
        """Test that ws_assistant is initially None."""
        self.assertIsNone(self.data_source._ws_assistant)

    def test_order_book_create_function(self):
        """Test that order_book_create_function returns OrderBook by default."""
        order_book = self.data_source.order_book_create_function()
        self.assertIsInstance(order_book, OrderBook)

    def test_order_book_create_function_setter(self):
        """Test setting a custom order_book_create_function."""
        class CustomOrderBook(OrderBook):
            pass

        self.data_source.order_book_create_function = lambda: CustomOrderBook()

        order_book = self.data_source.order_book_create_function()
        self.assertIsInstance(order_book, CustomOrderBook)


class OrderBookTrackerDataSourceListenerTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.data_source = ListenerTestOrderBookTrackerDataSource()

    async def _start_listener(self, assistant: WSAssistant) -> asyncio.Task:
        self.data_source.assistants.put_nowait(assistant)
        listener_task = asyncio.create_task(self.data_source.listen_for_subscriptions())
        subscribed_assistant = await asyncio.wait_for(
            self.data_source.subscribed_assistants.get(),
            timeout=0.2,
        )
        self.assertIs(assistant, subscribed_assistant)
        self.assertIs(assistant, self.data_source._ws_assistant)
        return listener_task

    async def _cancel_listener(self, listener_task: asyncio.Task):
        if not listener_task.done():
            listener_task.cancel("test cleanup")
        await asyncio.gather(listener_task, return_exceptions=True)

    async def test_listener_clears_exact_assistant_after_disconnect_returns(self):
        connection = _ControllableConnection()
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)

        listener_task.cancel("listener cancelled")
        with self.assertRaises(asyncio.CancelledError) as cancellation_context:
            await listener_task

        self.assertEqual(("listener cancelled",), cancellation_context.exception.args)
        self.assertEqual(1, connection.disconnect_calls)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_listener_clears_exact_assistant_when_disconnect_raises(self):
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        with self.assertRaisesRegex(RuntimeError, "disconnect failed"):
            await listener_task

        self.assertEqual(1, connection.disconnect_calls)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_tracker_data_source_loop_sanitizes_disconnect_error_after_clear(self):
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        self.data_source.assistants.put_nowait(assistant)
        tracker = OrderBookTracker(self.data_source, ["BTC-USDT"])
        listener_task = tracker._schedule_data_source_task(0, "listen_for_subscriptions")
        subscribed_assistant = await asyncio.wait_for(
            self.data_source.subscribed_assistants.get(),
            timeout=0.2,
        )
        self.assertIs(assistant, subscribed_assistant)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        with self.assertLogs(OrderBookTracker.logger(), level="ERROR") as log_context:
            result = await listener_task

        self.assertIsNone(result)
        self.assertTrue(any("disconnect failed" in record for record in log_context.output))
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_listener_keeps_exact_assistant_until_blocked_disconnect_returns(self):
        disconnect_release = asyncio.Event()
        connection = _ControllableConnection(disconnect_release=disconnect_release)
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        await asyncio.wait_for(connection.disconnect_started.wait(), timeout=0.2)
        self.assertFalse(listener_task.done())
        self.assertIs(assistant, self.data_source._ws_assistant)

        disconnect_release.set()
        for _ in range(10):
            if self.data_source._ws_assistant is None:
                break
            await asyncio.sleep(0)
        self.assertIsNone(self.data_source._ws_assistant)
        await self._cancel_listener(listener_task)

    async def test_listener_clears_exact_assistant_when_disconnect_is_cancelled(self):
        disconnect_release = asyncio.Event()
        connection = _ControllableConnection(disconnect_release=disconnect_release)
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        await asyncio.wait_for(connection.disconnect_started.wait(), timeout=0.2)
        self.assertIs(assistant, self.data_source._ws_assistant)
        listener_task.cancel("disconnect cancelled")
        with self.assertRaises(asyncio.CancelledError) as cancellation_context:
            await listener_task

        self.assertEqual(("disconnect cancelled",), cancellation_context.exception.args)
        self.assertEqual(1, connection.disconnect_calls)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_old_listener_finishing_cannot_clear_replacement_assistant(self):
        disconnect_release = asyncio.Event()
        old_connection = _ControllableConnection(disconnect_release=disconnect_release)
        old_assistant = WSAssistant(old_connection)
        old_listener_task = await self._start_listener(old_assistant)
        old_connection.incoming_messages.put_nowait(_END_OF_STREAM)
        await asyncio.wait_for(old_connection.disconnect_started.wait(), timeout=0.2)

        replacement_connection = _ControllableConnection()
        replacement_assistant = WSAssistant(replacement_connection)
        replacement_listener_task = await self._start_listener(replacement_assistant)

        disconnect_release.set()
        await asyncio.sleep(0)
        self.assertIs(replacement_assistant, self.data_source._ws_assistant)

        await self._cancel_listener(old_listener_task)
        self.assertIs(replacement_assistant, self.data_source._ws_assistant)
        await self._cancel_listener(replacement_listener_task)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_old_listener_raising_cannot_clear_replacement_assistant(self):
        disconnect_release = asyncio.Event()
        old_connection = _ControllableConnection(
            disconnect_release=disconnect_release,
            disconnect_error=RuntimeError("old disconnect failed"),
        )
        old_assistant = WSAssistant(old_connection)
        old_listener_task = await self._start_listener(old_assistant)
        old_connection.incoming_messages.put_nowait(_END_OF_STREAM)
        await asyncio.wait_for(old_connection.disconnect_started.wait(), timeout=0.2)

        replacement_connection = _ControllableConnection()
        replacement_assistant = WSAssistant(replacement_connection)
        replacement_listener_task = await self._start_listener(replacement_assistant)

        disconnect_release.set()
        with self.assertRaisesRegex(RuntimeError, "old disconnect failed"):
            await old_listener_task
        self.assertIs(replacement_assistant, self.data_source._ws_assistant)

        await self._cancel_listener(replacement_listener_task)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_reconnect_uses_fresh_assistant_and_ignores_stale_frames(self):
        old_connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        old_assistant = WSAssistant(old_connection)
        old_listener_task = await self._start_listener(old_assistant)
        old_connection.incoming_messages.put_nowait(_END_OF_STREAM)

        with self.assertRaisesRegex(RuntimeError, "disconnect failed"):
            await old_listener_task
        self.assertIsNone(self.data_source._ws_assistant)

        stale_frame = {"channel": self.data_source._trade_messages_queue_key, "session": "stale"}
        old_connection.incoming_messages.put_nowait(stale_frame)
        fresh_connection = _ControllableConnection()
        fresh_assistant = WSAssistant(fresh_connection)
        fresh_listener_task = await self._start_listener(fresh_assistant)
        fresh_frame = {"channel": self.data_source._trade_messages_queue_key, "session": "fresh"}
        fresh_connection.incoming_messages.put_nowait(fresh_frame)

        message_queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        received_frame = await asyncio.wait_for(message_queue.get(), timeout=0.2)
        self.assertEqual(fresh_frame, received_frame)
        self.assertTrue(message_queue.empty())
        self.assertEqual([old_assistant, fresh_assistant], self.data_source.connected_assistants)

        await self._cancel_listener(fresh_listener_task)
        self.assertIsNone(self.data_source._ws_assistant)
