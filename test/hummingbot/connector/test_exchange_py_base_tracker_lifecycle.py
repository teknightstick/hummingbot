from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange_py_base import ExchangePyBase


class ExchangePyBaseTrackerLifecycleTests(IsolatedAsyncioWrapperTestCase):

    async def test_stop_network_awaits_order_book_tracker_drain(self):
        order_book_tracker = MagicMock()
        order_book_tracker.stop_and_wait = AsyncMock(return_value=True)
        connector = SimpleNamespace(
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
        )

        await ExchangePyBase.stop_network(connector)

        order_book_tracker.stop_and_wait.assert_awaited_once_with()
