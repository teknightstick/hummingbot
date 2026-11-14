import asyncio
from test.hummingbot.data_feed.candles_feed.test_candles_base import TestCandlesBase
from unittest.mock import AsyncMock, patch

from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.data_feed.candles_feed.bitget_perpetual_candles import BitgetPerpetualCandles


class TestBitgetPerpetualCandles(TestCandlesBase):
    __test__ = True
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "ETH"
        cls.quote_asset = "USDT"
        cls.interval = "1m"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.max_records = 150

    def setUp(self) -> None:
        super().setUp()
        self.data_feed = BitgetPerpetualCandles(trading_pair=self.trading_pair, interval=self.interval)

        self.log_records = []
        self.data_feed.logger().setLevel(1)
        self.data_feed.logger().addHandler(self)
        self.resume_test_event = asyncio.Event()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.mocking_assistant = NetworkMockingAssistant()

    async def test_listener_waits_for_residual_fill_release_before_connecting(self):
        generation = object()
        started = asyncio.Event()
        cancellation_seen = asyncio.Event()
        release = asyncio.Event()

        async def cancellation_suppressing_fill():
            started.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancellation_seen.set()

        fill_task = asyncio.create_task(cancellation_suppressing_fill())
        self.data_feed._start_candle_generation(generation)
        self.data_feed._create_fill_candles_task(generation, lambda: fill_task)
        await started.wait()

        with patch.object(type(self.data_feed), "_FILL_CANDLES_TASK_CLEANUP_TIMEOUT", 0.01):
            await self.data_feed._on_candle_generation_interruption(generation)
        await cancellation_seen.wait()

        websocket_assistant = AsyncMock()
        connect_mock = AsyncMock(return_value=websocket_assistant)

        async def process_messages(_):
            await asyncio.Future()

        listener_task = None
        try:
            with patch.object(
                self.data_feed,
                "_connected_websocket_assistant",
                new=connect_mock,
            ), patch.object(
                self.data_feed,
                "_subscribe_channels",
                new=AsyncMock(),
            ), patch.object(
                self.data_feed,
                "_process_websocket_messages",
                side_effect=process_messages,
            ):
                listener_task = asyncio.create_task(self.data_feed.listen_for_subscriptions())
                await asyncio.sleep(0)
                connect_mock.assert_not_awaited()

                release.set()
                await fill_task
                for _ in range(10):
                    if connect_mock.await_count:
                        break
                    await asyncio.sleep(0)
                connect_mock.assert_awaited_once()
        finally:
            release.set()
            if listener_task is not None:
                listener_task.cancel()
                await asyncio.gather(listener_task, return_exceptions=True)
            await asyncio.gather(fill_task, return_exceptions=True)

    @staticmethod
    def get_candles_rest_data_mock():
        """
        Returns a mock response from the exchange REST API endpoint. At least it must contain four candles.
        """
        return {
            "code": "00000",
            "msg": "success",
            "requestTime": 1695800278693,
            "data": [
                [
                    "1758798420000",
                    "111694.49",
                    "111694.49",
                    "111588.85",
                    "111599.04",
                    "3.16054396428",
                    "352892.715820802517",
                    "352892.715820802517"
                ],
                [
                    "1758798480000",
                    "111599.04",
                    "111608.15",
                    "111595.02",
                    "111595.03",
                    "4.59290268736",
                    "512570.995025016836",
                    "512570.995025016836"
                ],
                [
                    "1758798540000",
                    "111595.03",
                    "111595.04",
                    "111521.01",
                    "111529.52",
                    "14.06507470738",
                    "1568969.70849836405",
                    "1568969.70849836405"
                ],
                [
                    "1758798600000",
                    "111529.52",
                    "111569.85",
                    "111529.52",
                    "111548.38",
                    "6.67627652466",
                    "744806.49272433786",
                    "744806.49272433786"
                ]
            ]
        }

    def get_fetch_candles_data_mock(self):
        return [
            [
                1758798420, "111694.49", "111694.49", "111588.85", "111599.04",
                "3.16054396428", "352892.715820802517", 0., 0., 0.
            ],
            [
                1758798480, "111599.04", "111608.15", "111595.02", "111595.03",
                "4.59290268736", "512570.995025016836", 0., 0., 0.
            ],
            [
                1758798540, "111595.03", "111595.04", "111521.01", "111529.52",
                "14.06507470738", "1568969.70849836405", 0., 0., 0.
            ],
            [
                1758798600, "111529.52", "111569.85", "111529.52", "111548.38",
                "6.67627652466", "744806.49272433786", 0., 0., 0.
            ]
        ]

    @staticmethod
    def get_candles_ws_data_mock_1():
        return {
            "action": "update",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": "ETHUSDT"
            },
            "data": [
                [
                    "1758798540000",
                    "111595.03",
                    "111595.04",
                    "111521.01",
                    "111529.52",
                    "14.06507470738",
                    "1568969.70849836405",
                    "1568969.70849836405"
                ]
            ],
            "ts": 1695702747821
        }

    @staticmethod
    def get_candles_ws_data_mock_2():
        return {
            "action": "update",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": "ETHUSDT"
            },
            "data": [
                [
                    "1758798600000",
                    "111529.52",
                    "111569.85",
                    "111529.52",
                    "111548.38",
                    "6.67627652466",
                    "744806.49272433786",
                    "744806.49272433786"
                ]
            ],
            "ts": 1695702747821
        }

    @staticmethod
    def _success_subscription_mock():
        return {
            "event": "subscribe",
            "arg": {
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": "ETHUSDT"
            }
        }
