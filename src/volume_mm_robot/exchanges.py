from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import aiohttp

from volume_mm_robot.brokers import OrderRejected
from volume_mm_robot.models import (
    MarketRules,
    OrderBook,
    OrderRequest,
    OrderStatus,
    Position,
    Side,
    Trade,
    VenueOrderState,
)

logger = logging.getLogger(__name__)
MarketEvent = Trade | OrderBook


def _status(value: str) -> OrderStatus:
    normalized = value.lower()
    if normalized in {"filled"}:
        return OrderStatus.FILLED
    if normalized in {"new", "open", "partially_filled", "partiallyfilled"}:
        return OrderStatus.OPEN
    if normalized.endswith("rejected"):
        return OrderStatus.REJECTED
    if normalized.endswith("canceled") or normalized in {"expired", "expired_in_match"}:
        return OrderStatus.CANCELED
    raise RuntimeError(f"Неизвестный статус ордера: {value}")


class VenueAPIError(RuntimeError):
    def __init__(self, status: int, code: int | None, message: str) -> None:
        super().__init__(f"HTTP {status}, code={code}: {message}")
        self.status = status
        self.code = code


async def _response_json(response: aiohttp.ClientResponse) -> Any:
    try:
        data: Any = await response.json(content_type=None)
    except (ValueError, aiohttp.ClientError) as error:
        raise VenueAPIError(response.status, None, "ответ не удалось прочитать как JSON") from error
    code = data.get("code") if isinstance(data, dict) else None
    code = code if isinstance(code, int) else None
    if response.status >= 400 or (code is not None and code < 0):
        message = str(data.get("msg", "ошибка API")) if isinstance(data, dict) else "ошибка API"
        raise VenueAPIError(response.status, code, message)
    return data


class AsterVenue:
    def __init__(self, session: aiohttp.ClientSession, testnet: bool) -> None:
        self.session = session
        self.testnet = testnet
        self.rest_url = "https://fapi.asterdex-testnet.com" if testnet else "https://fapi.asterdex.com"
        self.trading_url = self.rest_url if testnet else "https://fapi3.asterdex.com"
        self.websocket_url = (
            "wss://fstream5.asterdex-testnet.com" if testnet else "wss://fstream.asterdex.com"
        )
        self._last_nonce = 0

    async def market_rules(self, symbol: str) -> MarketRules:
        async with self.session.get(f"{self.rest_url}/fapi/v3/exchangeInfo") as response:
            data = await _response_json(response)
        item = next((entry for entry in data["symbols"] if entry["symbol"] == symbol), None)
        if item is None:
            raise ValueError(f"Инструмент {symbol} не найден на Aster")
        filters = {entry["filterType"]: entry for entry in item["filters"]}
        prices = filters["PRICE_FILTER"]
        lots = filters["LOT_SIZE"]
        min_notional = filters.get("MIN_NOTIONAL", {})
        return MarketRules(
            tick_size=float(prices["tickSize"]),
            step_size=float(lots["stepSize"]),
            min_notional=float(min_notional.get("notional", min_notional.get("minNotional", 5))),
            min_price=float(prices.get("minPrice", 0)),
            min_quantity=float(lots.get("minQty", 0)),
            reduce_only_below_min_notional=True,
        )

    async def events(self, symbol: str) -> AsyncIterator[MarketEvent]:
        stream = f"{symbol.lower()}@aggTrade/{symbol.lower()}@depth20@100ms"
        while True:
            try:
                async with self.session.ws_connect(
                    f"{self.websocket_url}/stream?streams={stream}", heartbeat=30
                ) as websocket:
                    async for message in websocket:
                        if message.type is aiohttp.WSMsgType.TEXT:
                            payload = json.loads(message.data)
                            data = payload.get("data", payload)
                            event_type = data.get("e")
                            if event_type == "aggTrade":
                                yield Trade(
                                    symbol=data["s"],
                                    timestamp_ms=int(data["T"]),
                                    price=float(data["p"]),
                                    quantity=float(data["q"]),
                                    side=Side.SELL if data["m"] else Side.BUY,
                                )
                            elif event_type == "depthUpdate":
                                yield OrderBook(
                                    symbol=data["s"],
                                    timestamp_ms=int(data["E"]),
                                    bids=tuple((float(p), float(q)) for p, q in data["b"]),
                                    asks=tuple((float(p), float(q)) for p, q in data["a"]),
                                )
                        elif message.type is aiohttp.WSMsgType.ERROR:
                            raise ConnectionError(str(websocket.exception()))
                    raise ConnectionError("поток завершён")
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, ConnectionError, TimeoutError) as error:
                logger.warning("Aster WebSocket: %s; переподключение", error)
                await asyncio.sleep(2)

    async def prepare_trading(self, symbol: str) -> Position:
        mode = await self._signed_request("GET", "/fapi/v3/positionSide/dual", {})
        if mode.get("dualSidePosition") is not False:
            raise RuntimeError("Для торговли на Aster требуется подтверждённый one-way режим")
        open_orders = await self._signed_request("GET", "/fapi/v3/openOrders", {"symbol": symbol})
        if open_orders:
            raise RuntimeError("На Aster уже есть открытые ордера по инструменту; запуск остановлен")
        data = await self._signed_request("GET", "/fapi/v3/positionRisk", {"symbol": symbol})
        item = next((entry for entry in data if entry["symbol"] == symbol), None)
        if item is None:
            return Position()
        return Position(
            quantity=float(item["positionAmt"]),
            average_price=float(item["entryPrice"]),
            opened_at_ms=int(time.time() * 1000) if float(item["positionAmt"]) else None,
        )

    async def place_order(self, request: OrderRequest) -> VenueOrderState:
        params = {
            "symbol": request.symbol,
            "side": request.side.value.upper(),
            "type": "LIMIT",
            "timeInForce": "GTX" if request.post_only else "IOC",
            "quantity": format(Decimal(str(request.quantity)), "f"),
            "price": format(Decimal(str(request.price)), "f"),
            "newClientOrderId": self._client_order_id(request.client_id),
            "newOrderRespType": "RESULT",
        }
        if request.reduce_only:
            params["reduceOnly"] = "true"
        try:
            data = await self._signed_request("POST", "/fapi/v3/order", params)
        except VenueAPIError as error:
            if (
                error.status < 500
                and error.status not in {408, 429}
                and error.code is not None
                and error.code not in {-1000, -1001, -1006, -1007}
            ):
                raise OrderRejected(str(error)) from error
            raise
        return self._order_state(data)

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        await self._signed_request("DELETE", "/fapi/v3/order", {"symbol": symbol, "orderId": order_id})

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState:
        data = await self._signed_request("GET", "/fapi/v3/order", {"symbol": symbol, "orderId": order_id})
        return self._order_state(data)

    async def fetch_order_by_client_id(self, symbol: str, client_id: str) -> VenueOrderState | None:
        try:
            data = await self._signed_request(
                "GET",
                "/fapi/v3/order",
                {"symbol": symbol, "origClientOrderId": self._client_order_id(client_id)},
            )
        except VenueAPIError as error:
            if error.code == -2013:
                return None
            raise
        return self._order_state(data)

    @staticmethod
    def _client_order_id(client_id: str) -> str:
        return f"vmm{client_id[:29]}"

    @staticmethod
    def _order_state(data: Any) -> VenueOrderState:
        return VenueOrderState(
            order_id=str(data["orderId"]),
            status=_status(str(data["status"])),
            filled_quantity=float(data.get("executedQty", 0)),
            average_fill_price=float(data.get("avgPrice", 0)),
        )

    async def _signed_request(self, method: str, path: str, params: dict[str, str]) -> Any:
        try:
            from eth_account import Account
            from eth_account.messages import encode_typed_data
        except ImportError as error:
            raise RuntimeError("Для Aster testnet/live установите extras: uv sync --extra live") from error
        user = os.environ.get("ASTER_USER_ADDRESS")
        signer = os.environ.get("ASTER_SIGNER_ADDRESS")
        private_key = os.environ.get("ASTER_PRIVATE_KEY")
        if not user or not signer or not private_key:
            raise RuntimeError("Нужны ASTER_USER_ADDRESS, ASTER_SIGNER_ADDRESS и ASTER_PRIVATE_KEY")
        self._last_nonce = max(time.time_ns() // 1000, self._last_nonce + 1)
        signed_params = {**params, "nonce": str(self._last_nonce), "user": user, "signer": signer}
        encoded = urlencode(signed_params)
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Message": [{"name": "msg", "type": "string"}],
            },
            "primaryType": "Message",
            "domain": {
                "name": "AsterSignTransaction",
                "version": "1",
                "chainId": 1666,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {"msg": encoded},
        }
        signature = Account.sign_message(
            encode_typed_data(full_message=typed_data), private_key=private_key
        ).signature.hex()
        url = f"{self.trading_url}{path}?{encoded}&signature={signature}"
        try:
            async with self.session.request(method, url, timeout=aiohttp.ClientTimeout(total=4)) as response:
                return await _response_json(response)
        except aiohttp.ClientError as error:
            raise RuntimeError(f"Ошибка соединения Aster при {method} {path}") from error


class HyperliquidVenue:
    def __init__(self, session: aiohttp.ClientSession, testnet: bool) -> None:
        self.session = session
        self.testnet = testnet
        self.api_url = "https://api.hyperliquid-testnet.xyz" if testnet else "https://api.hyperliquid.xyz"
        self._exchange: Any = None
        self._account_address = ""

    async def market_rules(self, symbol: str) -> MarketRules:
        async with self.session.post(f"{self.api_url}/info", json={"type": "meta"}) as response:
            data = await _response_json(response)
        item = next((entry for entry in data["universe"] if entry["name"] == symbol), None)
        if item is None:
            raise ValueError(f"Инструмент {symbol} не найден на Hyperliquid")
        size_decimals = int(item["szDecimals"])
        return MarketRules(
            tick_size=0.0,
            step_size=10.0 ** (-size_decimals),
            min_notional=10.0,
            max_price_decimals=max(0, 6 - size_decimals),
            significant_figures=5,
        )

    async def events(self, symbol: str) -> AsyncIterator[MarketEvent]:
        while True:
            try:
                async with self.session.ws_connect(f"{self.api_url}/ws", heartbeat=30) as websocket:
                    for subscription in (
                        {"type": "trades", "coin": symbol},
                        {"type": "l2Book", "coin": symbol},
                    ):
                        await websocket.send_json({"method": "subscribe", "subscription": subscription})
                    async for message in websocket:
                        if message.type is aiohttp.WSMsgType.TEXT:
                            payload = json.loads(message.data)
                            channel = payload.get("channel")
                            data = payload.get("data")
                            if channel == "trades":
                                for item in data:
                                    yield Trade(
                                        symbol=item["coin"],
                                        timestamp_ms=int(item["time"]),
                                        price=float(item["px"]),
                                        quantity=float(item["sz"]),
                                        side=Side.BUY if item["side"] in {"B", "Buy", "buy"} else Side.SELL,
                                    )
                            elif channel == "l2Book":
                                yield OrderBook(
                                    symbol=data["coin"],
                                    timestamp_ms=int(data["time"]),
                                    bids=tuple(
                                        (float(level["px"]), float(level["sz"]))
                                        for level in data["levels"][0]
                                    ),
                                    asks=tuple(
                                        (float(level["px"]), float(level["sz"]))
                                        for level in data["levels"][1]
                                    ),
                                )
                        elif message.type is aiohttp.WSMsgType.ERROR:
                            raise ConnectionError(str(websocket.exception()))
                    raise ConnectionError("поток завершён")
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, ConnectionError, TimeoutError) as error:
                logger.warning("Hyperliquid WebSocket: %s; переподключение", error)
                await asyncio.sleep(2)

    async def prepare_trading(self, symbol: str) -> Position:
        exchange = await self._call_sdk(self._sdk)
        orders = await self._call_sdk(exchange.info.frontend_open_orders, self._account_address)
        if any(item["coin"] == symbol for item in orders):
            raise RuntimeError("На Hyperliquid уже есть открытые ордера по инструменту; запуск остановлен")
        state = await self._call_sdk(exchange.info.user_state, self._account_address)
        for entry in state["assetPositions"]:
            item = entry["position"]
            if item["coin"] == symbol:
                quantity = float(item["szi"])
                return Position(
                    quantity=quantity,
                    average_price=float(item["entryPx"]),
                    opened_at_ms=int(time.time() * 1000) if quantity else None,
                )
        return Position()

    async def place_order(self, request: OrderRequest) -> VenueOrderState:
        from hyperliquid.utils.types import Cloid

        exchange = self._sdk()
        result = await self._call_sdk(
            exchange.order,
            request.symbol,
            request.side is Side.BUY,
            request.quantity,
            request.price,
            {"limit": {"tif": "Alo" if request.post_only else "Ioc"}},
            request.reduce_only,
            Cloid.from_str(f"0x{request.client_id}"),
        )
        if result.get("status") != "ok":
            raise RuntimeError(f"Hyperliquid order: {result}")
        status = result["response"]["data"]["statuses"][0]
        if "error" in status:
            raise OrderRejected(f"Hyperliquid order: {status['error']}")
        if "resting" in status:
            return VenueOrderState(str(status["resting"]["oid"]), OrderStatus.OPEN, 0.0, 0.0)
        if "filled" in status:
            filled = status["filled"]
            quantity = float(filled["totalSz"])
            return VenueOrderState(
                str(filled["oid"]),
                OrderStatus.FILLED if quantity >= request.quantity else OrderStatus.CANCELED,
                quantity,
                float(filled["avgPx"]),
            )
        raise RuntimeError(f"Hyperliquid order status: {status}")

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        result = await self._call_sdk(self._sdk().cancel, symbol, int(order_id))
        if result.get("status") != "ok" or result["response"]["data"]["statuses"] != ["success"]:
            raise RuntimeError(f"Hyperliquid cancel: {result}")

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState:
        exchange = self._sdk()
        result = await self._call_sdk(exchange.info.query_order_by_oid, self._account_address, int(order_id))
        state = await self._order_state(result)
        if state is None:
            raise RuntimeError(f"Hyperliquid не подтвердил статус ордера {order_id}")
        return state

    async def fetch_order_by_client_id(self, symbol: str, client_id: str) -> VenueOrderState | None:
        from hyperliquid.utils.types import Cloid

        result = await self._call_sdk(
            self._sdk().info.query_order_by_cloid, self._account_address, Cloid.from_str(f"0x{client_id}")
        )
        return await self._order_state(result)

    async def _order_state(self, result: Any) -> VenueOrderState | None:
        if result.get("status") == "unknownOid":
            return None
        if result.get("status") != "order":
            raise RuntimeError(f"Hyperliquid order status: {result}")
        wrapper = result["order"]
        basic = wrapper["order"]
        order_id = str(basic["oid"])
        status = _status(str(wrapper["status"]))
        filled = float(Decimal(basic["origSz"]) - Decimal(basic["sz"]))
        if filled <= 0:
            if status is OrderStatus.FILLED:
                raise RuntimeError(f"Hyperliquid не подтвердил объём ордера {order_id}")
            return VenueOrderState(order_id, status, 0.0, 0.0)
        fills = await self._call_sdk(self._sdk().info.user_fills, self._account_address)
        matching = [item for item in fills if str(item["oid"]) == order_id]
        quantity = sum(float(item["sz"]) for item in matching)
        if not math.isclose(quantity, filled, abs_tol=1e-12, rel_tol=0):
            raise RuntimeError(f"Hyperliquid вернул несогласованные объёмы исполнений ордера {order_id}")
        average = sum(float(item["px"]) * float(item["sz"]) for item in matching) / quantity
        return VenueOrderState(order_id, status, filled, average)

    @staticmethod
    async def _call_sdk[**P, T](method: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        from hyperliquid.utils.error import Error
        from requests.exceptions import RequestException

        try:
            return await asyncio.to_thread(method, *args, **kwargs)
        except (Error, RequestException) as error:
            raise RuntimeError(f"Ошибка Hyperliquid SDK: {type(error).__name__}") from error

    def _sdk(self) -> Any:
        if self._exchange is not None:
            return self._exchange
        try:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
        except ImportError as error:
            raise RuntimeError(
                "Для Hyperliquid testnet/live установите extras: uv sync --extra live"
            ) from error
        private_key = os.environ.get("HYPERLIQUID_PRIVATE_KEY")
        self._account_address = os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS", "")
        if not private_key or not self._account_address:
            raise RuntimeError("Нужны HYPERLIQUID_PRIVATE_KEY и HYPERLIQUID_ACCOUNT_ADDRESS")
        wallet = Account.from_key(private_key)
        self._exchange = Exchange(wallet, self.api_url, account_address=self._account_address, timeout=4.0)
        return self._exchange
