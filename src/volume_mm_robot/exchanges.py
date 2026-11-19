from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

import aiohttp

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
    if normalized in {"rejected", "expired"}:
        return OrderStatus.REJECTED
    return OrderStatus.CANCELED


async def _response_json(response: aiohttp.ClientResponse) -> Any:
    data: Any = await response.json(content_type=None)
    if response.status >= 400:
        raise RuntimeError(f"HTTP {response.status}: {data}")
    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        raise RuntimeError(f"API: {data}")
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
        min_notional = filters.get("MIN_NOTIONAL", {})
        return MarketRules(
            tick_size=float(filters["PRICE_FILTER"]["tickSize"]),
            step_size=float(filters["LOT_SIZE"]["stepSize"]),
            min_notional=float(min_notional.get("notional", min_notional.get("minNotional", 5))),
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
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, ConnectionError, TimeoutError) as error:
                logger.warning("Aster WebSocket: %s; переподключение", error)
                await asyncio.sleep(2)

    async def prepare_trading(self, symbol: str) -> Position:
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
            "quantity": str(request.quantity),
            "price": str(request.price),
            "newClientOrderId": f"vmm{request.client_id[:29]}",
            "newOrderRespType": "RESULT",
        }
        if request.reduce_only:
            params["reduceOnly"] = "true"
        data = await self._signed_request("POST", "/fapi/v3/order", params)
        return VenueOrderState(
            order_id=str(data["orderId"]),
            status=_status(str(data["status"])),
            filled_quantity=float(data.get("executedQty", 0)),
            average_fill_price=float(data.get("avgPrice", 0)),
        )

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        await self._signed_request("DELETE", "/fapi/v3/order", {"symbol": symbol, "orderId": order_id})

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState:
        data = await self._signed_request("GET", "/fapi/v3/order", {"symbol": symbol, "orderId": order_id})
        return VenueOrderState(
            order_id=order_id,
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
        async with self.session.request(method, url) as response:
            return await _response_json(response)


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
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, ConnectionError, TimeoutError) as error:
                logger.warning("Hyperliquid WebSocket: %s; переподключение", error)
                await asyncio.sleep(2)

    async def prepare_trading(self, symbol: str) -> Position:
        self._sdk()
        orders = await asyncio.to_thread(self._exchange.info.frontend_open_orders, self._account_address)
        if any(item["coin"] == symbol for item in orders):
            raise RuntimeError("На Hyperliquid уже есть открытые ордера по инструменту; запуск остановлен")
        state = await asyncio.to_thread(self._exchange.info.user_state, self._account_address)
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
        result = await asyncio.to_thread(
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
            raise RuntimeError(f"Hyperliquid order: {status['error']}")
        if "resting" in status:
            return VenueOrderState(str(status["resting"]["oid"]), OrderStatus.OPEN, 0.0, 0.0)
        if "filled" in status:
            filled = status["filled"]
            return VenueOrderState(
                str(filled["oid"]),
                OrderStatus.FILLED,
                float(filled["totalSz"]),
                float(filled["avgPx"]),
            )
        raise RuntimeError(f"Hyperliquid order status: {status}")

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        result = await asyncio.to_thread(self._sdk().cancel, symbol, int(order_id))
        if result.get("status") != "ok":
            raise RuntimeError(f"Hyperliquid cancel: {result}")

    async def fetch_order(self, symbol: str, order_id: str) -> VenueOrderState:
        exchange = self._sdk()
        result = await asyncio.to_thread(
            exchange.info.query_order_by_oid, self._account_address, int(order_id)
        )
        if result.get("status") != "order":
            return VenueOrderState(order_id, OrderStatus.CANCELED, 0.0, 0.0)
        wrapper = result["order"]
        basic = wrapper["order"]
        filled = max(0.0, float(basic["origSz"]) - float(basic["sz"]))
        fills = await asyncio.to_thread(exchange.info.user_fills, self._account_address)
        matching = [item for item in fills if str(item["oid"]) == order_id]
        quantity = sum(float(item["sz"]) for item in matching)
        average = (
            sum(float(item["px"]) * float(item["sz"]) for item in matching) / quantity
            if quantity > 0
            else 0.0
        )
        return VenueOrderState(order_id, _status(str(wrapper["status"])), filled, average)

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
        self._exchange = Exchange(wallet, self.api_url, account_address=self._account_address)
        return self._exchange
