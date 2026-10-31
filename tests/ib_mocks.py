"""Dobles de prueba de ib_async. `spec=IB` hace fallar cualquier llamada a un método que no exista en la API real."""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from eventkit import Event
from ib_async import IB, BarData

DEMO_ACCOUNT = "DU1234567"
EVENT_NAMES = (
    "connectedEvent",
    "disconnectedEvent",
    "orderStatusEvent",
    "execDetailsEvent",
    "commissionReportEvent",
    "errorEvent",
)


def make_mock_ib(accounts: Sequence[str] = (DEMO_ACCOUNT,), equity: float = 100_000.0) -> MagicMock:
    ib = MagicMock(spec=IB)
    for name in dir(IB):
        if name.endswith("Async"):
            setattr(ib, name, AsyncMock(name=name))
    for name in EVENT_NAMES:
        setattr(ib, name, Event(name))

    ids = itertools.count(1)
    ib.client = MagicMock()
    ib.client.getReqId.side_effect = lambda: next(ids)
    ib.placed = []
    ib.placeOrder.side_effect = lambda contract, order: ib.placed.append((contract, order)) or MagicMock()

    ib.managedAccounts.return_value = list(accounts)
    ib.isConnected.return_value = True
    ib.fills.return_value = []
    ib.trades.return_value = []
    ib.openTrades.return_value = []
    ib.positions.return_value = []
    ib.qualifyContractsAsync.side_effect = lambda *contracts: list(contracts)
    ib.accountSummaryAsync.return_value = [SimpleNamespace(tag="NetLiquidation", value=str(equity))]
    ib.reqPositionsAsync.return_value = []
    ib.reqOpenOrdersAsync.return_value = []
    ib.reqAllOpenOrdersAsync.return_value = []
    ib.reqScannerDataAsync.return_value = []
    ib.reqHistoricalDataAsync.return_value = []
    ib.reqContractDetailsAsync.return_value = []
    return ib


def bar(open_: float, high: float, low: float, close: float, volume: float = 1_000_000, day: int = 0) -> BarData:
    when = date(2026, 1, 1) + timedelta(days=day)
    return BarData(date=when, open=open_, high=high, low=low, close=close, volume=volume)


def flat_bars(count: int = 100, price: float = 50.0) -> list[BarData]:
    """Barras planas: high-low = 2, por lo que el ATR es exactamente 2."""
    return [bar(price, price + 1, price - 1, price, day=i) for i in range(count)]


def scanner_row(con_id: int, symbol: str, exchange: str = "SMART", currency: str = "USD") -> Any:
    contract = SimpleNamespace(
        conId=con_id, symbol=symbol, exchange=exchange, primaryExchange="NYSE", currency=currency
    )
    return SimpleNamespace(contractDetails=SimpleNamespace(contract=contract))
