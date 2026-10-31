"""Backtest reproducible: ejecuta el `strategy.run_once` real, día a día, contra un broker simulado.

No hay una segunda implementación de la estrategia, así que el backtest no puede divergir de lo que opera el bot.

Uso:
    venv/bin/python src/backtest.py --synthetic --seed 42 --symbols 30 --days 800
    venv/bin/python src/backtest.py --csv-dir datos/ --out backtest_out/run.json

CSV: un archivo por símbolo (SIMBOLO.csv) más VIX.csv, con columnas date,open,high,low,close,volume y fecha ISO.

Modelo de ejecución (mismo horario que producción: run_once corre antes de la apertura con barras hasta ayer):
- MOO (tif OPG): se ejecuta a la apertura; activa sus órdenes hijas.
- STP: se dispara si low <= stop; con gap a la baja se ejecuta a la apertura.
- LOC de venta: se ejecuta al cierre si close >= límite. Con ocaType 1 o 2 se rechaza, como hace IB (error 201).
- OCA: al ejecutarse una, se cancelan las demás del grupo.
- tif DAY o vacío: la orden expira al cierre, igual que en IB. No se simulan comisiones ni deslizamiento.

Limitaciones: el escáner es una aproximación (precio entre 1 y 100, cercanía al máximo de 13 semanas, top 50) y los
datos sintéticos no predicen rentabilidad real. Sirve para regresión y para validar mecánica, no para estimar retornos.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import logging
import math
import random
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from ib_async import BarData

import strategy

SCAN_SIZE = 50
SCAN_LOOKBACK = 63
INITIAL_EQUITY = 10_000.0  # igual que InitialEquity en docs/strategy.txt
TIMEZONE = "US/Eastern"
DURATION_SESSIONS = {"Y": 252, "M": 21, "W": 5, "D": 1}
VIX_CON_ID = 0


@dataclass(frozen=True)
class Trade:
    symbol: str
    qty: int
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    exit_reason: str
    pnl: float


@dataclass
class BacktestResult:
    initial_equity: float
    final_equity: float
    trades: list[Trade]
    equity_curve: list[float]
    open_positions: int
    unprotected_position_days: int
    rejected_orders: int
    max_drawdown_pct: float
    fingerprint: str = field(default="")

    def summary(self) -> dict[str, Any]:
        wins = [t for t in self.trades if t.pnl > 0]
        return {
            "initial_equity": self.initial_equity,
            "final_equity": self.final_equity,
            "trades": len(self.trades),
            "win_rate": round(len(wins) / len(self.trades), 4) if self.trades else None,
            "exit_reasons": _count(t.exit_reason for t in self.trades),
            "open_positions": self.open_positions,
            "unprotected_position_days": self.unprotected_position_days,
            "rejected_orders": self.rejected_orders,
            "max_drawdown_pct": self.max_drawdown_pct,
            "fingerprint": self.fingerprint,
        }


def _count(items: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[item] = counts.get(item, 0) + 1
    return dict(sorted(counts.items()))


def synthetic_market(seed: int, symbols: int, days: int) -> dict[str, list[BarData]]:
    """Mercado sintético determinista: paseos con deriva, ráfagas alcistas y caídas bruscas que activan señales."""
    rng = random.Random(seed)
    start = date(2020, 1, 1)
    sessions = [start + timedelta(days=i) for i in range(days * 2) if (start + timedelta(days=i)).weekday() < 5][:days]
    market: dict[str, list[BarData]] = {}

    for index in range(symbols):
        price = rng.uniform(15, 80)
        vol = rng.uniform(0.008, 0.025)
        volume = rng.uniform(200_000, 2_000_000)
        bars = []
        for day in sessions:
            shock = rng.gauss(0.0005, vol)
            regime = rng.random()
            if regime < 0.01:
                shock += rng.uniform(0.04, 0.10)
            elif regime < 0.03:
                shock -= rng.uniform(0.05, 0.12)
            open_ = price * (1 + rng.gauss(0, vol / 3))
            close = max(1.0, open_ * (1 + shock))
            high = max(open_, close) * (1 + abs(rng.gauss(0, vol / 2)))
            low = min(open_, close) * (1 - abs(rng.gauss(0, vol / 2)))
            bars.append(_bar(day, open_, high, low, close, volume * rng.uniform(0.7, 1.3)))
            price = close
        market[f"S{index:03d}"] = bars

    vix = rng.uniform(14, 22)
    vix_bars = []
    for day in sessions:
        vix = max(9.0, min(80.0, vix + 0.05 * (18 - vix) + rng.gauss(0, 1.0)))
        vix_bars.append(_bar(day, vix, vix, vix, vix, 0.0))
    market["VIX"] = vix_bars
    return market


def _bar(day: date, open_: float, high: float, low: float, close: float, volume: float) -> BarData:
    return BarData(
        date=day,
        open=round(open_, 4),
        high=round(high, 4),
        low=round(low, 4),
        close=round(close, 4),
        volume=round(volume),
    )


def load_csv_market(directory: Path) -> dict[str, list[BarData]]:
    market: dict[str, list[BarData]] = {}
    for path in sorted(directory.glob("*.csv")):
        with path.open(newline="") as handle:
            rows = sorted(csv.DictReader(handle), key=lambda r: r["date"])
        market[path.stem.upper()] = [
            _bar(
                date.fromisoformat(r["date"]),
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
            )
            for r in rows
        ]
    if "VIX" not in market:
        raise ValueError("Falta VIX.csv")
    return market


class SimIB:
    """Implementa solo la parte de la API de IB que usa strategy.py. Es un simulador: no abre red."""

    def __init__(self, market: dict[str, list[BarData]], equity: float = INITIAL_EQUITY) -> None:
        self.market = market
        self.symbols = sorted(s for s in market if s != "VIX")
        self.con_ids = {s: i + 1 for i, s in enumerate(self.symbols)}
        self.by_con_id = {i: s for s, i in self.con_ids.items()}
        self.by_date = {s: {b.date: b for b in bars} for s, bars in market.items()}
        self.calendar = sorted(b.date for b in market["VIX"])
        self.today_index = 0
        self.cash = equity
        self.positions: dict[int, dict[str, Any]] = {}
        self.orders: dict[int, dict[str, Any]] = {}
        self.trades: list[Trade] = []
        self.unprotected_days = 0
        self.rejected_orders = 0
        self.seen_ids: set[int] = set()
        self._next_id = 0
        self.client = SimpleNamespace(getReqId=self._req_id)

    def _req_id(self) -> int:
        self._next_id += 1
        return self._next_id

    @property
    def today(self) -> date:
        return self.calendar[self.today_index]

    def _history(self, symbol: str) -> list[BarData]:
        """Sin sesgo de anticipación: solo barras estrictamente anteriores a hoy."""
        return [b for b in self.market[symbol] if b.date < self.today]

    def managedAccounts(self) -> list[str]:  # noqa: N802
        return ["DSIM0001"]

    async def reqContractDetailsAsync(self, contract: Any) -> list[Any]:  # noqa: N802
        real_today = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y%m%d")
        return [SimpleNamespace(timeZoneId=TIMEZONE, tradingHours=f"{real_today}:0930-1600")]

    async def qualifyContractsAsync(self, *contracts: Any) -> list[Any]:  # noqa: N802
        for contract in contracts:
            if contract.conId in self.by_con_id:
                contract.symbol = self.by_con_id[contract.conId]
        return list(contracts)

    async def reqHistoricalDataAsync(self, contract: Any, **kwargs: Any) -> list[BarData]:  # noqa: N802
        symbol = "VIX" if contract.symbol == "VIX" else self.by_con_id[contract.conId]
        amount, unit = kwargs["durationStr"].split()
        return self._history(symbol)[-int(amount) * DURATION_SESSIONS[unit] :]

    async def reqScannerDataAsync(self, *args: Any) -> list[Any]:  # noqa: N802
        scored = []
        for symbol in self.symbols:
            history = self._history(symbol)
            if len(history) < SCAN_LOOKBACK or not 1 < history[-1].close < 100:
                continue
            top = max(b.high for b in history[-SCAN_LOOKBACK:])
            scored.append((history[-1].close / top, symbol))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [
            SimpleNamespace(
                contractDetails=SimpleNamespace(
                    contract=SimpleNamespace(
                        conId=self.con_ids[symbol],
                        symbol=symbol,
                        exchange="SMART",
                        primaryExchange="SIM",
                        currency="USD",
                    )
                )
            )
            for _, symbol in scored[:SCAN_SIZE]
        ]

    async def accountSummaryAsync(self) -> list[Any]:  # noqa: N802
        return [SimpleNamespace(tag="NetLiquidation", value=str(self.mark_to_market()))]

    async def reqOpenOrdersAsync(self) -> list[Any]:  # noqa: N802
        return await self.reqAllOpenOrdersAsync()

    async def reqAllOpenOrdersAsync(self) -> list[Any]:  # noqa: N802
        return [
            SimpleNamespace(contract=SimpleNamespace(conId=record["con_id"]), order=record["order"])
            for record in self.orders.values()
        ]

    async def reqPositionsAsync(self) -> list[Any]:  # noqa: N802
        return [
            SimpleNamespace(
                contract=SimpleNamespace(conId=con_id, symbol=self.by_con_id[con_id]),
                position=float(pos["qty"]),
                avgCost=pos["entry_price"],
            )
            for con_id, pos in sorted(self.positions.items())
        ]

    def placeOrder(self, contract: Any, order: Any) -> None:  # noqa: N802
        if order.orderType == "LOC" and order.ocaGroup and order.ocaType in (1, 2):
            self.rejected_orders += 1  # IB: error 201 "Invalid OCA handling method", comprobado en demo
            return
        if order.orderId in self.seen_ids and order.orderId not in self.orders:
            self.rejected_orders += 1  # IB: error 103 "Duplicate order id", comprobado en demo
            return
        self.seen_ids.add(order.orderId)
        self.orders[order.orderId] = {"con_id": contract.conId, "order": order, "active": order.parentId == 0}

    def cancelOrder(self, order: Any) -> None:  # noqa: N802
        self.orders.pop(order.orderId, None)

    def openTrades(self) -> list[Any]:  # noqa: N802
        return [SimpleNamespace(order=record["order"]) for record in self.orders.values()]

    def _close_of(self, con_id: int, fallback: float) -> float:
        bars = self._history(self.by_con_id[con_id])
        return bars[-1].close if bars else fallback

    def mark_to_market(self) -> float:
        value = self.cash
        for con_id, pos in self.positions.items():
            value += pos["qty"] * self._close_of(con_id, pos["entry_price"])
        return round(value, 2)

    def _cancel_oca(self, group: str, keep: int) -> None:
        if group:
            for order_id in [i for i, o in self.orders.items() if o["order"].ocaGroup == group and i != keep]:
                del self.orders[order_id]

    def _exit(self, con_id: int, price: float, reason: str) -> None:
        pos = self.positions.pop(con_id)
        self.cash += pos["qty"] * price
        self.trades.append(
            Trade(
                symbol=self.by_con_id[con_id],
                qty=pos["qty"],
                entry_date=pos["entry_date"],
                entry_price=pos["entry_price"],
                exit_date=self.today.isoformat(),
                exit_price=round(price, 4),
                exit_reason=reason,
                pnl=round(pos["qty"] * (price - pos["entry_price"]), 2),
            )
        )

    def simulate_session(self) -> None:
        """Ejecuta las órdenes vivas contra la barra de hoy, en orden cronológico: apertura, sesión, cierre."""
        for order_id, record in sorted(self.orders.items()):
            order = record["order"]
            if order.tif != "OPG" or order_id not in self.orders:
                continue
            bar = self.by_date[self.by_con_id[record["con_id"]]].get(self.today)
            del self.orders[order_id]
            if bar is None:
                for child_id in [i for i, o in self.orders.items() if o["order"].parentId == order_id]:
                    del self.orders[child_id]
                continue
            self.cash -= order.totalQuantity * bar.open
            self.positions[record["con_id"]] = {
                "qty": int(order.totalQuantity),
                "entry_price": bar.open,
                "entry_date": self.today.isoformat(),
            }
            for child in self.orders.values():
                if child["order"].parentId == order_id:
                    child["active"] = True

        protected = {r["con_id"] for r in self.orders.values() if r["active"] and r["order"].orderType == "STP"}
        self.unprotected_days += sum(1 for con_id in self.positions if con_id not in protected)

        for order_id, record in sorted(self.orders.items()):
            order = record["order"]
            if order.orderType != "STP" or order_id not in self.orders or not record["active"]:
                continue
            if record["con_id"] not in self.positions:
                continue
            bar = self.by_date[self.by_con_id[record["con_id"]]].get(self.today)
            if bar is not None and bar.low <= order.auxPrice:
                self._exit(record["con_id"], min(order.auxPrice, bar.open), "stop")
                del self.orders[order_id]
                self._cancel_oca(order.ocaGroup, order_id)

        for order_id, record in sorted(self.orders.items()):
            order = record["order"]
            if order.orderType != "LOC" or order_id not in self.orders:
                continue
            if record["con_id"] not in self.positions:
                continue
            bar = self.by_date[self.by_con_id[record["con_id"]]].get(self.today)
            if bar is not None and bar.close >= order.lmtPrice:
                self._exit(record["con_id"], bar.close, "loc")
                del self.orders[order_id]
                self._cancel_oca(order.ocaGroup, order_id)

        # tif vacío o DAY: expira al cierre, como en IB.
        self.orders = {i: r for i, r in self.orders.items() if r["order"].tif not in ("", "DAY")}


def _fingerprint(trades: list[Trade], curve: list[float]) -> str:
    payload = json.dumps({"trades": [asdict(t) for t in trades], "curve": curve}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _max_drawdown(curve: list[float]) -> float:
    peak, worst = -math.inf, 0.0
    for value in curve:
        peak = max(peak, value)
        worst = max(worst, (peak - value) / peak if peak > 0 else 0.0)
    return round(worst * 100, 2)


async def _run(market: dict[str, list[BarData]], warmup: int, equity: float) -> BacktestResult:
    sim: Any = SimIB(market, equity)
    curve = []
    for index in range(warmup, len(sim.calendar)):
        sim.today_index = index
        await strategy.run_once(sim)
        sim.simulate_session()
        curve.append(sim.mark_to_market())
    return BacktestResult(
        initial_equity=equity,
        final_equity=curve[-1] if curve else equity,
        trades=sim.trades,
        equity_curve=curve,
        open_positions=len(sim.positions),
        unprotected_position_days=sim.unprotected_days,
        rejected_orders=sim.rejected_orders,
        max_drawdown_pct=_max_drawdown(curve),
        fingerprint=_fingerprint(sim.trades, curve),
    )


def run_backtest(
    market: dict[str, list[BarData]], warmup: int = 260, equity: float = INITIAL_EQUITY
) -> BacktestResult:
    """Usa siempre una base de datos temporal: nunca lee ni escribe trading_state.db."""
    original_db = strategy.DB_PATH
    previous_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with tempfile.TemporaryDirectory(prefix="backtest-") as tmp:
            strategy.DB_PATH = str(Path(tmp) / "state.db")
            return asyncio.run(_run(market, warmup, equity))
    finally:
        strategy.DB_PATH = original_db
        logging.disable(previous_disable)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--synthetic", action="store_true")
    source.add_argument("--csv-dir", type=Path)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--symbols", type=int, default=30)
    parser.add_argument("--days", type=int, default=800)
    parser.add_argument("--warmup", type=int, default=260, help="sesiones iniciales sin operar")
    parser.add_argument("--equity", type=float, default=INITIAL_EQUITY)
    parser.add_argument("--out", type=Path, help="JSON con operaciones y curva de capital")
    args = parser.parse_args(argv)

    market = load_csv_market(args.csv_dir) if args.csv_dir else synthetic_market(args.seed, args.symbols, args.days)
    result = run_backtest(market, args.warmup, args.equity)
    print(json.dumps(result.summary(), indent=2))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
