"""Observa una sesión de ib_async sin tocar la estrategia: órdenes, fills, errores, conexión y latencias.

ObservedIB envuelve a IB y delega todo lo demás, de modo que strategy.py recibe la misma interfaz.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from demo_guard import assert_order_is_demo
from events import emit, timed

# Códigos informativos de IB (estado de granjas de datos, etc.): no son errores operativos.
IB_INFO_CODES = {1102, 2104, 2106, 2107, 2108, 2119, 2158, 10167}
IB_WARNING_CODES = {1100, 1101, 1300, 2103, 2105, 2110}


def _symbol(contract: Any) -> str | None:
    return getattr(contract, "symbol", None) or None


def _order_fields(contract: Any, order: Any) -> dict[str, Any]:
    return {
        "symbol": _symbol(contract),
        "con_id": getattr(contract, "conId", None),
        "order_id": order.orderId,
        "parent_id": order.parentId,
        "action": order.action,
        "order_type": order.orderType,
        "qty": order.totalQuantity,
        "lmt_price": order.lmtPrice,
        "aux_price": order.auxPrice,
        "tif": order.tif,
        "oca_group": order.ocaGroup,
        "transmit": order.transmit,
    }


def _fill_fields(fill: Any) -> dict[str, Any]:
    execution = fill.execution
    report = fill.commissionReport
    return {
        "symbol": _symbol(fill.contract),
        "exec_id": execution.execId,
        "order_id": execution.orderId,
        "side": execution.side,
        "shares": execution.shares,
        "price": execution.price,
        "exec_time": execution.time,
        "commission": getattr(report, "commission", None),
        "realized_pnl": getattr(report, "realizedPNL", None),
    }


class ObservedIB:
    def __init__(self, ib: Any) -> None:
        self._ib = ib
        ib.orderStatusEvent += self._on_order_status
        ib.execDetailsEvent += self._on_exec_details
        ib.errorEvent += self._on_error
        ib.disconnectedEvent += self._on_disconnected

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._ib, name)
        if name.endswith("Async") and callable(attr):
            return self._timed_call(name, attr)
        return attr

    def _timed_call(self, name: str, call: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            with timed(f"ib.{name}", symbol=_symbol(args[0]) if args else None):
                return await call(*args, **kwargs)

        return wrapper

    def placeOrder(self, contract: Any, order: Any) -> Any:  # noqa: N802 - misma API que ib_async
        assert_order_is_demo(order)
        with timed("ib.placeOrder", symbol=_symbol(contract)):
            trade = self._ib.placeOrder(contract, order)
        emit("order.submitted", **_order_fields(contract, order))
        return trade

    def _on_order_status(self, trade: Any) -> None:
        status = trade.orderStatus
        emit(
            "order.status",
            symbol=_symbol(trade.contract),
            order_id=trade.order.orderId,
            status=status.status,
            filled=status.filled,
            remaining=status.remaining,
            avg_fill_price=status.avgFillPrice,
        )

    def _on_exec_details(self, trade: Any, fill: Any) -> None:
        emit("fill", source="live", **_fill_fields(fill))

    def _on_error(self, req_id: int, code: int, message: str, contract: Any) -> None:
        if code in IB_INFO_CODES:
            level = logging.INFO
        elif code in IB_WARNING_CODES:
            level = logging.WARNING
        else:
            level = logging.ERROR
        emit("ib.error", level=level, req_id=req_id, code=code, message=message, symbol=_symbol(contract))

    def _on_disconnected(self) -> None:
        emit("connection.disconnected")


def reconcile_at_startup(ib: Any) -> None:
    """El bot duerme durante la sesión, así que los fills ocurren desconectado. Registra lo que IB devuelve
    al conectar. Puede repetir fills entre ejecuciones: deduplicar por exec_id."""
    for fill in ib.fills():
        emit("fill", source="reconcile", **_fill_fields(fill))
    for trade in ib.trades():
        status = trade.orderStatus
        emit(
            "order.reconciled",
            symbol=_symbol(trade.contract),
            order_id=trade.order.orderId,
            order_type=trade.order.orderType,
            status=status.status,
            filled=status.filled,
            avg_fill_price=status.avgFillPrice,
        )
    positions = [
        {"symbol": _symbol(p.contract), "con_id": p.contract.conId, "qty": p.position, "avg_cost": p.avgCost}
        for p in ib.positions()
    ]
    emit("positions.snapshot", count=len(positions), positions=positions)
