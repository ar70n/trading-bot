"""Fija el comportamiento ACTUAL de la estrategia. Si uno de estos tests cambia, ha cambiado la estrategia:
eso requiere argumentación y confirmación explícita del usuario. Fijar no significa aprobar:
los tests `test_known_issue_*` documentan defectos conocidos pendientes de decisión.
"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import strategy
from ib_mocks import bar, flat_bars


def _entry(**overrides):
    base = {"con_id": 1, "symbol": "AAA", "exchange": "SMART", "currency": "USD"}
    return {**base, **overrides}


BREAKOUT = bar(52, 60, 55, 58)  # H 60 > 51: ruptura; Limit_buy = 55 - 2 * 2.4 = 50.2
HOLD = bar(58, 59, 57, 58)  # dentro de la ventana, sin tocar el nivel
PULLBACK_RED = bar(52, 53, 49, 50)  # L 49 < 50.2 y C < O: compra


def _signal_bars(*tail, count=100):
    bars = flat_bars(count - len(tail))
    return bars + [bar(b.open, b.high, b.low, b.close, day=len(bars) + i) for i, b in enumerate(tail)]


def test_parameters_are_pinned():
    assert (strategy.MAX_POSITIONS, strategy.MARGIN_PCT, strategy.POSITION_SIZE_PCT_OF_EQUITY) == (10, 50, 20.0)
    assert (strategy.HH_LOOKBACK_DAYS, strategy.ATR_PERIOD, strategy.ATR_MULTIPLIER) == (63, 20, 2)
    assert (strategy.BREAKOUT_WINDOW_DAYS, strategy.HV_LOOKBACK_DAYS) == (10, 252)
    assert (strategy.VIX_LOOKBACK_DAYS, strategy.VIX_PERCENTILE_MAX) == (100, 85)
    assert len(strategy.load_universe()) == 503 and "BRK B" in strategy.load_universe()


def test_atr_on_flat_bars():
    assert strategy.atr_series(flat_bars(50))[-1] == pytest.approx(2.0)
    assert strategy.atr_series(flat_bars(21))[-1] == pytest.approx(2.0)
    assert strategy.atr_series(flat_bars(20))[-1] is None


def test_breakout_then_red_pullback_is_a_buy_with_atr_of_previous_bar():
    bars = _signal_bars(BREAKOUT, HOLD, HOLD, HOLD, PULLBACK_RED)
    stop_distance = strategy.entry_signal(bars)
    assert stop_distance == pytest.approx(2 * strategy.atr_series(bars)[-2])  # SLatentry = Ref(ATRvar, -1)
    assert stop_distance == pytest.approx(4.6859, abs=1e-4)


def test_breakout_level_uses_atr_including_breakout_bar():
    """Limit_buy = L - ATRvar de la propia barra de ruptura: 50.2. Un mínimo de 50.3 no la toca."""
    assert strategy.entry_signal(_signal_bars(BREAKOUT, bar(52, 53, 50.3, 51))) is None
    assert strategy.entry_signal(_signal_bars(BREAKOUT, bar(52, 53, 50.1, 51))) is not None


@pytest.mark.parametrize(
    "tail",
    [
        (BREAKOUT, HOLD, bar(49, 53, 49, 50)),  # toca el nivel con vela verde
        (BREAKOUT, bar(51, 53, 49, 52), PULLBACK_RED),  # el nivel se anuló al tocarlo el día anterior
        (BREAKOUT, *[HOLD] * 9, PULLBACK_RED),  # fuera de la ventana de 10 sesiones
        (PULLBACK_RED,),  # sin ruptura previa
    ],
    ids=["green_candle", "level_cancelled", "window_expired", "no_breakout"],
)
def test_no_buy_without_all_conditions(tail):
    assert strategy.entry_signal(_signal_bars(*tail)) is None


def test_new_breakout_recomputes_buy_level():
    """Cada ruptura dentro de la ventana fija un nivel nuevo, como el bucle de Limit_buy del AFL."""
    second = bar(58, 70, 65, 68)
    hold = bar(68, 69, 67, 68)
    pullback = bar(66, 66, 58, 60)  # por debajo del nivel nuevo (~59.4), muy por encima del primero (50.2)
    assert strategy.entry_signal(_signal_bars(BREAKOUT, HOLD, second, hold, pullback)) is not None


async def test_vix_filter_threshold(ib):
    ib.reqHistoricalDataAsync.return_value = [bar(v, v, v, v) for v in range(1, 101)]
    assert await strategy.get_vix_percentile_ok(ib) is False  # el último es el máximo: percentil 100

    closes = list(range(1, 100)) + [84]  # 85 valores <= 84: percentil 85, justo en el límite
    ib.reqHistoricalDataAsync.return_value = [bar(v, v, v, v) for v in closes]
    assert await strategy.get_vix_percentile_ok(ib) is True

    ib.reqHistoricalDataAsync.return_value = flat_bars(10)
    assert await strategy.get_vix_percentile_ok(ib) is False  # sin datos: no se permiten entradas


@pytest.mark.parametrize(
    "suffix,expected", [("0930-1600", True), ("CLOSED", False)], ids=["open", "holiday"]
)
async def test_trading_day_from_calendar(ib, suffix, expected):
    today = datetime.now(ZoneInfo("US/Eastern")).strftime("%Y%m%d")
    details = SimpleNamespace(timeZoneId="US/Eastern", tradingHours=f"{today}:{suffix};20000101:CLOSED")
    ib.reqContractDetailsAsync.return_value = [details]
    assert await strategy.is_trading_day(ib) is expected


async def test_entry_bracket_orders(ib):
    await strategy.submit_entry_bracket(ib, SimpleNamespace(), _entry(), 4.0, 49.0, 52.0)

    (_, parent), (_, stop), (_, take_profit) = ib.placed
    assert (parent.action, parent.orderType, parent.tif, parent.transmit) == ("BUY", "MKT", "OPG", False)
    assert parent.totalQuantity == 408  # 100.000 * 20% // 49
    assert (stop.action, stop.orderType, stop.totalQuantity, stop.auxPrice) == ("SELL", "STP", 408, 45.0)
    assert (stop.parentId, stop.ocaGroup, stop.ocaType, stop.transmit) == (parent.orderId, "pos_1", 1, False)
    assert (take_profit.action, take_profit.orderType, take_profit.totalQuantity) == ("SELL", "LMT", 408)
    assert (take_profit.lmtPrice, take_profit.tif, take_profit.parentId) == (52.0, "GTC", parent.orderId)
    assert (take_profit.ocaGroup, take_profit.ocaType, take_profit.transmit) == ("pos_1", 1, True)
    position = strategy.load_positions()[0]
    assert (position["stop_order_id"], position["exit_order_id"]) == (stop.orderId, take_profit.orderId)
    assert position["entry_atr"] == 4.0


def _tracked(con_id=1, exit_order_id=None):
    strategy.insert_position(
        {
            "con_id": con_id,
            "symbol": "AAA",
            "exchange": "SMART",
            "currency": "USD",
            "stop_order_id": 7,
            "exit_order_id": exit_order_id,
            "entry_atr": 4.0,
            "oca_group": f"pos_{con_id}",
        }
    )


async def test_sync_sends_take_profit_at_two_close_high(ib):
    _tracked()
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=7))]
    ib.reqPositionsAsync.return_value = [SimpleNamespace(contract=SimpleNamespace(conId=1), position=408.0)]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    (_, order), = ib.placed
    assert (order.action, order.orderType, order.tif) == ("SELL", "LMT", "GTC")
    assert (order.totalQuantity, order.lmtPrice, order.ocaGroup, order.ocaType) == (408, 52.0, "pos_1", 1)
    assert strategy.load_positions()[0]["exit_order_id"] == order.orderId


async def test_sync_clears_tracking_when_no_position(ib):
    _tracked()
    await strategy.sync_positions(ib)
    assert strategy.load_positions() == []
    assert ib.placed == []


async def test_run_once_does_nothing_on_holiday(ib):
    today = datetime.now(ZoneInfo("US/Eastern")).strftime("%Y%m%d")
    ib.reqContractDetailsAsync.return_value = [
        SimpleNamespace(timeZoneId="US/Eastern", tradingHours=f"{today}:CLOSED")
    ]
    await strategy.run_once(ib, universe=["AAA"])
    ib.qualifyContractsAsync.assert_not_called()
    assert ib.placed == []


def _open_market(ib):
    today = datetime.now(ZoneInfo("US/Eastern")).strftime("%Y%m%d")
    ib.reqContractDetailsAsync.return_value = [
        SimpleNamespace(timeZoneId="US/Eastern", tradingHours=f"{today}:0930-1600")
    ]


def _qualify_with_ids(*contracts):
    for i, contract in enumerate(contracts, start=100):
        contract.conId = i
    return list(contracts)


async def test_run_once_buys_lowest_volatility_signals_up_to_free_slots(ib, monkeypatch):
    _open_market(ib)
    monkeypatch.setattr(strategy, "get_vix_percentile_ok", lambda ib: asyncio.sleep(0, True))
    ib.qualifyContractsAsync.side_effect = _qualify_with_ids
    _tracked(con_id=1)  # una posición abierta: quedan 9 huecos
    ib.reqPositionsAsync.return_value = [_long()]
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=7))]

    def history(contract, **kwargs):
        if kwargs["durationStr"] != strategy.HISTORY_DURATION:
            return [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]  # take profit de la posición abierta
        index = int(contract.symbol[1:])  # cuanto mayor el índice, más volatilidad
        if index == 0:
            return flat_bars(300)  # sin señal
        noise = [bar(50, 51, 49, 50 + (i % 2) * 0.05 * index, day=i) for i in range(295)]
        return noise + _signal_bars(BREAKOUT, HOLD, HOLD, HOLD, PULLBACK_RED)[-5:]

    ib.reqHistoricalDataAsync.side_effect = history
    universe = [f"S{i}" for i in range(13)]

    await strategy.run_once(ib, universe=universe)

    bought = [contract.symbol for contract, order in ib.placed if order.orderType == "MKT"]
    assert bought == [f"S{i}" for i in range(1, 10)]
    assert len(strategy.load_positions()) == strategy.MAX_POSITIONS


async def test_run_once_does_not_scan_when_vix_blocks(ib, monkeypatch):
    _open_market(ib)
    monkeypatch.setattr(strategy, "get_vix_percentile_ok", lambda ib: asyncio.sleep(0, False))
    await strategy.run_once(ib, universe=["AAA"])
    ib.qualifyContractsAsync.assert_not_called()
    assert ib.placed == []


async def test_stop_stays_alive_across_sessions(ib):
    """El bot duerme durante la sesión y no reenvía el stop: debe ser GTC. Aceptación probada en demo."""
    await strategy.submit_entry_bracket(ib, SimpleNamespace(), _entry(), 4.0, 49.0, 52.0)
    _, stop = ib.placed[1]
    assert stop.tif == "GTC"


def test_exit_threshold_is_highest_of_last_two_closes():
    assert strategy.exit_threshold([bar(50, 51, 49, 47), bar(50, 51, 49, 52), bar(50, 51, 49, 50)]) == 52


def _long(qty=408.0):
    return SimpleNamespace(contract=SimpleNamespace(conId=1, symbol="AAA"), position=qty, avgCost=48.0)


async def test_finished_take_profit_gets_a_new_order_id(ib):
    """Reutilizar el id de una orden terminada da el error 103, probado en demo."""
    _tracked(exit_order_id=99)
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=7))]
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    (_, order), = ib.placed
    assert order.orderId != 99
    assert strategy.load_positions()[0]["exit_order_id"] == order.orderId


async def test_live_take_profit_is_modified_not_duplicated(ib):
    """El take profit vivo se modifica cada mañana: nunca se cancela ni se duplica."""
    _tracked(exit_order_id=99)
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=i)) for i in (7, 99)]
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    (_, order), = ib.placed
    assert order.orderId == 99


@pytest.mark.parametrize("tracked", [True, False], ids=["tracked", "untracked"])
async def test_short_position_is_never_sold_and_raises_alarm(ib, monkeypatch, tracked):
    events = []
    monkeypatch.setattr(strategy, "emit", lambda event, /, **fields: events.append((event, fields)))
    if tracked:
        _tracked()
    ib.reqPositionsAsync.return_value = [_long(-408.0)]

    await strategy.sync_positions(ib)

    assert ib.placed == []
    assert [e for e, _ in events] == ["position.short_detected"]
    assert events[0][1]["qty"] == -408.0
    assert len(strategy.load_positions()) == (1 if tracked else 0)


def _capture_events(monkeypatch):
    events = []
    monkeypatch.setattr(strategy, "emit", lambda event, /, **fields: events.append((event, fields)))
    return events


@pytest.fixture(autouse=True)
def _no_stop_check_wait(monkeypatch):
    monkeypatch.setattr(strategy, "STOP_CHECK_DELAY_S", 0)


async def test_replaced_stop_killed_by_ib_alerts_the_same_day(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]
    ib.placeOrder.side_effect = lambda contract, order: SimpleNamespace(
        orderStatus=SimpleNamespace(status="Cancelled")
    )

    await strategy.sync_positions(ib)

    assert [e for e, _ in events][-1] == "position.stop_missing"


async def test_missing_stop_not_replaced_if_all_orders_request_fails(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()
    monkeypatch.setattr(strategy, "OPEN_ORDERS_TIMEOUT_S", 0.01)

    async def never_answers():
        await asyncio.sleep(1)

    ib.reqAllOpenOrdersAsync.side_effect = never_answers
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    assert "position.stop_missing" in [e for e, _ in events]
    assert all(order.orderType != "STP" for _, order in ib.placed)


async def test_missing_stop_raises_alarm(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()  # stop_order_id=7, que no está vivo
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    names = [e for e, _ in events]
    assert "position.stop_replaced" in names and "position.stop_missing" not in names
    stop = next(order for _, order in ib.placed if order.orderType == "STP")
    assert (stop.action, stop.totalQuantity, stop.auxPrice, stop.tif) == ("SELL", 408, 44.0, "GTC")
    assert stop.parentId == 0
    assert stop.ocaGroup != "pos_1"  # IB no admite reutilizar un grupo terminado
    take_profit = next(order for _, order in ib.placed if order.orderType == "LMT")
    assert take_profit.ocaGroup == stop.ocaGroup  # si salta uno, se cancela el otro: nunca se vende de más
    saved = strategy.load_positions()[0]
    assert (saved["stop_order_id"], saved["oca_group"]) == (stop.orderId, stop.ocaGroup)


async def test_missing_stop_not_duplicated_if_another_sell_stop_is_live(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()
    other = SimpleNamespace(
        contract=SimpleNamespace(conId=1), order=SimpleNamespace(action="SELL", orderType="STP", orderId=55)
    )
    ib.reqAllOpenOrdersAsync.return_value = [other]
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    assert "position.stop_missing" in [e for e, _ in events]
    assert all(order.orderType != "STP" for _, order in ib.placed)


async def test_missing_stop_without_data_only_alerts(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()
    strategy.update_position(1, entry_atr=None)
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    assert "position.stop_missing" in [e for e, _ in events]
    assert all(order.orderType != "STP" for _, order in ib.placed)


async def test_unresponsive_ib_changes_nothing(ib, monkeypatch):
    """Sin lista de órdenes (Gateway sin conexión con IB) no se cancela, ni se repone, ni se limpia nada."""
    events = _capture_events(monkeypatch)
    _tracked()
    monkeypatch.setattr(strategy, "OPEN_ORDERS_TIMEOUT_S", 0.01)

    async def never_answers():
        await asyncio.sleep(1)

    ib.reqOpenOrdersAsync.side_effect = never_answers

    await strategy.sync_positions(ib)

    assert [e for e, _ in events] == ["positions.sync_skipped"]
    assert ib.placed == [] and not ib.cancelOrder.called
    assert len(strategy.load_positions()) == 1


async def test_live_stop_raises_no_alarm(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=7))]
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    assert "position.stop_missing" not in [e for e, _ in events]


async def test_orphan_stop_is_cancelled_when_position_closed(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    _tracked()
    stop = SimpleNamespace(orderId=7, parentId=6, orderType="STP")
    ib.openTrades.return_value = [SimpleNamespace(order=stop)]

    await strategy.sync_positions(ib)

    ib.cancelOrder.assert_called_once_with(stop)
    assert strategy.load_positions() == []
    assert "position.orphan_cancelled" in [e for e, _ in events]


async def test_orphan_take_profit_is_cancelled_too(ib):
    """Un límite de venta GTC sin posición abriría un corto al ejecutarse."""
    _tracked(exit_order_id=8)
    stop = SimpleNamespace(orderId=7, parentId=6, orderType="STP")
    take_profit = SimpleNamespace(orderId=8, parentId=6, orderType="LMT")
    ib.openTrades.return_value = [SimpleNamespace(order=stop), SimpleNamespace(order=take_profit)]

    await strategy.sync_positions(ib)

    assert [c.args[0] for c in ib.cancelOrder.call_args_list] == [stop, take_profit]


@pytest.mark.parametrize("old_status,alarm", [("Cancelled", False), ("Submitted", True)])
async def test_replaced_stop_moves_take_profit_to_its_new_group(ib, monkeypatch, old_status, alarm):
    """El take profit del grupo viejo se cancela: si no, quedaría fuera del OCA del stop nuevo."""
    events = _capture_events(monkeypatch)
    _tracked(exit_order_id=8)
    old_take_profit = SimpleNamespace(orderId=8, parentId=0, orderType="LMT")
    old_trade = SimpleNamespace(order=old_take_profit, orderStatus=SimpleNamespace(status=old_status))
    ib.openTrades.return_value = [old_trade]
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    ib.cancelOrder.assert_called_once_with(old_take_profit)
    assert ("position.exit_cancel_unconfirmed" in [e for e, _ in events]) is alarm
    stop = next(order for _, order in ib.placed if order.orderType == "STP")
    take_profit = next(order for _, order in ib.placed if order.orderType == "LMT")
    assert take_profit.orderId != 8 and take_profit.ocaGroup == stop.ocaGroup


async def test_stop_of_pending_entry_is_not_cancelled(ib):
    """Segunda ejecución el mismo día: aún no hay posición, pero la compra madre sigue viva."""
    _tracked()
    parent = SimpleNamespace(orderId=6, parentId=0)
    stop = SimpleNamespace(orderId=7, parentId=6)
    ib.openTrades.return_value = [SimpleNamespace(order=parent), SimpleNamespace(order=stop)]

    await strategy.sync_positions(ib)

    ib.cancelOrder.assert_not_called()
    assert len(strategy.load_positions()) == 1


async def test_untracked_long_raises_alarm(ib, monkeypatch):
    events = _capture_events(monkeypatch)
    ib.reqPositionsAsync.return_value = [_long()]

    await strategy.sync_positions(ib)

    assert [e for e, _ in events] == ["position.untracked_long"]
    assert ib.placed == []
