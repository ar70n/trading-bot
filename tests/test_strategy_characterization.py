"""Fija el comportamiento ACTUAL de la estrategia. Si uno de estos tests cambia, ha cambiado la estrategia:
eso requiere argumentación y confirmación explícita del usuario (ver CLAUDE.md). Fijar no significa aprobar:
los tests `test_known_issue_*` documentan defectos conocidos pendientes de decisión.
"""

import asyncio
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import strategy
from ib_mocks import bar, flat_bars, scanner_row


def _entry(**overrides):
    base = {"con_id": 1, "symbol": "AAA", "exchange": "SMART", "currency": "USD", "phase": "breakout_wait"}
    return {**base, **overrides}


def test_parameters_are_pinned():
    assert (strategy.MAX_POSITIONS, strategy.MARGIN_PCT, strategy.POSITION_SIZE_PCT_OF_EQUITY) == (10, 50, 20.0)
    assert (strategy.HH_LOOKBACK_DAYS, strategy.ATR_PERIOD, strategy.ATR_MULTIPLIER) == (63, 20, 2)
    assert (strategy.BREAKOUT_WINDOW_DAYS, strategy.HV_LOOKBACK_DAYS) == (10, 252)
    assert (strategy.MIN_DOLLAR_VOLUME, strategy.VIX_LOOKBACK_DAYS, strategy.VIX_PERCENTILE_MAX) == (5_000_000, 100, 85)


def test_atr_on_flat_bars():
    assert strategy.get_atr(flat_bars(50)) == pytest.approx(2.0)
    assert strategy.get_atr(flat_bars(20)) is None


def test_rolling_high_excludes_last_bar():
    bars = flat_bars(70)
    bars[-1] = bar(50, 99, 49, 50)
    assert strategy.get_rolling_hh(bars) == 51
    assert strategy.get_rolling_hh(flat_bars(63)) is None


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


async def test_breakout_arms_entry_level(ib):
    bars = flat_bars(100)
    bars[-1] = bar(52, 60, 55, 58)
    ib.reqHistoricalDataAsync.return_value = bars
    strategy.upsert_watch_entry(_entry())

    await strategy.update_watch_entry(ib, strategy.load_watchlist()[0], True)

    row = strategy.load_watchlist()[0]
    assert row["phase"] == "entry_wait"
    assert row["limit_buy"] == pytest.approx(55 - 2 * 2)
    assert ib.placed == []


async def test_pullback_red_candle_submits_bracket(ib):
    bars = flat_bars(100)
    bars[-1] = bar(50, 50.5, 48, 49)
    ib.reqHistoricalDataAsync.return_value = bars
    strategy.upsert_watch_entry(_entry(phase="entry_wait", limit_buy=51.0))

    await strategy.update_watch_entry(ib, strategy.load_watchlist()[0], True)

    assert strategy.load_watchlist() == []
    (_, parent), (_, stop) = ib.placed
    assert (parent.action, parent.orderType, parent.tif, parent.transmit) == ("BUY", "MKT", "OPG", False)
    assert parent.totalQuantity == 408  # 100.000 * 20% // 49
    assert (stop.action, stop.orderType, stop.totalQuantity, stop.auxPrice) == ("SELL", "STP", 408, 45.0)
    assert (stop.parentId, stop.ocaGroup, stop.ocaType, stop.transmit) == (parent.orderId, "pos_1", 1, True)
    position = strategy.load_positions()[0]
    assert (position["stop_order_id"], position["entry_atr"]) == (stop.orderId, 4.0)


@pytest.mark.parametrize(
    "last,vix_ok",
    [(bar(49, 50.5, 48, 50), True), (bar(50, 50.5, 48, 49), False)],
    ids=["green_candle", "vix_blocks"],
)
async def test_touch_without_conditions_discards(ib, last, vix_ok):
    bars = flat_bars(100)
    bars[-1] = last
    ib.reqHistoricalDataAsync.return_value = bars
    strategy.upsert_watch_entry(_entry(phase="entry_wait", limit_buy=51.0))

    await strategy.update_watch_entry(ib, strategy.load_watchlist()[0], vix_ok)

    assert ib.placed == []
    assert strategy.load_watchlist() == []


async def test_entry_window_expires(ib):
    ib.reqHistoricalDataAsync.return_value = flat_bars(100)
    strategy.upsert_watch_entry(_entry(phase="entry_wait", limit_buy=10.0, days_in_window=8))

    await strategy.update_watch_entry(ib, strategy.load_watchlist()[0], True)
    assert strategy.load_watchlist()[0]["days_in_window"] == 9

    await strategy.update_watch_entry(ib, strategy.load_watchlist()[0], True)
    assert strategy.load_watchlist() == []


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


async def test_sync_sends_loc_at_two_close_high(ib):
    _tracked()
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=7))]
    ib.reqPositionsAsync.return_value = [SimpleNamespace(contract=SimpleNamespace(conId=1), position=408.0)]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    (_, order), = ib.placed
    assert (order.action, order.orderType, order.tif) == ("SELL", "LOC", "DAY")
    assert (order.totalQuantity, order.lmtPrice, order.ocaGroup) == (408, 52.0, "pos_1")
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
    await strategy.run_once(ib)
    ib.reqScannerDataAsync.assert_not_called()
    assert ib.placed == []


async def test_run_once_fills_free_slots_ranked_by_low_volatility(ib):
    today = datetime.now(ZoneInfo("US/Eastern")).strftime("%Y%m%d")
    ib.reqContractDetailsAsync.return_value = [
        SimpleNamespace(timeZoneId="US/Eastern", tradingHours=f"{today}:0930-1600")
    ]
    ib.reqScannerDataAsync.return_value = [scanner_row(i, f"S{i}") for i in range(1, 13)]
    wiggly = [bar(50, 51, 49, 50 + (i % 2) * 0.5, day=i) for i in range(300)]
    ib.reqHistoricalDataAsync.return_value = wiggly

    await strategy.run_once(ib)

    watch = strategy.load_watchlist()
    assert len(watch) == strategy.MAX_POSITIONS  # la watchlist ocupa huecos igual que una posición
    assert {w["phase"] for w in watch} == {"breakout_wait"}


async def test_stop_stays_alive_across_sessions(ib):
    """El bot duerme durante la sesión y no reenvía el stop: debe ser GTC. Aceptación probada en demo."""
    await strategy.submit_entry_bracket(ib, SimpleNamespace(), _entry(), 4.0, 49.0)
    _, stop = ib.placed[1]
    assert stop.tif == "GTC"


def test_known_issue_loc_uses_oca_type_rejected_by_ib():
    """DEFECTO CONOCIDO: IB rechaza una LOC con ocaType 1 (error 201). El tipo 3 está vetado hasta tener pruebas."""
    assert strategy.build_loc_order("SELL", 1, 10.0, "pos_1").ocaType == 1


def _long(qty=408.0):
    return SimpleNamespace(contract=SimpleNamespace(conId=1, symbol="AAA"), position=qty, avgCost=48.0)


async def test_finished_loc_gets_a_new_order_id(ib):
    """Reutilizar el id de una orden terminada da el error 103, probado en demo."""
    _tracked(exit_order_id=99)
    ib.openTrades.return_value = [SimpleNamespace(order=SimpleNamespace(orderId=7))]
    ib.reqPositionsAsync.return_value = [_long()]
    ib.reqHistoricalDataAsync.return_value = [bar(50, 51, 49, 52), bar(50, 51, 49, 50)]

    await strategy.sync_positions(ib)

    (_, order), = ib.placed
    assert order.orderId != 99
    assert strategy.load_positions()[0]["exit_order_id"] == order.orderId


async def test_live_loc_is_modified_not_duplicated(ib):
    """Una segunda ejecución el mismo día no debe dejar dos LOC vivas."""
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


async def test_known_issue_breakout_wait_never_expires(ib):
    """DEFECTO CONOCIDO, pendiente de decisión: sin ruptura, un candidato ocupa hueco indefinidamente."""
    ib.reqHistoricalDataAsync.return_value = flat_bars(100)
    strategy.upsert_watch_entry(_entry())
    for _ in range(50):
        await strategy.update_watch_entry(ib, strategy.load_watchlist()[0], True)
    assert strategy.load_watchlist()[0]["phase"] == "breakout_wait"


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
    loc = next(order for _, order in ib.placed if order.orderType == "LOC")
    assert loc.ocaGroup == stop.ocaGroup  # si salta uno, se cancela el otro: nunca se vende de más
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
    stop = SimpleNamespace(orderId=7, parentId=6)
    ib.openTrades.return_value = [SimpleNamespace(order=stop)]

    await strategy.sync_positions(ib)

    ib.cancelOrder.assert_called_once_with(stop)
    assert strategy.load_positions() == []
    assert "position.orphan_stop_cancelled" in [e for e, _ in events]


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
