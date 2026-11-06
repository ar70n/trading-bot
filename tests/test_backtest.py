"""Reproducibilidad, ausencia de sesgo de anticipación y mecánica de ejecución del simulador."""

import asyncio
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from ib_async import Order

import backtest
import strategy
from backtest import SimIB, load_csv_market, run_backtest, synthetic_market

# Si cambia, ha cambiado el comportamiento de la estrategia o del simulador: no actualizar sin explicar por qué.
GOLDEN_SEED, GOLDEN_SYMBOLS, GOLDEN_DAYS = 42, 30, 800
GOLDEN_FINGERPRINT = "c48b646a30aeb59a"


@pytest.fixture(scope="module")
def golden_market():
    return synthetic_market(GOLDEN_SEED, GOLDEN_SYMBOLS, GOLDEN_DAYS)


def test_golden_fingerprint(golden_market):
    result = run_backtest(golden_market)
    assert result.fingerprint == GOLDEN_FINGERPRINT
    assert result.trades, "el escenario de referencia debe generar operaciones"


def test_same_input_same_output(golden_market):
    first, second = run_backtest(golden_market), run_backtest(golden_market)
    assert first == second


def test_different_seed_changes_result():
    assert synthetic_market(1, 3, 50) != synthetic_market(2, 3, 50)


def test_market_generation_is_deterministic():
    assert synthetic_market(1, 3, 50) == synthetic_market(1, 3, 50)


def test_no_lookahead(golden_market):
    """Alterar el futuro no puede cambiar las operaciones ya cerradas antes de ese punto."""
    cutoff = 600
    truncated = {s: bars[:cutoff] for s, bars in golden_market.items()}
    full, partial = run_backtest(golden_market), run_backtest(truncated)
    assert partial.trades
    assert partial.trades == full.trades[: len(partial.trades)]
    assert partial.equity_curve == full.equity_curve[: len(partial.equity_curve)]


def test_real_state_db_is_never_used(golden_market, monkeypatch):
    sentinel = "/nonexistent/real.db"
    monkeypatch.setattr(strategy, "DB_PATH", sentinel)
    run_backtest({s: bars[:300] for s, bars in golden_market.items()})
    assert strategy.DB_PATH == sentinel


def test_cli_writes_report(tmp_path, capsys):
    out = tmp_path / "run.json"
    code = backtest.main(["--synthetic", "--seed", "1", "--symbols", "3", "--days", "280", "--out", str(out)])
    assert code == 0
    assert "fingerprint" in json.loads(capsys.readouterr().out)
    assert json.loads(out.read_text())["initial_equity"] == backtest.INITIAL_EQUITY


def test_csv_market_round_trip(tmp_path: Path):
    header = "date,open,high,low,close,volume\n"
    (tmp_path / "AAA.csv").write_text(header + "2026-01-02,2,3,1,2,10\n2026-01-01,1,2,1,1,10\n")
    (tmp_path / "VIX.csv").write_text(header + "2026-01-01,15,15,15,15,0\n")
    market = load_csv_market(tmp_path)
    assert [b.date for b in market["AAA"]] == [date(2026, 1, 1), date(2026, 1, 2)]


def test_csv_requires_vix(tmp_path: Path):
    (tmp_path / "AAA.csv").write_text("date,open,high,low,close,volume\n")
    with pytest.raises(ValueError):
        load_csv_market(tmp_path)


def _sim(*bars):
    """Dos sesiones de historia y luego `bars` (open, high, low, close)."""
    start = date(2026, 1, 1)
    rows = [(50, 51, 49, 50), (50, 51, 49, 50), *bars]
    make = backtest._bar
    series = [make(start + timedelta(days=i), *row, 1000) for i, row in enumerate(rows)]
    vix = [make(start + timedelta(days=i), 15, 15, 15, 15, 0) for i in range(len(rows))]
    sim = SimIB({"AAA": series, "VIX": vix}, equity=10_000)
    sim.today_index = 2
    return sim


def _order(order_id, kind, tif="", parent=0, oca="g", **prices):
    order = Order(orderId=order_id, action="SELL", orderType=kind, totalQuantity=10, tif=tif, parentId=parent)
    order.ocaGroup = oca
    for name, value in prices.items():
        setattr(order, name, value)
    return order


def _contract():
    return SimpleNamespace(conId=1)


def _hold(sim):
    sim.positions[1] = {"qty": 10, "entry_price": 50.0, "entry_date": "2026-01-01"}


def test_moo_fills_at_open_and_activates_child():
    sim = _sim((52, 53, 51, 52))
    parent = Order(orderId=1, action="BUY", orderType="MKT", totalQuantity=10, tif="OPG")
    sim.placeOrder(_contract(), parent)
    sim.placeOrder(_contract(), _order(2, "STP", tif="GTC", parent=1, auxPrice=40.0))

    sim.simulate_session()

    assert sim.positions[1]["entry_price"] == 52
    assert sim.orders[2]["active"] is True
    assert sim.cash == 10_000 - 520


def test_stop_gap_fills_at_open_not_at_stop():
    sim = _sim((40, 41, 39, 40))
    _hold(sim)
    sim.placeOrder(_contract(), _order(2, "STP", tif="GTC", auxPrice=45.0))
    sim.simulate_session()
    assert sim.trades[0].exit_price == 40 and sim.trades[0].exit_reason == "stop"


def test_stop_fills_at_stop_price_without_gap():
    sim = _sim((50, 51, 44, 46))
    _hold(sim)
    sim.placeOrder(_contract(), _order(2, "STP", tif="GTC", auxPrice=45.0))
    sim.simulate_session()
    assert sim.trades[0].exit_price == 45


@pytest.mark.parametrize(
    "bar_,exit_price",
    [((50, 56, 49, 53), 54.0), ((57, 58, 56, 57), 57), ((50, 53, 49, 52), None)],
    ids=["touched_intraday", "gap_above_fills_at_open", "not_reached"],
)
def test_take_profit_fills_when_high_reaches_limit(bar_, exit_price):
    sim = _sim(bar_)
    _hold(sim)
    sim.placeOrder(_contract(), _order(3, "LMT", tif="GTC", lmtPrice=54.0))
    sim.simulate_session()
    assert [(t.exit_price, t.exit_reason) for t in sim.trades] == ([(exit_price, "take_profit")] if exit_price else [])


def test_oca_cancels_sibling():
    sim = _sim((50, 51, 44, 55))
    _hold(sim)
    sim.placeOrder(_contract(), _order(2, "STP", tif="GTC", auxPrice=45.0))
    sim.placeOrder(_contract(), _order(3, "LMT", tif="GTC", lmtPrice=54.0))
    sim.simulate_session()
    assert len(sim.trades) == 1 and sim.trades[0].exit_reason == "stop"  # mismo día: el stop, por prudencia
    assert sim.orders == {}


def test_day_orders_expire_and_position_counts_as_unprotected():
    sim = _sim((50, 51, 49, 50), (50, 51, 30, 35))
    _hold(sim)
    sim.placeOrder(_contract(), _order(2, "STP", tif="", auxPrice=45.0))

    sim.simulate_session()
    assert sim.orders == {} and sim.unprotected_days == 0

    sim.today_index = 3
    sim.simulate_session()
    assert sim.trades == []  # el precio cae a 30 sin stop vivo
    assert sim.unprotected_days == 1


def test_history_never_includes_today():
    sim = _sim((60, 61, 59, 60))
    bars = asyncio.run(sim.reqHistoricalDataAsync(SimpleNamespace(symbol="AAA", conId=1), durationStr="5 D"))
    assert [b.close for b in bars] == [50, 50]


def test_every_position_keeps_a_live_stop(golden_market):
    assert run_backtest(golden_market).unprotected_position_days == 0


def test_exits_are_accepted_and_used(golden_market):
    result = run_backtest(golden_market)
    assert result.rejected_orders == 0
    assert {t.exit_reason for t in result.trades} == {"stop", "take_profit"}


def test_simulator_rejects_reused_order_id():
    sim = _sim((50, 56, 49, 55))
    order = _order(4, "LMT", tif="GTC", lmtPrice=54.0)
    sim.placeOrder(_contract(), order)
    del sim.orders[4]
    sim.placeOrder(_contract(), order)  # id de una orden ya terminada: IB da el error 103
    assert sim.rejected_orders == 1 and sim.orders == {}


def test_modifying_an_active_child_keeps_it_active():
    sim = _sim((50, 56, 49, 53))
    _hold(sim)
    child = _order(3, "LMT", tif="GTC", lmtPrice=60.0)
    sim.placeOrder(_contract(), child)
    sim.orders[3]["active"] = True
    child.parentId = 2
    child.lmtPrice = 54.0
    sim.placeOrder(_contract(), child)
    sim.simulate_session()
    assert [t.exit_reason for t in sim.trades] == ["take_profit"]
