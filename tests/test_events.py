"""Formato JSON lines, rotación, redacción y resumen diario."""

import json
import logging
import os
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from ib_async import Order

from events import emit, emit_daily_summary, mask_account, setup_logging, timed
from ib_mocks import make_mock_ib
from ib_observer import ObservedIB, reconcile_at_startup


@pytest.fixture
def log_path(tmp_path):
    path = setup_logging(tmp_path)
    yield path
    root = logging.getLogger()
    for handler in list(root.handlers):
        if str(tmp_path) in getattr(handler, "baseFilename", ""):
            root.removeHandler(handler)
            handler.close()


def read(path):
    for handler in logging.getLogger().handlers:
        handler.flush()
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_every_line_has_the_contract(log_path):
    emit("signal.entry", symbol="AAA", close=1.5)
    logging.getLogger("strategy").info("texto %s", "libre")
    first, second = read(log_path)
    assert first["event"] == "signal.entry" and first["symbol"] == "AAA" and first["close"] == 1.5
    assert first["run_id"] == second["run_id"] and first["schema"] == 1
    assert datetime.fromisoformat(first["ts"]).tzinfo == UTC
    assert (second["event"], second["message"], second["logger"]) == ("log", "texto libre", "strategy")


def test_hostile_values_still_produce_valid_json(log_path):
    emit(
        "x",
        nan=float("nan"),
        inf=float("inf"),
        obj=object(),
        when=datetime(2026, 1, 1),
        text='a"b\nc',
        ts="dup",
        event="dup",
    )
    line = read(log_path)[0]
    assert line["nan"] is None and line["inf"] is None
    assert line["text"] == 'a"b\nc'
    assert line["event"] == "x" and line["field_event"] == "dup" and line["field_ts"] == "dup"


def test_secrets_and_accounts_are_redacted(log_path):
    emit("x", nested={"password": "hunter2", "api_key": "k", "ok": 1}, accounts=["DU1234567"], note="cuenta DU1234567")
    logging.getLogger("y").info("cuenta %s", "DU1234567")
    raw = log_path.read_text()
    assert "hunter2" not in raw and "1234567" not in raw
    assert read(log_path)[0]["nested"]["ok"] == 1
    assert mask_account("DU1234567") == "DU***567"


def test_exception_is_structured(log_path):
    try:
        raise ValueError("boom")
    except ValueError:
        emit("run.failed", level=logging.ERROR, exc_info=True)
    line = read(log_path)[0]
    assert line["exc_type"] == "ValueError" and "boom" in line["traceback"]


def test_timed_records_failures_and_reraises(log_path):
    with pytest.raises(ValueError), timed("op"):
        raise ValueError
    line = read(log_path)[0]
    assert (line["event"], line["op"], line["ok"]) == ("latency", "op", False)


def test_rotates_when_day_changes_across_process_restarts(tmp_path):
    """Caso real: el proceso vive minutos y arranca un día nuevo; debe rotar lo de ayer."""
    path = setup_logging(tmp_path)
    emit("ayer")
    logging.shutdown()
    two_days_ago = time.time() - 2 * 86400
    os.utime(path, (two_days_ago, two_days_ago))

    path = setup_logging(tmp_path)
    emit("hoy")
    for handler in logging.getLogger().handlers:
        handler.flush()

    rotated = list(tmp_path.glob("events.jsonl.*"))
    assert len(rotated) == 1
    assert json.loads(rotated[0].read_text())["event"] == "ayer"
    assert read(path)[0]["event"] == "hoy"


def test_daily_summary_aggregates(log_path):
    emit("signal.entry", symbol="A")
    emit("signal.breakout", symbol="B")
    emit("order.submitted", symbol="A")
    emit("fill", symbol="A")
    emit("ib.error", level=logging.ERROR, code=201)
    with timed("ib.op"):
        pass

    summary = emit_daily_summary("ok", exit_code=0)

    assert (summary["signals"], summary["orders_submitted"], summary["fills"], summary["errors"]) == (2, 1, 1, 1)
    assert summary["latency"]["ib.op"]["n"] == 1
    assert summary["error_samples"][0]["code"] == 201
    assert read(log_path)[-1]["event"] == "daily.summary"


def _trade():
    return SimpleNamespace(
        contract=SimpleNamespace(symbol="AAA"),
        order=SimpleNamespace(orderId=5, orderType="MKT"),
        orderStatus=SimpleNamespace(status="Filled", filled=10.0, remaining=0.0, avgFillPrice=1.5),
    )


def _fill():
    return SimpleNamespace(
        contract=SimpleNamespace(symbol="AAA"),
        execution=SimpleNamespace(
            execId="e1", orderId=5, side="BOT", shares=10.0, price=1.5, time=datetime(2026, 1, 1, tzinfo=UTC)
        ),
        commissionReport=SimpleNamespace(commission=1.0, realizedPNL=0.0),
    )


async def test_observer_logs_order_lifecycle(log_path):
    ib = make_mock_ib()
    observed = ObservedIB(ib)
    order = Order(orderId=5, action="BUY", orderType="MKT", totalQuantity=10)

    observed.placeOrder(SimpleNamespace(symbol="AAA", conId=1), order)
    ib.orderStatusEvent.emit(_trade())
    ib.execDetailsEvent.emit(_trade(), _fill())
    ib.errorEvent.emit(5, 201, "rechazada", None)
    ib.errorEvent.emit(-1, 2104, "granja ok", None)
    ib.disconnectedEvent.emit()
    await observed.reqPositionsAsync()

    lines = read(log_path)
    by_event = {line["event"]: line for line in lines}
    assert by_event["order.submitted"]["order_id"] == 5
    assert by_event["order.status"]["status"] == "Filled"
    assert by_event["fill"]["exec_id"] == "e1" and by_event["fill"]["source"] == "live"
    assert "connection.disconnected" in by_event
    levels = {line["code"]: line["level"] for line in lines if line["event"] == "ib.error"}
    assert levels == {201: "ERROR", 2104: "INFO"}
    assert any(line.get("op") == "ib.reqPositionsAsync" for line in lines)
    assert ib.placed[0][1] is order


def test_reconcile_reports_overnight_activity(log_path):
    ib = make_mock_ib()
    ib.fills.return_value = [_fill()]
    ib.trades.return_value = [_trade()]
    contract = SimpleNamespace(symbol="AAA", conId=1)
    ib.positions.return_value = [SimpleNamespace(contract=contract, position=10.0, avgCost=1.5)]

    reconcile_at_startup(ib)

    by_event = {line["event"]: line for line in read(log_path)}
    assert by_event["fill"]["source"] == "reconcile"
    assert by_event["order.reconciled"]["status"] == "Filled"
    assert by_event["positions.snapshot"]["count"] == 1


@pytest.mark.parametrize(
    "account,expected",
    [("DU1234567", "DU***567"), ("DUR123456", "DUR***456"), ("DF1234567", "DF***567"), ("D1234567", "D***567")],
)
def test_account_formats_are_masked(account, expected):
    assert mask_account(f"cuenta {account}.") == f"cuenta {expected}."


def test_ordinary_words_are_not_masked():
    assert mask_account("DATA order 1234567 USD") == "DATA order 1234567 USD"
