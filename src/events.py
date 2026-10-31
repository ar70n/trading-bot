"""Logging estructurado en JSON lines, pensado para que lo lea un agente.

Archivo: log/events.jsonl, rotado a medianoche UTC a log/events.jsonl.AAAA-MM-DD.
Cada línea es un objeto con: ts (ISO-8601 UTC), level, event, run_id, schema y los campos del evento.
El catálogo de eventos está documentado en CLAUDE.md.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import statistics
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
BACKUP_DAYS = 90
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[1] / "log"
LOG_FILE = "events.jsonl"

_RESERVED = {"ts", "level", "event", "run_id", "schema"}
_SECRET_KEY = re.compile(r"pass|secret|token|credential|api_?key", re.I)
_ACCOUNT_ID = re.compile(r"\b([DU][A-Z]{0,3})\d{3,6}(\d{3})\b")

_logger = logging.getLogger("tradingbot.events")


def mask_account(text: str) -> str:
    """Conserva el prefijo (distingue demo de real) y los 3 últimos dígitos."""
    return _ACCOUNT_ID.sub(r"\1***\2", text)


def _clean(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(k): "***" if _SECRET_KEY.search(str(k)) else _clean(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [_clean(v) for v in value]
    if isinstance(value, bool | int | float) or value is None:
        return value
    return mask_account(str(value))


class JsonLinesFormatter(logging.Formatter):
    """Una línea JSON válida por registro, incluso con valores no serializables o NaN."""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "fields", None)
        line: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "event": getattr(record, "event", "log"),
            "run_id": self.run_id,
            "schema": SCHEMA_VERSION,
        }
        if fields is None:
            line["logger"] = record.name
            line["message"] = mask_account(record.getMessage())
        else:
            for key, value in _clean(fields).items():
                line[f"field_{key}" if key in _RESERVED else key] = value
        if record.exc_info and record.exc_info[0] is not None:
            line["exc_type"] = record.exc_info[0].__name__
            line["traceback"] = mask_account(self.formatException(record.exc_info))
        return json.dumps(line, ensure_ascii=False, allow_nan=False)


class RunStats:
    """Agregados de la ejecución en curso; alimentan daily.summary."""

    def __init__(self) -> None:
        self.run_id = uuid.uuid4().hex[:12]
        self.started = time.monotonic()
        self.counts: Counter[str] = Counter()
        self.latencies: defaultdict[str, list[float]] = defaultdict(list)
        self.errors: list[dict[str, Any]] = []


_stats = RunStats()


def setup_logging(log_dir: str | Path | None = None, backup_days: int = BACKUP_DAYS) -> Path:
    """Configura el logger raíz para escribir JSON lines. Idempotente; inicia una ejecución nueva."""
    global _stats
    _stats = RunStats()
    directory = Path(log_dir or os.environ.get("TRADINGBOT_LOG_DIR") or DEFAULT_LOG_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / LOG_FILE

    handler = TimedRotatingFileHandler(path, when="midnight", utc=True, backupCount=backup_days, encoding="utf-8")
    handler.setFormatter(JsonLinesFormatter(_stats.run_id))

    root = logging.getLogger()
    for old in list(root.handlers):
        if isinstance(old, TimedRotatingFileHandler):
            root.removeHandler(old)
            old.close()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    logging.getLogger("ib_async").setLevel(logging.WARNING)  # ib_async es muy verboso en INFO
    return path


def emit(event: str, /, level: int = logging.INFO, exc_info: bool = False, **fields: Any) -> None:
    """Registra un evento. Nunca lanza: un fallo de logging no debe interrumpir el trading."""
    _stats.counts[event] += 1
    if level >= logging.ERROR:
        _stats.errors.append({"event": event, **{k: _clean(v) for k, v in fields.items()}})
    _logger.log(level, event, exc_info=exc_info, extra={"event": event, "fields": fields})


@contextmanager
def timed(op: str, /, **fields: Any) -> Iterator[None]:
    """Emite un evento `latency` con la duración y el resultado de la operación."""
    start = time.perf_counter()
    ok = True
    try:
        yield
    except BaseException:
        ok = False
        raise
    finally:
        ms = round((time.perf_counter() - start) * 1000, 1)
        _stats.latencies[op].append(ms)
        emit("latency", op=op, ms=ms, ok=ok, **fields)


def _latency_summary() -> dict[str, dict[str, float]]:
    summary = {}
    for op, samples in sorted(_stats.latencies.items()):
        summary[op] = {
            "n": len(samples),
            "p50_ms": statistics.median(samples),
            "max_ms": max(samples),
            "total_ms": round(sum(samples), 1),
        }
    return summary


def emit_daily_summary(status: str, **fields: Any) -> dict[str, Any]:
    """Resumen de la ejecución. `status`: ok | refused_not_demo | connection_failed | interrupted | error."""
    counts = dict(sorted(_stats.counts.items()))
    summary = {
        "status": status,
        "duration_s": round(time.monotonic() - _stats.started, 1),
        "signals": sum(n for name, n in counts.items() if name.startswith("signal.")),
        "orders_submitted": counts.get("order.submitted", 0),
        "fills": counts.get("fill", 0),
        "errors": len(_stats.errors),
        "error_samples": _stats.errors[:20],
        "event_counts": counts,
        "latency": _latency_summary(),
        **fields,
    }
    emit("daily.summary", **summary)
    return summary
