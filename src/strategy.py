"""
strategy.py

Implementación de la estrategia "Systematic market structure trading
strategy" (Critical Trading — https://youtu.be/QKCDt2QnmeM,
https://youtu.be/rKGXIopVeQ0 — Critical-Trading.com), adaptada de AFL
(AmiBroker) a un job diario sobre ib_async.

DISEÑO: este job está pensado para ejecutarse UNA VEZ AL DÍA, POR LA
MAÑANA, ANTES DE LA APERTURA del mercado (p. ej. 8:00 hora del exchange,
vía cron). Con los datos hasta el cierre de ayer:

  - Las señales de entrada se mandan como orden Market-on-Open (tif='OPG'):
    ejecutan en la apertura de HOY, sin que el bot esté despierto en ese
    instante.
  - El stop-loss se manda como orden hija del MOO en el mismo envío
    (bracket), usando como precio de referencia el cierre del día en que
    se disparó la señal (proxy razonable del precio de apertura de hoy,
    ya que el ATR es una distancia fija, no algo anclado al precio exacto
    de fill).
  - La regla de salida (Sell = C > Ref(HHV(C,2),-1)) se implementa con una
    orden Limit-on-Close (LOC): el umbral (máximo de los 2 cierres más
    recientes ya conocidos) se calcula por la mañana y se envía/actualiza
    como límite de una LOC para el cierre de HOY. IB resuelve la condición
    "vender si el cierre supera X" en la subasta de cierre sin intervención
    del bot.

Con este horario (mañana, no tras el cierre) tanto la entrada como la
salida se ejecutan el mismo día que la señal lo indica, aceptando el pequeño
margen de error frente al gap de apertura real.

El stop-loss y la LOC de salida comparten grupo OCA (One-Cancels-All): si
salta uno se cancela automáticamente el otro.
"""

import asyncio
import logging
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from ib_async import IB, Index, MarketOrder, Order, OrderStatus, ScannerSubscription, Stock, StopOrder, TagValue

from demo_guard import assert_session_is_demo
from events import emit

logger = logging.getLogger("strategy")

DB_PATH = "/home/bot/tradingbot/trading_state.db"

# --- Parámetros de la estrategia (AFL -> Python) ---------------------------
MAX_POSITIONS = 10
MARGIN_PCT = 50  # 50 = margen estándar acciones US (2x), 100 = sin margen
POSITION_SIZE_PCT_OF_EQUITY = (200 if MARGIN_PCT == 50 else 100) / MAX_POSITIONS

HH_LOOKBACK_DAYS = 21 * 3   # 63 sesiones
ATR_PERIOD = 20
ATR_MULTIPLIER = 2
BREAKOUT_WINDOW_DAYS = 10
HV_LOOKBACK_DAYS = 252
OPEN_ORDERS_TIMEOUT_S = 15
STOP_CHECK_DELAY_S = 5
OCA_TYPE = 1  # IB rechaza la LOC con este tipo (error 201). El tipo 3 está pendiente de decisión: ver CLAUDE.md
MIN_DOLLAR_VOLUME = 5_000_000 # $ diarios mínimos (precio x volumen), media de 20 días

VIX_LOOKBACK_DAYS = 100
VIX_PERCENTILE_MAX = 85


# ---------------------------------------------------------------------------
# Persistencia (SQLite)
# ---------------------------------------------------------------------------

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                con_id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                currency TEXT NOT NULL,
                phase TEXT NOT NULL,          -- 'breakout_wait' | 'entry_wait'
                limit_buy REAL,
                days_in_window INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                con_id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                currency TEXT NOT NULL,
                stop_order_id INTEGER,
                exit_order_id INTEGER,
                entry_atr REAL,
                oca_group TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def load_watchlist():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM watchlist").fetchall()
        return [dict(r) for r in rows]


def upsert_watch_entry(entry: dict):
    entry.setdefault("limit_buy", None)
    entry.setdefault("days_in_window", 0)
    now = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO watchlist
                (con_id, symbol, exchange, currency, phase, limit_buy, days_in_window, created_at, updated_at)
            VALUES
                (:con_id, :symbol, :exchange, :currency, :phase, :limit_buy, :days_in_window, :now, :now)
            ON CONFLICT(con_id) DO UPDATE SET
                phase = excluded.phase,
                limit_buy = excluded.limit_buy,
                days_in_window = excluded.days_in_window,
                updated_at = excluded.updated_at
            """,
            {**entry, "now": now},
        )
        conn.commit()


def remove_watch_entry(con_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM watchlist WHERE con_id = ?", (con_id,))
        conn.commit()


def load_positions():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM positions").fetchall()
        return [dict(r) for r in rows]


def insert_position(pos: dict):
    now = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO positions
                (con_id, symbol, exchange, currency, stop_order_id, exit_order_id,
                 entry_atr, oca_group, created_at, updated_at)
            VALUES
                (:con_id, :symbol, :exchange, :currency, :stop_order_id, :exit_order_id,
                 :entry_atr, :oca_group, :now, :now)
            """,
            {**pos, "now": now},
        )
        conn.commit()


def update_position(con_id: int, **fields):
    fields["updated_at"] = datetime.now(UTC).isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            f"UPDATE positions SET {set_clause} WHERE con_id = :con_id",
            {**fields, "con_id": con_id},
        )
        conn.commit()


def remove_position(con_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM positions WHERE con_id = ?", (con_id,))
        conn.commit()


# ---------------------------------------------------------------------------
# Indicadores
# ---------------------------------------------------------------------------

def get_atr(bars, period=ATR_PERIOD):
    """Wilder's ATR (sin multiplicar). Síncrona: no hace IO."""
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        high, low, prev_close = bars[i].high, bars[i].low, bars[i - 1].close
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if len(trs) <= period:
        return sum(trs) / len(trs)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def get_rolling_hh(bars, lookback=HH_LOOKBACK_DAYS):
    """Máximo de 'high' de los `lookback` días ANTERIORES a la última barra
    (equivalente a Ref(HHV(H, lookback), -1) en AFL)."""
    if len(bars) < lookback + 1:
        return None
    prior_bars = bars[:-1]
    return max(b.high for b in prior_bars[-lookback:])


async def compute_annualized_hv(ib: IB, contract):
    """HV = StDev(ROC(C,1), 252) * sqrt(252)."""
    bars = await ib.reqHistoricalDataAsync(
        contract, endDateTime="", durationStr="2 Y", barSizeSetting="1 day",
        whatToShow="TRADES", useRTH=True, formatDate=1,
    )
    if not bars or len(bars) < HV_LOOKBACK_DAYS + 1:
        return None
    # Check wheter volume is sufficient (average dollar volume over last 20 days)
    recent = bars[-20:]
    avg_dollar_volume = sum(b.close * b.volume for b in recent) / len(recent)
    if avg_dollar_volume < MIN_DOLLAR_VOLUME:
        return None

    closes = [b.close for b in bars[-(HV_LOOKBACK_DAYS + 1):]]
    returns = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return (variance ** 0.5) * (252 ** 0.5)


async def get_vix_percentile_ok(ib: IB):
    """VIXfilter = PercentRank(C, 100) <= 85."""
    vix = Index("VIX", "CBOE")
    await ib.qualifyContractsAsync(vix)
    bars = await ib.reqHistoricalDataAsync(
        vix, endDateTime="", durationStr="150 D", barSizeSetting="1 day",
        whatToShow="TRADES", useRTH=True, formatDate=1,
    )
    if len(bars) < VIX_LOOKBACK_DAYS:
        logger.warning("Histórico de VIX insuficiente para el percentil; se asume filtro NO superado")
        return False
    window = [b.close for b in bars[-VIX_LOOKBACK_DAYS:]]
    current = window[-1]
    rank = sum(1 for v in window if v <= current) / len(window) * 100
    ok = rank <= VIX_PERCENTILE_MAX
    emit("vix.filter", percentile=rank, max_percentile=VIX_PERCENTILE_MAX, entries_allowed=ok)
    logger.info("VIX percentil (%d días) = %.1f%% -> %s", VIX_LOOKBACK_DAYS, rank,
                "permite nuevas entradas" if ok else "bloquea nuevas entradas")
    return ok


async def get_equity(ib: IB):
    summary = await ib.accountSummaryAsync()
    for val in summary:
        if val.tag == "NetLiquidation":
            return float(val.value)
    logger.warning("No se encontró 'NetLiquidation' en accountSummary")
    return 0.0


# ---------------------------------------------------------------------------
# Escáner y ranking (PositionScore = 1/HV -> menor volatilidad primero)
# ---------------------------------------------------------------------------

async def scan_candidates(ib: IB):
    sub = ScannerSubscription()
    sub.instrument = "STK"
    sub.locationCode = "STK.US.MAJOR"
    sub.scanCode = "HIGH_VS_13W_HL"
    filter_options = [
        TagValue("priceAbove", "1"),
        TagValue("priceBelow", "100"),
        TagValue("lastVsEMAChangeRatio200Above", "0"),
        TagValue("marketCapAbove1e6", "500"),
        TagValue("avgVolumeAbove", "100000"),
    ]
    results = await ib.reqScannerDataAsync(sub, [], filter_options)
    emit("scanner.result", scan_code=sub.scanCode, count=len(results))
    logger.info("Scanner: %d contratos encontrados", len(results))
    return results


async def rank_candidates(ib: IB, candidates):
    async def hv_for(data):
        contract = data.contractDetails.contract
        hv = await compute_annualized_hv(ib, contract)
        if not hv or hv <= 0:
            return None
        return (contract, hv)

    results = await asyncio.gather(*(hv_for(d) for d in candidates))
    ranked = [r for r in results if r is not None]
    ranked.sort(key=lambda x: x[1])  # HV ascendente == PositionScore (1/HV) descendente
    emit("scanner.ranked", candidates=len(candidates), ranked=len(ranked))
    return ranked


# ---------------------------------------------------------------------------
# Watchlist: detección de ruptura y del pullback de entrada
# ---------------------------------------------------------------------------

async def update_watch_entry(ib: IB, entry: dict, vix_filter_ok: bool):
    contract = Stock(conId=entry["con_id"], exchange=entry["exchange"], currency=entry["currency"])
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified:
        logger.warning("No se pudo cualificar %s, se descarta de la watchlist", entry["symbol"])
        emit("signal.discarded", symbol=entry["symbol"], reason="not_qualified")
        remove_watch_entry(entry["con_id"])
        return

    bars = await ib.reqHistoricalDataAsync(
        contract, endDateTime="", durationStr="6 M", barSizeSetting="1 day",
        whatToShow="TRADES", useRTH=True, formatDate=1,
    )
    if len(bars) < HH_LOOKBACK_DAYS + ATR_PERIOD + 2:
        return

    last = bars[-1]  # sesión más reciente ya cerrada (ayer, si el job corre por la mañana)

    if entry["phase"] == "breakout_wait":
        rolling_hh = get_rolling_hh(bars)
        if rolling_hh is None:
            return
        if last.high > rolling_hh:  # H_breakout
            atr_last = get_atr(bars[:-1])
            if atr_last is None:
                remove_watch_entry(entry["con_id"])
                return
            entry.update(
                phase="entry_wait",
                limit_buy=last.low - atr_last * ATR_MULTIPLIER,
                days_in_window=0,
            )
            upsert_watch_entry(entry)
            emit("signal.breakout", symbol=entry["symbol"], limit_buy=entry["limit_buy"], atr=atr_last)
            logger.info("%s: ruptura de máximo de %d días; limit_buy=%.2f",
                        entry["symbol"], HH_LOOKBACK_DAYS, entry["limit_buy"])
        return

    if entry["phase"] == "entry_wait":
        entry["days_in_window"] += 1
        touched = last.low < entry["limit_buy"]

        if touched:
            red_candle = last.close < last.open
            if red_candle and vix_filter_ok:
                atr_prev = get_atr(bars[:-2])  # ATR "del día anterior a la señal"
                if atr_prev is not None:
                    emit("signal.entry", symbol=entry["symbol"], limit_buy=entry["limit_buy"], close=last.close)
                    await submit_entry_bracket(ib, contract, entry, atr_prev * ATR_MULTIPLIER, last.close)
            else:
                emit("signal.discarded", symbol=entry["symbol"], reason="no_red_candle_or_vix",
                     red_candle=red_candle, vix_ok=vix_filter_ok)
                logger.info("%s: tocó el nivel de compra pero faltó vela roja/VIX; se descarta",
                            entry["symbol"])
            remove_watch_entry(entry["con_id"])
            return

        if entry["days_in_window"] >= BREAKOUT_WINDOW_DAYS:
            emit("signal.expired", symbol=entry["symbol"], days=entry["days_in_window"])
            logger.info("%s: expiró la ventana de %d días sin tocar el nivel de entrada",
                        entry["symbol"], BREAKOUT_WINDOW_DAYS)
            remove_watch_entry(entry["con_id"])
            return

        upsert_watch_entry(entry)


async def submit_entry_bracket(ib: IB, contract, entry: dict, stop_distance: float, reference_price: float):
    """Envía de una vez: MOO de compra (ejecuta en la apertura de hoy) + stop
    de protección como hija del MOO, usando reference_price (cierre del día
    de la señal) como proxy del precio de entrada real."""
    equity = await get_equity(ib)
    notional = equity * POSITION_SIZE_PCT_OF_EQUITY / 100
    if not reference_price or reference_price <= 0:
        return
    quantity = int(notional // reference_price)
    if quantity <= 0:
        logger.warning("%s: notional insuficiente para abrir posición", entry["symbol"])
        return

    oca_group = f"pos_{entry['con_id']}"
    stop_price = round(reference_price - stop_distance, 2)

    parent = MarketOrder("BUY", quantity)
    parent.tif = "OPG"
    parent.orderId = ib.client.getReqId()
    parent.transmit = False

    stop = StopOrder("SELL", quantity, stop_price)
    stop.orderId = ib.client.getReqId()
    stop.parentId = parent.orderId
    stop.ocaGroup = oca_group
    stop.ocaType = OCA_TYPE
    stop.tif = "GTC"  # sin esto IB lo trata como DAY y la posición queda sin stop desde el segundo día
    stop.transmit = True

    ib.placeOrder(contract, parent)
    ib.placeOrder(contract, stop)

    insert_position({
        "con_id": entry["con_id"],
        "symbol": entry["symbol"],
        "exchange": entry["exchange"],
        "currency": entry["currency"],
        "stop_order_id": stop.orderId,
        "exit_order_id": None,
        "entry_atr": stop_distance,
        "oca_group": oca_group,
    })
    emit("order.bracket", symbol=entry["symbol"], qty=quantity, stop_price=stop_price,
         reference_price=reference_price, equity=equity)
    logger.info("%s: bracket enviado — MOO compra qty=%d, stop en %.2f (referencia %.2f)",
                entry["symbol"], quantity, stop_price, reference_price)


# ---------------------------------------------------------------------------
# Posiciones abiertas: LOC de salida actualizada a diario + limpieza de tracking
# ---------------------------------------------------------------------------

def build_loc_order(action: str, quantity: int, limit_price: float, oca_group: str):
    order = Order()
    order.action = action
    order.orderType = "LOC"
    order.totalQuantity = quantity
    order.lmtPrice = round(limit_price, 2)
    order.tif = "DAY"
    order.ocaGroup = oca_group
    order.ocaType = OCA_TYPE
    return order


async def update_exit_order(ib: IB, contract, pos: dict, quantity: int):
    """Sell = C > Ref(HHV(C,2),-1): el umbral se calcula con los 2 cierres
    más recientes ya conocidos y se manda/actualiza como límite de una LOC
    para el cierre de hoy."""
    bars = await ib.reqHistoricalDataAsync(
        contract, endDateTime="", durationStr="5 D", barSizeSetting="1 day",
        whatToShow="TRADES", useRTH=True, formatDate=1,
    )
    if len(bars) < 2:
        return
    threshold = max(bars[-1].close, bars[-2].close)

    # Una LOC viva se modifica; una ya terminada no se puede reutilizar (IB: error 103, id duplicado).
    live_ids = {trade.order.orderId for trade in ib.openTrades()}
    order_id = pos["exit_order_id"] if pos["exit_order_id"] in live_ids else ib.client.getReqId()
    order = build_loc_order("SELL", quantity, threshold, pos["oca_group"])
    order.orderId = order_id
    ib.placeOrder(contract, order)  # nueva orden si order_id es nuevo, modificación si ya existía

    if pos["exit_order_id"] != order_id:
        update_position(pos["con_id"], exit_order_id=order_id)
    emit("order.exit_updated", symbol=pos["symbol"], limit=threshold, qty=quantity)
    logger.info("%s: LOC de venta actualizada a %.2f para el cierre de hoy", pos["symbol"], threshold)


async def replace_missing_stop(ib: IB, contract, pos: dict, ib_pos):
    """Repone el stop perdido al nivel de entrada real menos la distancia original, en un grupo OCA nuevo:
    IB no admite reutilizar uno terminado, y la LOC de hoy entra en el mismo grupo que este stop.
    Devuelve la orden enviada, o None si no se repuso. Mira todos los clientes: un stop ajeno también cuenta."""
    con_id = pos["con_id"]
    try:
        open_trades = await asyncio.wait_for(ib.reqAllOpenOrdersAsync(), OPEN_ORDERS_TIMEOUT_S)
    except TimeoutError:
        emit("position.stop_missing", level=logging.CRITICAL, symbol=pos["symbol"],
             stop_order_id=pos["stop_order_id"], qty=ib_pos.position, reason="IB no devolvió las órdenes")
        return None
    has_sell_stop = any(
        t.contract.conId == con_id and t.order.action == "SELL" and t.order.orderType.startswith("STP")
        for t in open_trades
    )
    stop_price = round(ib_pos.avgCost - (pos["entry_atr"] or 0), 2)
    if has_sell_stop or not pos["entry_atr"] or stop_price <= 0:
        emit("position.stop_missing", level=logging.CRITICAL, symbol=pos["symbol"],
             stop_order_id=pos["stop_order_id"], qty=ib_pos.position,
             reason="otro stop de venta vivo" if has_sell_stop else "sin datos para calcular el nivel")
        return None

    stop = StopOrder("SELL", int(ib_pos.position), stop_price)
    stop.orderId = ib.client.getReqId()
    stop.ocaGroup = f"pos_{con_id}_{stop.orderId}"
    stop.ocaType = OCA_TYPE
    stop.tif = "GTC"
    trade = ib.placeOrder(contract, stop)

    update_position(con_id, stop_order_id=stop.orderId, oca_group=stop.ocaGroup, exit_order_id=None)
    pos.update(stop_order_id=stop.orderId, oca_group=stop.ocaGroup, exit_order_id=None)
    emit("position.stop_replaced", level=logging.WARNING, symbol=pos["symbol"], order_id=stop.orderId,
         stop_price=stop_price, qty=ib_pos.position)
    return trade


async def sync_positions(ib: IB):
    """Para cada posición en tracking: si ya no existe en IB (nunca llegó a
    ejecutarse, o ya cerró por stop/LOC), se limpia. Si sigue abierta, se
    actualiza su LOC de salida con el umbral de hoy."""
    tracked = load_positions()
    # Sin una lista fresca de órdenes no se decide nada: cancelar o reponer a ciegas podría dejar sin stop o duplicarlo.
    try:
        await asyncio.wait_for(ib.reqOpenOrdersAsync(), OPEN_ORDERS_TIMEOUT_S)
    except TimeoutError:
        emit("positions.sync_skipped", level=logging.ERROR, tracked=len(tracked))
        return

    ib_positions = {p.contract.conId: p for p in await ib.reqPositionsAsync() if p.position != 0}

    # La estrategia es solo largos: un corto solo puede venir de un solape de órdenes. No se opera sobre él.
    short_ids = {con_id for con_id, p in ib_positions.items() if p.position < 0}
    for con_id in sorted(short_ids):
        p = ib_positions[con_id]
        emit("position.short_detected", level=logging.CRITICAL, con_id=con_id,
             symbol=p.contract.symbol, qty=p.position)

    live_orders = {trade.order.orderId: trade.order for trade in ib.openTrades()}

    # Una posición larga que el bot no sigue no tiene quien la proteja ni la cierre.
    tracked_ids = {pos["con_id"] for pos in tracked}
    for con_id, p in sorted(ib_positions.items()):
        if p.position > 0 and con_id not in tracked_ids:
            emit("position.untracked_long", level=logging.CRITICAL, con_id=con_id,
                 symbol=p.contract.symbol, qty=p.position)

    for pos in tracked:
        ib_pos = ib_positions.get(pos["con_id"])
        if ib_pos is None:
            # Sin posición, un stop GTC vivo queda huérfano y al saltar abriría un corto.
            # Con la compra madre aún viva no es huérfano: la entrada todavía no se ha ejecutado.
            orphan = live_orders.get(pos["stop_order_id"])
            if orphan is not None and orphan.parentId in live_orders:
                continue
            if orphan is not None:
                ib.cancelOrder(orphan)
                emit("position.orphan_stop_cancelled", level=logging.WARNING, symbol=pos["symbol"],
                     order_id=orphan.orderId)
            logger.info("%s: sin posición abierta en IB; se limpia el tracking", pos["symbol"])
            emit("position.untracked", symbol=pos["symbol"], reason="no_position_in_ib")
            remove_position(pos["con_id"])
            continue

        if pos["con_id"] in short_ids:
            continue

        contract = Stock(conId=pos["con_id"], exchange=pos["exchange"], currency=pos["currency"])
        await ib.qualifyContractsAsync(contract)
        replaced = None
        if pos["stop_order_id"] not in live_orders:
            replaced = await replace_missing_stop(ib, contract, pos, ib_pos)
        await update_exit_order(ib, contract, pos, int(abs(ib_pos.position)))

        # Sin probar contra IB: que el rechazo de la LOC o del propio stop lo mate debe saberse hoy, no mañana.
        if replaced is not None:
            await asyncio.sleep(STOP_CHECK_DELAY_S)
            if replaced.orderStatus.status in OrderStatus.DoneStates:
                emit("position.stop_missing", level=logging.CRITICAL, symbol=pos["symbol"],
                     stop_order_id=pos["stop_order_id"], qty=ib_pos.position,
                     reason=f"stop repuesto terminado: {replaced.orderStatus.status}")

async def is_trading_day(ib: IB) -> bool:
    """Comprueba si hoy hay sesión abierta usando el calendario de negociación
    de IB (tradingHours), para no operar en festivos que el cron por sí solo
    no puede conocer."""
    reference = Stock("SPY", "SMART", "USD")
    details = await ib.reqContractDetailsAsync(reference)
    if not details:
        logger.warning("No se pudo obtener el calendario de negociación; se asume mercado abierto")
        return True

    tz = ZoneInfo(details[0].timeZoneId)
    today_str = datetime.now(tz).strftime("%Y%m%d")

    for session in details[0].tradingHours.split(";"):
        if session.startswith(today_str):
            return "CLOSED" not in session

    logger.warning("No se encontró la sesión de hoy (%s) en tradingHours; se asume mercado abierto", today_str)
    return True

# ---------------------------------------------------------------------------
# Ejecución diaria (pensada para correr por la mañana, antes de la apertura)
# ---------------------------------------------------------------------------

async def run_once(ib: IB):
    logger.info("=== Ejecución diaria iniciada (antes de la apertura) ===")
    assert_session_is_demo(ib)  # segunda barrera: también si se llama a run_once sin pasar por bot.py
    init_db()

    if not await is_trading_day(ib):
        logger.info("Hoy el mercado está cerrado (festivo/fin de semana); no se hace nada")
        emit("run.market_closed")
        return

    # 1) Posiciones ya abiertas: limpiar tracking obsoleto y refrescar la LOC de salida
    await sync_positions(ib)

    # 2) Filtro de VIX (percentil) para decidir si hoy se permiten entradas nuevas
    vix_ok = await get_vix_percentile_ok(ib)

    # 3) Avanzar la watchlist (rupturas / pullbacks pendientes); las que disparan
    #    compra mandan ya su bracket (MOO + stop) para la apertura de hoy
    watchlist = load_watchlist()
    logger.info("Watchlist actual: %d contratos", len(watchlist))
    for entry in watchlist:
        try:
            await update_watch_entry(ib, entry, vix_ok)
        except Exception:
            emit("strategy.entry_error", level=logging.ERROR, exc_info=True, symbol=entry["symbol"])
            logger.exception("Error procesando %s", entry["symbol"])

    # 4) Si hay huecos, buscar candidatos nuevos
    watchlist = load_watchlist()
    occupied = len(load_positions()) + len(watchlist)
    slots = MAX_POSITIONS - occupied
    if slots <= 0:
        logger.info("Sin huecos libres, no se buscan candidatos nuevos")
        logger.info("=== Ejecución diaria terminada ===")
        return

    candidates = await scan_candidates(ib)
    ranked = await rank_candidates(ib, candidates)

    existing_ids = {w["con_id"] for w in watchlist} | {p["con_id"] for p in load_positions()}
    added = 0
    for contract, _hv in ranked:
        if added >= slots:
            break
        if contract.conId in existing_ids:
            continue
        bars = await ib.reqHistoricalDataAsync(
            contract, endDateTime="", durationStr="6 M", barSizeSetting="1 day",
            whatToShow="TRADES", useRTH=True, formatDate=1,
        )
        if len(bars) < HH_LOOKBACK_DAYS + ATR_PERIOD + 2:
            continue
        upsert_watch_entry({
            "con_id": contract.conId,
            "symbol": contract.symbol,
            "exchange": contract.exchange or contract.primaryExchange,
            "currency": contract.currency,
            "phase": "breakout_wait",
        })
        added += 1

    emit("watchlist.updated", added=added, size=len(load_watchlist()), positions=len(load_positions()))
    logger.info("Añadidos %d candidatos nuevos a la watchlist", added)
    logger.info("=== Ejecución diaria terminada ===")


# ---------------------------------------------------------------------------
# Punto de entrada para bot.py
# ---------------------------------------------------------------------------

async def strategy(ib: IB, run_immediately: bool = True):
    """Pensado para un proceso persistente (bot.py): lanza run_once() y
    luego espera 24h antes de repetir. Si prefieres invocarla desde un
    cron externo (recomendado: por la mañana, antes de la apertura), no
    uses esta función: llama a run_once(ib) directamente tras conectar y
    desconecta al terminar.
    """
    if run_immediately:
        await run_once(ib)

    while True:
        await asyncio.sleep(24 * 60 * 60)
        try:
            await run_once(ib)
        except Exception:
            logger.exception("Fallo en la ejecución diaria de la estrategia")
