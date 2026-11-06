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
  - La regla de salida (Sell = C > Ref(HHV(C,2),-1)) se aproxima con un
    take profit: una orden límite de venta GTC al umbral (máximo de los 2
    cierres más recientes ya conocidos), cuyo precio se modifica cada mañana.
    Decisión del usuario del 6/11/2026: el AFL vende al cierre si el cierre
    supera el umbral; el take profit vende en cuanto el precio lo toca. La
    LOC equivalente no se podía combinar con el stop en un grupo OCA en IB.

Con este horario (mañana, no tras el cierre) tanto la entrada como la
salida se ejecutan el mismo día que la señal lo indica, aceptando el pequeño
margen de error frente al gap de apertura real.

El stop-loss y el take profit comparten grupo OCA (One-Cancels-All): si
salta uno se cancela automáticamente el otro.
"""

import asyncio
import logging
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_async import IB, Index, MarketOrder, Order, OrderStatus, Stock, StopOrder

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
OCA_TYPE = 1  # bracket clásico: al ejecutarse el stop o el take profit, IB cancela el otro

# Cribado propio: el AFL se evalúa sobre barras diarias de un universo fijo, sin escáner de IB ni filtros añadidos
UNIVERSE_FILE = Path(__file__).with_name("sp500.txt")
HISTORY_DURATION = "2 Y"  # cubre HV (252 + 1 cierres) y el arranque del ATR de Wilder
SCAN_CONCURRENCY = 20     # peticiones de histórico simultáneas; IB admite hasta 50

VIX_LOOKBACK_DAYS = 100
VIX_PERCENTILE_MAX = 85


# ---------------------------------------------------------------------------
# Persistencia (SQLite)
# ---------------------------------------------------------------------------

def _write(*statements: tuple) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        for statement in statements:
            conn.execute(*statement)
        conn.commit()


def _load_all(table: str) -> list[dict]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(f"SELECT * FROM {table}").fetchall()]


def init_db():
    _write(
        (
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
            """,
        ),
    )


def load_positions():
    return _load_all("positions")


def insert_position(pos: dict):
    now = datetime.now(UTC).isoformat()
    _write((
        """
        INSERT INTO positions
            (con_id, symbol, exchange, currency, stop_order_id, exit_order_id,
             entry_atr, oca_group, created_at, updated_at)
        VALUES
            (:con_id, :symbol, :exchange, :currency, :stop_order_id, :exit_order_id,
             :entry_atr, :oca_group, :now, :now)
        """,
        {**pos, "now": now},
    ))


def update_position(con_id: int, **fields):
    fields["updated_at"] = datetime.now(UTC).isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    _write((f"UPDATE positions SET {set_clause} WHERE con_id = :con_id", {**fields, "con_id": con_id}))


def remove_position(con_id: int):
    _write(("DELETE FROM positions WHERE con_id = ?", (con_id,)))


# ---------------------------------------------------------------------------
# Indicadores
# ---------------------------------------------------------------------------

def atr_series(bars, period=ATR_PERIOD):
    """ATR de Wilder (sin multiplicar) en cada barra; None mientras no hay `period` rangos verdaderos."""
    series = [None] * len(bars)
    true_ranges = []
    atr = None
    for i in range(1, len(bars)):
        high, low, prev_close = bars[i].high, bars[i].low, bars[i - 1].close
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        if len(true_ranges) == period:
            atr = sum(true_ranges) / period
        elif atr is not None:
            atr = (atr * (period - 1) + true_ranges[-1]) / period
        series[i] = atr
    return series


def entry_signal(bars):
    """Buy del AFL en la última barra, sin el filtro del VIX. Devuelve SLatentry = Ref(ATRvar, -1) o None.

        H_breakout = H > Ref(HHV(H, 63), -1);  H_breakout_count = BarsSince(H_breakout) < 10
        Limit_buy:  L - ATRvar en cada ruptura; se mantiene en la ventana; se anula si L < Ref(Limit_buy, -1)
        Buy = H_breakout_count && L < Ref(Limit_buy, -1) && C < O
    """
    if len(bars) < HH_LOOKBACK_DAYS + 2:
        return None
    atr = atr_series(bars)
    limit_buy = [None] * len(bars)
    last_breakout = None
    # Solo cuentan las rupturas de la ventana: una anterior no deja nivel ni en la penúltima barra ni en la última.
    for i in range(max(HH_LOOKBACK_DAYS, len(bars) - BREAKOUT_WINDOW_DAYS - 1), len(bars)):
        breakout = bars[i].high > max(b.high for b in bars[i - HH_LOOKBACK_DAYS : i])
        previous = limit_buy[i - 1]
        if breakout:
            last_breakout = i
            limit_buy[i] = None if atr[i] is None else bars[i].low - atr[i] * ATR_MULTIPLIER
        elif last_breakout is not None and i - last_breakout < BREAKOUT_WINDOW_DAYS:
            limit_buy[i] = previous
        if previous is not None and bars[i].low < previous:
            limit_buy[i] = None

    last = len(bars) - 1
    in_window = last_breakout is not None and last - last_breakout < BREAKOUT_WINDOW_DAYS
    previous = limit_buy[last - 1]
    buy = in_window and previous is not None and bars[last].low < previous and bars[last].close < bars[last].open
    if not buy or atr[last - 1] is None:
        return None
    return atr[last - 1] * ATR_MULTIPLIER


async def get_daily_bars(ib: IB, contract, duration: str):
    return await ib.reqHistoricalDataAsync(
        contract, endDateTime="", durationStr=duration, barSizeSetting="1 day",
        whatToShow="TRADES", useRTH=True, formatDate=1,
    )


def annualized_hv(bars):
    """HV = StDev(ROC(C,1), 252) * sqrt(252)."""
    if len(bars) < HV_LOOKBACK_DAYS + 1:
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
    bars = await get_daily_bars(ib, vix, "150 D")
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
# Cribado del universo y ranking (PositionScore = 1/HV -> menor volatilidad primero)
# ---------------------------------------------------------------------------

def load_universe(path=UNIVERSE_FILE):
    return [line.strip() for line in path.read_text().splitlines() if line.strip() and not line.startswith("#")]


async def scan_candidates(ib: IB, symbols):
    """Evalúa el AFL sobre las barras diarias de cada símbolo. Devuelve (contrato, barras, distancia del stop)."""
    contracts = [Stock(symbol, "SMART", "USD") for symbol in symbols]
    qualified = [c for c in await ib.qualifyContractsAsync(*contracts) if isinstance(c, Stock) and c.conId]
    limit = asyncio.Semaphore(SCAN_CONCURRENCY)

    async def evaluate(contract):
        async with limit:
            try:
                bars = await get_daily_bars(ib, contract, HISTORY_DURATION)
            except Exception:
                logger.warning("%s: sin histórico diario", contract.symbol, exc_info=True)
                return None
        stop_distance = entry_signal(bars) if bars else None
        return (contract, bars, stop_distance) if stop_distance else None

    signals = [s for s in await asyncio.gather(*(evaluate(c) for c in qualified)) if s is not None]
    emit("scanner.result", universe=len(symbols), qualified=len(qualified), count=len(signals))
    logger.info("Cribado: %d de %d símbolos dan señal de compra", len(signals), len(symbols))
    return signals


def rank_candidates(signals):
    ranked = [(signal, hv) for signal in signals if (hv := annualized_hv(signal[1]))]
    ranked.sort(key=lambda item: item[1])  # HV ascendente == PositionScore (1/HV) descendente
    emit("scanner.ranked", candidates=len(signals), ranked=len(ranked))
    return [signal for signal, _hv in ranked]


async def submit_entry_bracket(ib: IB, contract, entry: dict, stop_distance: float, reference_price: float,
                               exit_threshold: float):
    """Envía de una vez: MOO de compra (ejecuta en la apertura de hoy) + stop de protección y take profit como
    hijas del MOO, usando reference_price (cierre del día de la señal) como proxy del precio de entrada real."""
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
    stop.transmit = False

    take_profit = build_take_profit_order(quantity, exit_threshold, oca_group)
    take_profit.orderId = ib.client.getReqId()
    take_profit.parentId = parent.orderId
    take_profit.transmit = True  # la última hija transmite el bracket entero

    ib.placeOrder(contract, parent)
    ib.placeOrder(contract, stop)
    ib.placeOrder(contract, take_profit)

    insert_position({
        "con_id": entry["con_id"],
        "symbol": entry["symbol"],
        "exchange": entry["exchange"],
        "currency": entry["currency"],
        "stop_order_id": stop.orderId,
        "exit_order_id": take_profit.orderId,
        "entry_atr": stop_distance,
        "oca_group": oca_group,
    })
    emit("order.bracket", symbol=entry["symbol"], qty=quantity, stop_price=stop_price,
         take_profit=take_profit.lmtPrice, reference_price=reference_price, equity=equity)
    logger.info("%s: bracket enviado — MOO compra qty=%d, stop en %.2f (referencia %.2f)",
                entry["symbol"], quantity, stop_price, reference_price)


# ---------------------------------------------------------------------------
# Posiciones abiertas: take profit actualizado a diario + limpieza de tracking
# ---------------------------------------------------------------------------

def exit_threshold(bars):
    """Umbral de Sell = C > Ref(HHV(C,2),-1): el máximo de los 2 cierres más recientes ya conocidos."""
    return max(bars[-1].close, bars[-2].close)


def build_take_profit_order(quantity: int, limit_price: float, oca_group: str):
    order = Order()
    order.action = "SELL"
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = round(limit_price, 2)
    order.tif = "GTC"  # vive con la posición aunque el bot no se ejecute algún día
    order.ocaGroup = oca_group
    order.ocaType = OCA_TYPE
    return order


async def update_exit_order(ib: IB, contract, pos: dict, quantity: int):
    """Lleva el take profit al umbral de hoy. Uno vivo se modifica, nunca se cancela: cancelar un miembro de
    un grupo OCA puede arrastrar al stop."""
    bars = await get_daily_bars(ib, contract, "5 D")
    if len(bars) < 2:
        return
    threshold = exit_threshold(bars)

    # El vivo se modifica sobre la orden que devuelve IB, que conserva su madre y su grupo.
    # Uno terminado no se puede reutilizar (IB: error 103, id duplicado): se crea otro.
    live = next((t.order for t in ib.openTrades() if t.order.orderId == pos["exit_order_id"]), None)
    if live is not None:
        order = live
        order.lmtPrice = round(threshold, 2)
        order.totalQuantity = quantity
    else:
        order = build_take_profit_order(quantity, threshold, pos["oca_group"])
        order.orderId = ib.client.getReqId()
    ib.placeOrder(contract, order)

    if pos["exit_order_id"] != order.orderId:
        update_position(pos["con_id"], exit_order_id=order.orderId)
    emit("order.exit_updated", symbol=pos["symbol"], limit=threshold, qty=quantity)
    logger.info("%s: take profit en %.2f", pos["symbol"], threshold)


def _emit_stop_missing(pos: dict, ib_pos, reason: str) -> None:
    emit("position.stop_missing", level=logging.CRITICAL, symbol=pos["symbol"],
         stop_order_id=pos["stop_order_id"], qty=ib_pos.position, reason=reason)


async def replace_missing_stop(ib: IB, contract, pos: dict, ib_pos):
    """Repone el stop perdido al nivel de entrada real menos la distancia original, en un grupo OCA nuevo:
    IB no admite reutilizar uno terminado. El take profit del grupo viejo se cancela y update_exit_order crea
    otro en el grupo nuevo, así que nunca quedan dos órdenes de venta sin OCA entre ellas.
    Devuelve (stop enviado, take profit viejo cancelado o None), o None si no se repuso.
    Mira todos los clientes: un stop ajeno también cuenta."""
    con_id = pos["con_id"]
    try:
        open_trades = await asyncio.wait_for(ib.reqAllOpenOrdersAsync(), OPEN_ORDERS_TIMEOUT_S)
    except TimeoutError:
        _emit_stop_missing(pos, ib_pos, "IB no devolvió las órdenes")
        return None
    has_sell_stop = any(
        t.contract.conId == con_id and t.order.action == "SELL" and t.order.orderType.startswith("STP")
        for t in open_trades
    )
    stop_price = round(ib_pos.avgCost - (pos["entry_atr"] or 0), 2)
    if has_sell_stop or not pos["entry_atr"] or stop_price <= 0:
        reason = "otro stop de venta vivo" if has_sell_stop else "sin datos para calcular el nivel"
        _emit_stop_missing(pos, ib_pos, reason)
        return None

    old_exit = next((t for t in ib.openTrades() if t.order.orderId == pos["exit_order_id"]), None)
    if old_exit is not None:
        ib.cancelOrder(old_exit.order)

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
    return trade, old_exit


async def sync_positions(ib: IB):
    """Para cada posición en tracking: si ya no existe en IB (nunca llegó a
    ejecutarse, o ya cerró por stop/take profit), se limpia. Si sigue abierta,
    se actualiza su take profit con el umbral de hoy."""
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
            # Sin posición, un stop o un take profit GTC vivo queda huérfano y al ejecutarse abriría un corto.
            # Con la compra madre aún viva no lo es: la entrada todavía no se ha ejecutado.
            orphans = [live_orders[i] for i in (pos["stop_order_id"], pos["exit_order_id"]) if i in live_orders]
            if any(orphan.parentId in live_orders for orphan in orphans):
                continue
            for orphan in orphans:
                ib.cancelOrder(orphan)
                emit("position.orphan_cancelled", level=logging.WARNING, symbol=pos["symbol"],
                     order_id=orphan.orderId, order_type=orphan.orderType)
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

        # Que IB rechace el stop repuesto, o no cancele el take profit viejo, debe saberse hoy, no mañana.
        if replaced is not None:
            stop_trade, old_exit = replaced
            await asyncio.sleep(STOP_CHECK_DELAY_S)
            if stop_trade.orderStatus.status in OrderStatus.DoneStates:
                _emit_stop_missing(pos, ib_pos, f"stop repuesto terminado: {stop_trade.orderStatus.status}")
            if old_exit is not None and old_exit.orderStatus.status not in OrderStatus.DoneStates:
                # Dos ventas sin OCA entre ellas: si se ejecutan las dos, queda un corto.
                emit("position.exit_cancel_unconfirmed", level=logging.CRITICAL, symbol=pos["symbol"],
                     order_id=old_exit.order.orderId, status=old_exit.orderStatus.status)

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

async def run_once(ib: IB, universe=None):
    logger.info("=== Ejecución diaria iniciada (antes de la apertura) ===")
    assert_session_is_demo(ib)  # segunda barrera: también si se llama a run_once sin pasar por bot.py
    init_db()

    if not await is_trading_day(ib):
        logger.info("Hoy el mercado está cerrado (festivo/fin de semana); no se hace nada")
        emit("run.market_closed")
        return

    # 1) Posiciones ya abiertas: limpiar tracking obsoleto y llevar el take profit al umbral de hoy
    await sync_positions(ib)

    # 2) Filtro de VIX (percentil) para decidir si hoy se permiten entradas nuevas
    vix_ok = await get_vix_percentile_ok(ib)

    # 3) Cribado del universo: las señales mejor clasificadas mandan su bracket (MOO + stop) para hoy
    positions = load_positions()
    slots = MAX_POSITIONS - len(positions)
    if slots <= 0 or not vix_ok:
        logger.info("Sin entradas nuevas hoy: %d huecos libres, VIX %s", slots, "permite" if vix_ok else "bloquea")
        logger.info("=== Ejecución diaria terminada ===")
        return

    held = {p["con_id"] for p in positions}
    signals = [s for s in await scan_candidates(ib, universe or load_universe()) if s[0].conId not in held]
    for contract, bars, stop_distance in rank_candidates(signals)[:slots]:
        entry = {"con_id": contract.conId, "symbol": contract.symbol, "exchange": contract.exchange or "SMART",
                 "currency": contract.currency}
        try:
            emit("signal.entry", symbol=contract.symbol, close=bars[-1].close, stop_distance=stop_distance)
            await submit_entry_bracket(ib, contract, entry, stop_distance, bars[-1].close, exit_threshold(bars))
        except Exception:
            emit("strategy.entry_error", level=logging.ERROR, exc_info=True, symbol=contract.symbol)
            logger.exception("Error enviando la entrada de %s", contract.symbol)

    logger.info("=== Ejecución diaria terminada ===")
