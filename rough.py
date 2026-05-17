"""
Paper Trading Short Straddle Bot — BTC/USD Daily Options
Exchange : Delta Exchange India
Strategy : Mid-frequency short straddle with 5-EMA slope entry filter
Expiry   : Daily (every day) BTC options

Requires:
    pip install delta-rest-client
"""

import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from delta_rest_client import DeltaRestClient, OrderType, TimeInForce

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
API_KEY    = "Qq2vhYvusJUBRzTBb42C0B1C9bh0xR"
API_SECRET = "iVrq55jfT0o03KJ6LeeVT8CPPnUYnU3rW71ozJmKqj06mXcrBUR86hqR13x8"

# Pick one BASE_URL:
BASE_URL   = "https://cdn-ind.testnet.deltaex.org"      # Demo India production
# BASE_URL = "https://api.delta.exchange"            # Global production
# BASE_URL = "https://cdn-ind.testnet.deltaex.org"   # India testnet
# BASE_URL = "https://testnet-api.delta.exchange"    # Global testnet

QUANTITY             = 1          # contracts per leg
SL_PCT               = 0.25       # 25 % SL on each leg premium
EMA_PERIOD           = 5          # EMA period on straddle price
SLOPE_THRESH         = 10       # EMA slope entry threshold
MAX_REENTRY          = 10         # max trades per session
REENTRY_WAIT         = 5 * 60    # cooldown seconds after SL hit
STRIKE_STEP          = 500        # BTC ATM rounding step

# Set to a specific expiry date string "DD-MM-YYYY" to override auto-detection,
# or leave as None to auto-detect today's (or tomorrow's) daily expiry.
EXPIRY_DATE_OVERRIDE : Optional[str] = '22-05-2026'   # e.g. "25-05-2025"

SESSION_START        = (4, 00)    # (hour, minute) UTC
SESSION_END          = (23, 55)   # exit all before daily expiry at midnight UTC

CANDLE_POLL_INTERVAL = 60         # seconds between signal checks
SL_MONITOR_INTERVAL  = 15         # seconds between SL polls

# ─────────────────────────────────────────────
# LOGGING SETUP
# Console  → INFO and above  (clean, readable output)
# Log file → DEBUG and above (full trace for debugging)
# ─────────────────────────────────────────────
_fmt     = "%(asctime)s | %(levelname)-8s | %(message)s"
_datefmt = "%H:%M:%S"

_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter(_fmt, _datefmt))

_file_h = logging.FileHandler("straddle_bot_delta.log", encoding="utf-8")
_file_h.setLevel(logging.DEBUG)
_file_h.setFormatter(logging.Formatter(_fmt, _datefmt))

log = logging.getLogger("DeltaStraddleBot")
log.setLevel(logging.DEBUG)   # master gate — handlers filter further
log.addHandler(_console)
log.addHandler(_file_h)
log.propagate = False         # don't double-log via root logger


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_request_json(resp, label: str) -> dict:
    """
    Parse a raw requests.Response from SDK's request().
    Always logs HTTP status + first 500 chars of body at DEBUG so
    every raw API response is visible in the log file.
    Raises on non-2xx so callers can catch and handle.
    """
    log.debug(f"[HTTP] {label} → status={resp.status_code}")
    log.debug(f"[HTTP] {label} → body={resp.text[:500]}")
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────
@dataclass
class Leg:
    symbol      : str
    product_id  : int
    option_type : str       # "call" or "put"
    strike      : int
    entry_price : float = 0.0
    sl_price    : float = 0.0
    ltp         : float = 0.0
    sl_hit      : bool  = False
    exited      : bool  = False


@dataclass
class Trade:
    trade_id   : int
    call_leg   : Leg
    put_leg    : Leg
    entry_time : datetime = field(default_factory=_utcnow)
    exit_time  : Optional[datetime] = None
    exit_reason: str = ""
    pnl        : float = 0.0

    def is_open(self) -> bool:
        return not self.call_leg.exited and not self.put_leg.exited


@dataclass
class BotState:
    active_trade   : Optional[Trade] = None
    trade_count    : int = 0
    all_trades     : list = field(default_factory=list)
    straddle_series: deque = field(default_factory=lambda: deque(maxlen=100))
    last_sl_time   : Optional[datetime] = None
    day_pnl        : float = 0.0
    running        : bool = True


# ─────────────────────────────────────────────
# LAYER 1 — DATA FEED
# ─────────────────────────────────────────────
class DataFeed:
    """
    Wraps the delta-rest-client SDK.
    Named SDK methods (get_ticker) return a parsed dict directly.
    The lower-level request() returns a requests.Response — always
    pass through _safe_request_json() so every call is logged.
    """

    def __init__(self, client: DeltaRestClient):
        self.client = client
        log.debug("[DataFeed] Initialised with base_url=%s", BASE_URL)

    # ── BTC spot ───────────────────────────────────────────────────────────

    def get_btc_spot(self) -> float:
        """
        BTC spot via BTCUSD perpetual mark_price.
        SDK get_ticker() returns the ticker dict directly (no result wrapper).
        """
        log.debug("[DataFeed] get_btc_spot → get_ticker('BTCUSD')")
        resp  = self.client.get_ticker("BTCUSD")
        log.debug("[DataFeed] get_ticker('BTCUSD') raw: %s", resp)
        price = float(resp["mark_price"])
        log.info(f"[DataFeed] BTC Spot (mark_price): ${price:,.2f}")
        return price

    # ── Expiry ─────────────────────────────────────────────────────────────

    def get_today_expiry_str(self) -> str:
        """
        Returns expiry as DD-MM-YYYY.
        If EXPIRY_DATE_OVERRIDE is set in config, that value is used directly.
        Otherwise auto-detects today's expiry and rolls to tomorrow past SESSION_END.
        """
        if EXPIRY_DATE_OVERRIDE:
            log.debug(f"[DataFeed] Expiry={EXPIRY_DATE_OVERRIDE}  source=MANUAL_OVERRIDE")
            return EXPIRY_DATE_OVERRIDE

        now    = _utcnow()
        rolled = False
        if (now.hour, now.minute) >= SESSION_END:
            now   += timedelta(days=1)
            rolled = True
        expiry = now.strftime("%d-%m-%Y")
        log.debug(f"[DataFeed] Expiry={expiry}  rolled={rolled}  "
                  f"utc={_utcnow().strftime('%H:%M:%S')}")
        return expiry

    # ── Option chain ───────────────────────────────────────────────────────

    def get_option_chain(self, expiry_date_str: str) -> list:
        """All BTC call+put contracts for expiry_date_str (DD-MM-YYYY)."""
        params = {
            "contract_types"          : "call_options,put_options",
            "underlying_asset_symbols": "BTC",
            "expiry_date"             : expiry_date_str,
        }
        log.debug(f"[DataFeed] get_option_chain → GET /v2/tickers  params={params}")

        raw  = self.client.request(method="GET", path="/v2/tickers",
                                   query=params, auth=False)
        data = _safe_request_json(raw, "get_option_chain")

        products = data.get("result", [])
        log.info(f"[DataFeed] Option chain [{expiry_date_str}]: "
                 f"{len(products)} contracts")

        if not products:
            log.warning("[DataFeed] Empty option chain — check expiry date "
                        "format or exchange schedule")
        else:
            strikes = sorted(set(int(float(p["strike_price"])) for p in products))
            log.debug(f"[DataFeed] Available strikes: {strikes}")

        return products

    # ── ATM lookup ─────────────────────────────────────────────────────────

    def find_atm_products(self, chain: list, atm_strike: int) -> tuple:
        """Find call and put dicts for atm_strike from the chain list."""
        log.debug(f"[DataFeed] find_atm_products: looking for strike {atm_strike} "
                  f"in {len(chain)} contracts")

        call = next((p for p in chain
                     if p.get("contract_type") == "call_options"
                     and int(float(p.get("strike_price", 0))) == atm_strike), None)
        put  = next((p for p in chain
                     if p.get("contract_type") == "put_options"
                     and int(float(p.get("strike_price", 0))) == atm_strike), None)

        if call:
            log.debug(f"[DataFeed] CALL found: {call.get('symbol')}  "
                      f"mark_price={call.get('mark_price')}")
        else:
            log.warning(f"[DataFeed] No CALL found for strike {atm_strike}")

        if put:
            log.debug(f"[DataFeed] PUT  found: {put.get('symbol')}  "
                      f"mark_price={put.get('mark_price')}")
        else:
            log.warning(f"[DataFeed] No PUT  found for strike {atm_strike}")

        if not call or not put:
            available = sorted(set(int(float(p["strike_price"])) for p in chain))
            log.warning(f"[DataFeed] ATM {atm_strike} not in chain. "
                        f"Available strikes: {available}")

        return call, put

    # ── Mark price ─────────────────────────────────────────────────────────

    def get_mark_price(self, symbol: str) -> float:
        """
        Live mark price for any option symbol.
        SDK get_ticker() returns dict directly; mark_price is top-level.
        """
        log.debug(f"[DataFeed] get_mark_price({symbol})")
        try:
            resp  = self.client.get_ticker(symbol)
            log.debug(f"[DataFeed] get_ticker({symbol}) raw: %s", resp)
            price = float(resp["mark_price"])
            log.debug(f"[DataFeed] mark_price({symbol}) = {price:.4f}")
            return price
        except Exception as e:
            log.error(f"[DataFeed] get_mark_price({symbol}) failed: {e}",
                      exc_info=True)
            return 0.0

    # ── Candles ────────────────────────────────────────────────────────────

    def get_1min_candles(self, symbol: str, limit: int = 30) -> list:
        """
        Last `limit` 1-minute close prices for an option symbol.
        Looks back up to 24 hours so sparse/illiquid testnet options
        with infrequent trades are still captured.
        The API returns bars only for minutes where a trade occurred,
        so a short window on an illiquid option yields nothing.
        """
        now_ts   = int(time.time())
        # Always look back 24 hours regardless of limit, so we capture
        # sparse candles on illiquid testnet options.
        lookback = 24 * 60 * 60
        start    = now_ts - lookback
        params   = {"resolution": "1m", "symbol": symbol,
                    "start": start, "end": now_ts}
        log.debug(f"[DataFeed] get_1min_candles({symbol}, limit={limit})  "
                  f"params={params}")
        try:
            raw  = self.client.request(method="GET", path="/v2/history/candles",
                                       query=params, auth=False)
            data    = _safe_request_json(raw, f"candles({symbol})")
            result  = data.get("result", [])
            # API returns result as a list of candle dicts (newest first)
            candles = result if isinstance(result, list) else result.get("candles", [])
            # Keep only the most recent `limit` bars
            candles = candles[:limit]
            closes  = [float(c["close"]) for c in candles]

            if closes:
                log.debug(f"[DataFeed] candles({symbol}): {len(closes)} bars  "
                          f"last_close={closes[-1]:.4f}  "
                          f"min={min(closes):.4f}  max={max(closes):.4f}")
            else:
                log.warning(f"[DataFeed] candles({symbol}): 0 bars returned — "
                            "symbol may be illiquid or too new")
            return closes
        except Exception as e:
            log.error(f"[DataFeed] get_1min_candles({symbol}) failed: {e}",
                      exc_info=True)
            return []


# ─────────────────────────────────────────────
# LAYER 2 — FEATURE ENGINE
# ─────────────────────────────────────────────
class FeatureEngine:

    @staticmethod
    def atm_strike(spot: float, step: int = STRIKE_STEP) -> int:
        strike = round(spot / step) * step
        log.debug(f"[Feature] ATM: spot={spot:.2f}  step={step}  → {strike}")
        return strike

    @staticmethod
    def calc_ema(prices: list, period: int) -> list:
        if len(prices) < period:
            log.debug(f"[Feature] calc_ema: need {period} bars, "
                      f"have {len(prices)} — returning empty")
            return []
        k   = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for p in prices[period:]:
            ema.append(p * k + ema[-1] * (1 - k))
        log.debug(f"[Feature] EMA({period}): first={ema[0]:.4f}  "
                  f"last={ema[-1]:.4f}  bars={len(ema)}")
        return ema

    @staticmethod
    def calc_slope(ema: list, lookback: int = 3) -> float:
        if len(ema) < lookback:
            log.debug(f"[Feature] calc_slope: need {lookback} EMA bars, "
                      f"have {len(ema)}")
            return 0.0
        r   = ema[-lookback:]
        n   = len(r)
        xm  = (n - 1) / 2
        ym  = sum(r) / n
        num = sum((i - xm) * (v - ym) for i, v in enumerate(r))
        den = sum((i - xm) ** 2 for i in range(n))
        slope = num / den if den else 0.0
        log.debug(f"[Feature] Slope (lookback={lookback}): {slope:.6f}  "
                  f"points={[f'{v:.4f}' for v in r]}")
        return slope

    @staticmethod
    def build_straddle_series(call_closes: list, put_closes: list) -> list:
        length = min(len(call_closes), len(put_closes))
        series = [call_closes[i] + put_closes[i] for i in range(length)]
        log.debug(
            f"[Feature] Straddle series: {length} bars  "
            f"call_bars={len(call_closes)}  put_bars={len(put_closes)}"
            + (f"  last=${series[-1]:.4f}" if series else "  (empty)")
        )
        return series


# ─────────────────────────────────────────────
# LAYER 3 — ALGO / STRATEGY
# ─────────────────────────────────────────────
class StrategyEngine:

    def __init__(self, feature: FeatureEngine, state: BotState):
        self.feature = feature
        self.state   = state

    def should_enter(self, straddle_series: list) -> tuple[bool, list, float]:
        """Returns (signal, ema_values, slope)."""
        log.debug(f"[Strategy] should_enter: series_len={len(straddle_series)}")

        if len(straddle_series) < EMA_PERIOD + 2:
            log.info(f"[Strategy] Insufficient bars: {len(straddle_series)} "
                     f"(need ≥{EMA_PERIOD + 2}) — no signal")
            return False, [], 0.0

        ema   = self.feature.calc_ema(straddle_series, EMA_PERIOD)
        slope = self.feature.calc_slope(ema)
        above = slope >= SLOPE_THRESH

        log.info(f"[Strategy] slope={slope:.6f}  threshold={SLOPE_THRESH}  "
                 f"{'ABOVE — no entry ❌' if above else 'BELOW — entry ✅'}")

        if not above:
            log.info(f"[Strategy] ✅ ENTRY SIGNAL triggered")
            return True, ema, slope

        return False, ema, slope

    def can_reenter(self) -> bool:
        if self.state.trade_count >= MAX_REENTRY:
            log.info(f"[Strategy] Max re-entries reached "
                     f"({self.state.trade_count}/{MAX_REENTRY})")
            return False
        if self.state.last_sl_time:
            elapsed   = (_utcnow() - self.state.last_sl_time).total_seconds()
            remaining = REENTRY_WAIT - elapsed
            if remaining > 0:
                log.info(f"[Strategy] Cooldown: {int(remaining)}s left  "
                         f"(elapsed={int(elapsed)}s / wait={REENTRY_WAIT}s)")
                return False
            log.debug(f"[Strategy] Cooldown expired (elapsed={int(elapsed)}s)")
        return True

    def past_session_end(self) -> bool:
        now    = _utcnow()
        result = (now.hour, now.minute) >= SESSION_END
        if result:
            log.debug(f"[Strategy] past_session_end=True  "
                      f"utc={now.strftime('%H:%M')}")
        return result

    def before_session_start(self) -> bool:
        now    = _utcnow()
        result = (now.hour, now.minute) < SESSION_START
        log.debug(f"[Strategy] before_session_start={result}  "
                  f"utc={now.strftime('%H:%M')}")
        return result


# ─────────────────────────────────────────────
# LAYER 4 — ALLOCATION
# ─────────────────────────────────────────────
class AllocationEngine:
    def get_qty(self) -> int:
        log.debug(f"[Allocation] qty={QUANTITY}")
        return QUANTITY


# ─────────────────────────────────────────────
# LAYER 5 — POSITION MANAGER
# ─────────────────────────────────────────────
class PositionManager:

    def __init__(self, data: DataFeed, state: BotState):
        self.data  = data
        self.state = state

    def open_straddle(self, call_product: dict,
                      put_product: dict, trade_id: int) -> Trade:
        strike = int(float(call_product["strike_price"]))
        log.debug(f"[PositionMgr] open_straddle: id={trade_id}  strike={strike}")

        # Prefer mark_price already in the chain dict; fallback to live fetch
        raw_call = call_product.get("mark_price")
        raw_put  = put_product.get("mark_price")
        log.debug(f"[PositionMgr] Chain mark_prices — CALL={raw_call}  PUT={raw_put}")

        call_price = float(raw_call or self.data.get_mark_price(call_product["symbol"]))
        put_price  = float(raw_put  or self.data.get_mark_price(put_product["symbol"]))

        if call_price == 0.0:
            log.warning(f"[PositionMgr] CALL mark_price=0 for "
                        f"{call_product['symbol']} — entry may be stale")
        if put_price == 0.0:
            log.warning(f"[PositionMgr] PUT  mark_price=0 for "
                        f"{put_product['symbol']} — entry may be stale")

        call_leg = Leg(
            symbol      = call_product["symbol"],
            product_id  = call_product["product_id"],
            option_type = "call",
            strike      = strike,
            entry_price = call_price,
            sl_price    = round(call_price * (1 + SL_PCT), 2),
            ltp         = call_price,
        )
        put_leg = Leg(
            symbol      = put_product["symbol"],
            product_id  = put_product["product_id"],
            option_type = "put",
            strike      = strike,
            entry_price = put_price,
            sl_price    = round(put_price * (1 + SL_PCT), 2),
            ltp         = put_price,
        )

        trade = Trade(trade_id=trade_id, call_leg=call_leg, put_leg=put_leg)
        self.state.active_trade = trade
        self.state.trade_count += 1
        self.state.all_trades.append(trade)

        combined = call_price + put_price
        log.info("=" * 55)
        log.info(f"[PositionMgr] 📌 LIVE SELL Straddle — Trade #{trade_id}")
        log.info(f"  Strike        : {strike}")
        log.info(f"  CALL symbol   : {call_leg.symbol}")
        log.info(f"  CALL entry    : ${call_price:.4f}")
        log.info(f"  CALL SL       : ${call_leg.sl_price:.4f}  "
                 f"(+{SL_PCT*100:.0f}%)")
        log.info(f"  PUT  symbol   : {put_leg.symbol}")
        log.info(f"  PUT  entry    : ${put_price:.4f}")
        log.info(f"  PUT  SL       : ${put_leg.sl_price:.4f}  "
                 f"(+{SL_PCT*100:.0f}%)")
        log.info(f"  Combined prem : ${combined:.4f}")
        log.info(f"  Entry time    : {trade.entry_time.strftime('%H:%M:%S')} UTC")
        log.info("=" * 55)
        return trade

    def monitor_sl(self, trade: Trade):
        """Poll mark prices; exit both legs if either SL is breached."""
        if not trade.is_open():
            log.debug("[PositionMgr] monitor_sl: trade already closed — skip")
            return

        log.debug(f"[PositionMgr] SL poll — trade #{trade.trade_id}")
        for leg in [trade.call_leg, trade.put_leg]:
            if leg.exited:
                log.debug(f"[PositionMgr]   {leg.symbol}: already exited")
                continue

            ltp = self.data.get_mark_price(leg.symbol)
            if ltp > 0:
                leg.ltp = ltp
            else:
                log.warning(f"[PositionMgr]   {leg.symbol}: ltp=0 returned — "
                            f"keeping last known ltp=${leg.ltp:.4f}")

            pct = ((leg.ltp - leg.entry_price) / leg.entry_price * 100
                   if leg.entry_price else 0)
            log.debug(f"[PositionMgr]   {leg.option_type.upper()} {leg.symbol}: "
                      f"ltp=${leg.ltp:.4f}  sl=${leg.sl_price:.4f}  "
                      f"entry=${leg.entry_price:.4f}  move={pct:+.2f}%")

            if leg.ltp >= leg.sl_price:
                leg.sl_hit = True
                log.warning(f"[PositionMgr] 🛑 SL HIT — {leg.symbol}  "
                            f"ltp=${leg.ltp:.4f} ≥ sl=${leg.sl_price:.4f}  "
                            f"move={pct:+.2f}%")

        if trade.call_leg.sl_hit or trade.put_leg.sl_hit:
            legs_hit = "+".join(
                leg.option_type.upper()
                for leg in [trade.call_leg, trade.put_leg] if leg.sl_hit
            )
            log.info(f"[PositionMgr] SL triggered on [{legs_hit}] — "
                     "exiting both legs")
            self.exit_trade(trade, "SL_HIT")
            self.state.last_sl_time = _utcnow()

    def exit_trade(self, trade: Trade, reason: str):
        if trade.call_leg.exited and trade.put_leg.exited:
            log.debug(f"[PositionMgr] exit_trade: trade #{trade.trade_id} "
                      "already fully closed — skip")
            return

        log.info(f"[PositionMgr] Exiting trade #{trade.trade_id}  reason={reason}")
        for leg in [trade.call_leg, trade.put_leg]:
            if not leg.exited:
                ltp = self.data.get_mark_price(leg.symbol)
                if ltp > 0:
                    leg.ltp = ltp
                else:
                    log.warning(f"[PositionMgr]   {leg.symbol}: exit ltp=0 — "
                                f"using last known ltp=${leg.ltp:.4f}")
                leg.exited = True
                leg_pnl = (leg.entry_price - leg.ltp) * QUANTITY
                log.info(f"[PositionMgr]   {leg.option_type.upper()} "
                         f"{leg.symbol}: "
                         f"entry=${leg.entry_price:.4f} → "
                         f"exit=${leg.ltp:.4f}  "
                         f"pnl=${leg_pnl:+.4f}")

        trade.exit_time   = _utcnow()
        trade.exit_reason = reason
        call_pnl = (trade.call_leg.entry_price - trade.call_leg.ltp) * QUANTITY
        put_pnl  = (trade.put_leg.entry_price  - trade.put_leg.ltp)  * QUANTITY
        trade.pnl          = call_pnl + put_pnl
        self.state.day_pnl += trade.pnl
        self.state.active_trade = None

        duration = (trade.exit_time - trade.entry_time).total_seconds()
        log.info(f"[PositionMgr] 💰 Trade #{trade.trade_id} closed")
        log.info(f"  call_pnl  : ${call_pnl:+.4f}")
        log.info(f"  put_pnl   : ${put_pnl:+.4f}")
        log.info(f"  total_pnl : ${trade.pnl:+.4f}")
        log.info(f"  day_pnl   : ${self.state.day_pnl:+.4f}")
        log.info(f"  duration  : {int(duration//60)}m{int(duration%60)}s")
        log.info(f"  reason    : {reason}")


# ─────────────────────────────────────────────
# LAYER 6 — ORDER MANAGER (paper + live)
# ─────────────────────────────────────────────
class OrderManager:
    """
    Paper mode — logs all orders instead of placing them.

    To go live:
      1. Replace paper_sell/paper_buy calls with live_sell/live_buy
      2. Uncomment the live methods below
    """

    def __init__(self, client: DeltaRestClient):
        self.client    = client
        self.order_log = []

    # ── PAPER METHODS ──────────────────────────────────────────────────────

    def paper_sell(self, product_id: int, symbol: str,
                   qty: int, price: float) -> str:
        oid   = f"PAPER-SELL-{int(time.time()*1000)}"
        entry = {"id": oid, "action": "SELL", "product_id": product_id,
                 "symbol": symbol, "qty": qty, "price": price,
                 "time": _utcnow().isoformat()}
        self.order_log.append(entry)
        log.info(f"[OrderMgr] 📝 PAPER SELL  {qty}x {symbol} @ ${price:.4f} | {oid}")
        log.debug(f"[OrderMgr] order_log entry: {entry}")
        return oid

    def paper_buy(self, product_id: int, symbol: str,
                  qty: int, price: float) -> str:
        oid   = f"PAPER-BUY-{int(time.time()*1000)}"
        entry = {"id": oid, "action": "BUY", "product_id": product_id,
                 "symbol": symbol, "qty": qty, "price": price,
                 "time": _utcnow().isoformat()}
        self.order_log.append(entry)
        log.info(f"[OrderMgr] 📝 PAPER BUY   {qty}x {symbol} @ ${price:.4f} | {oid}")
        log.debug(f"[OrderMgr] order_log entry: {entry}")
        return oid

    # ── LIVE METHODS ───────────────────────────────────────────────────────

    def live_sell(self, product_id: int, symbol: str, qty: int) -> str:
        log.info(f"[OrderMgr] LIVE SELL {qty}x {symbol} "
                 f"(product_id={product_id})")
        resp = self.client.place_order(
            product_id    = product_id,
            size          = qty,
            side          = "sell",
            order_type    = OrderType.MARKET,
            time_in_force = TimeInForce.IOC,
        )
        log.debug(f"[OrderMgr] live_sell raw response: {resp}")
        if not resp.get("success"):
            raise RuntimeError(f"LIVE SELL failed for {symbol}: {resp}")
        oid = str(resp["result"]["id"])
        log.info(f"[OrderMgr] LIVE SELL confirmed | order_id={oid}")
        return oid

    def live_buy(self, product_id: int, symbol: str, qty: int) -> str:
        log.info(f"[OrderMgr] LIVE BUY  {qty}x {symbol} "
                 f"(product_id={product_id})")
        resp = self.client.place_order(
            product_id    = product_id,
            size          = qty,
            side          = "buy",
            order_type    = OrderType.MARKET,
            time_in_force = TimeInForce.IOC,
        )
        log.debug(f"[OrderMgr] live_buy raw response: {resp}")
        if not resp.get("success"):
            raise RuntimeError(f"LIVE BUY failed for {symbol}: {resp}")
        oid = str(resp["result"]["id"])
        log.info(f"[OrderMgr] LIVE BUY  confirmed | order_id={oid}")
        return oid


# ─────────────────────────────────────────────
# LAYER 7 — ANALYTICS
# ─────────────────────────────────────────────
class Analytics:

    def __init__(self, state: BotState):
        self.state = state

    def print_summary(self):
        trades = self.state.all_trades
        if not trades:
            log.info("[Analytics] No trades this session.")
            return

        winners  = [t for t in trades if t.pnl > 0]
        losers   = [t for t in trades if t.pnl <= 0]
        total    = sum(t.pnl for t in trades)
        avg_pnl  = total / len(trades)
        win_rate = len(winners) / len(trades) * 100

        log.info("=" * 60)
        log.info("                📊 SESSION SUMMARY")
        log.info("=" * 60)
        log.info(f"  Total Trades  : {len(trades)}")
        log.info(f"  Winners       : {len(winners)}")
        log.info(f"  Losers        : {len(losers)}")
        log.info(f"  Win Rate      : {win_rate:.1f}%")
        log.info(f"  Total P&L     : ${total:+,.4f}")
        log.info(f"  Avg P&L/trade : ${avg_pnl:+,.4f}")
        log.info("-" * 60)
        for t in trades:
            dur = ""
            if t.exit_time:
                s   = (t.exit_time - t.entry_time).total_seconds()
                dur = f"{int(s//60)}m{int(s%60)}s"
            sign = "✅" if t.pnl > 0 else "❌"
            log.info(f"  {sign} Trade #{t.trade_id:02d} | BTC {t.call_leg.strike} | "
                     f"PnL ${t.pnl:>+10.4f} | {t.exit_reason:<22} | {dur}")
        log.info("=" * 60)


# ─────────────────────────────────────────────
# MAIN BOT ORCHESTRATOR
# ─────────────────────────────────────────────
class ShortStraddleBot:

    def __init__(self):
        log.info("=" * 55)
        log.info("  Delta Exchange Short Straddle Bot — starting up")
        log.info("=" * 55)
        log.info(f"  BASE_URL      : {BASE_URL}")
        log.info(f"  QUANTITY      : {QUANTITY} contract(s) per leg")
        log.info(f"  SL_PCT        : {SL_PCT*100:.0f}%")
        log.info(f"  EMA_PERIOD    : {EMA_PERIOD}")
        log.info(f"  SLOPE_THRESH  : {SLOPE_THRESH}")
        log.info(f"  MAX_REENTRY   : {MAX_REENTRY}")
        log.info(f"  REENTRY_WAIT  : {REENTRY_WAIT}s")
        log.info(f"  SESSION       : "
                 f"{SESSION_START[0]:02d}:{SESSION_START[1]:02d} – "
                 f"{SESSION_END[0]:02d}:{SESSION_END[1]:02d} UTC")
        log.info(f"  Console level : INFO  |  File level: DEBUG")
        log.info("=" * 55)

        self.sdk_client = DeltaRestClient(
            base_url   = BASE_URL,
            api_key    = API_KEY,
            api_secret = API_SECRET,
        )
        log.debug("[Bot] SDK client initialised")

        self.state     = BotState()
        self.data      = DataFeed(self.sdk_client)
        self.feature   = FeatureEngine()
        self.strategy  = StrategyEngine(self.feature, self.state)
        self.alloc     = AllocationEngine()
        self.position  = PositionManager(self.data, self.state)
        self.orders    = OrderManager(self.sdk_client)
        self.analytics = Analytics(self.state)
        log.info("[Bot] All components ready.")

    # ── Session gating ─────────────────────────────────────────────────────

    def wait_for_session(self):
        while True:
            now = _utcnow()
            t   = (now.hour, now.minute)
            if t >= SESSION_END:
                log.warning(f"[Bot] Already past session end "
                            f"{SESSION_END[0]:02d}:{SESSION_END[1]:02d} UTC "
                            f"(now {now.strftime('%H:%M:%S')}) — exiting")
                self.analytics.print_summary()
                raise SystemExit(0)
            if t >= SESSION_START:
                log.info(f"[Bot] ✅ Session OPEN  "
                         f"({SESSION_START[0]:02d}:{SESSION_START[1]:02d}–"
                         f"{SESSION_END[0]:02d}:{SESSION_END[1]:02d} UTC)  "
                         f"| now={now.strftime('%H:%M:%S')} UTC")
                return
            wait_s = ((SESSION_START[0] * 3600 + SESSION_START[1] * 60) -
                      (now.hour * 3600 + now.minute * 60 + now.second))
            log.info(f"[Bot] ⏳ Waiting for session open  "
                     f"| now={now.strftime('%H:%M:%S')} UTC  "
                     f"| ~{wait_s//60}m{wait_s%60}s remaining")
            time.sleep(15)

    # ── Straddle candle series ──────────────────────────────────────────────

    def fetch_straddle_series(self, call_sym: str, put_sym: str,
                              call_mark: float = 0.0,
                              put_mark: float = 0.0) -> list:
        log.debug(f"[Bot] fetch_straddle_series: CALL={call_sym}  PUT={put_sym}")
        call_closes = self.data.get_1min_candles(call_sym, limit=30)
        put_closes  = self.data.get_1min_candles(put_sym,  limit=30)
        log.debug(f"[Bot] Candle counts: call={len(call_closes)}  "
                  f"put={len(put_closes)}")

        # Fallback: if one leg has no candle history (illiquid on testnet),
        # synthesise a flat series from the current mark_price so the
        # straddle series is not permanently blocked.
        if not call_closes and call_mark > 0 and put_closes:
            log.warning(f"[Bot] No CALL candles for {call_sym} -- "
                        f"using mark_price {call_mark:.4f} x {len(put_closes)} bars")
            call_closes = [call_mark] * len(put_closes)
        elif not call_closes:
            log.warning(f"[Bot] No CALL candles for {call_sym}")

        if not put_closes and put_mark > 0 and call_closes:
            log.warning(f"[Bot] No PUT candles for {put_sym} -- "
                        f"using mark_price {put_mark:.4f} x {len(call_closes)} bars")
            put_closes = [put_mark] * len(call_closes)
        elif not put_closes:
            log.warning(f"[Bot] No PUT  candles for {put_sym}")

        series = self.feature.build_straddle_series(call_closes, put_closes)
        if series:
            log.info(f"[Bot] Straddle series: {len(series)} bars  "
                     f"last=${series[-1]:.4f}  "
                     f"min=${min(series):.4f}  max=${max(series):.4f}")
        else:
            log.warning("[Bot] Straddle series empty — cannot compute signal")
        return series

    # ── Monitor loop (SL thread) ────────────────────────────────────────────

    def monitor_loop(self, trade: Trade):
        log.info(f"[Bot] 🔍 SL monitor started — trade #{trade.trade_id}")
        poll = 0
        while trade.is_open() and self.state.running:
            poll += 1
            log.debug(f"[Bot] SL poll #{poll} — trade #{trade.trade_id}")

            if self.strategy.past_session_end():
                log.info("[Bot] ⏰ Session end inside monitor — "
                         "force squaring off")
                self.position.exit_trade(trade, "SESSION_END_SQUAREOFF")
                for leg in [trade.call_leg, trade.put_leg]:
                    self.orders.live_buy(leg.product_id, leg.symbol, QUANTITY)
                self.state.running = False
                break

            self.position.monitor_sl(trade)

            if not trade.is_open():
                log.info(f"[Bot] Trade #{trade.trade_id} closed by SL — "
                         "placing exit buys")
                for leg in [trade.call_leg, trade.put_leg]:
                    self.orders.live_buy(leg.product_id, leg.symbol, QUANTITY)
                break

            log.debug(f"[Bot] Trade #{trade.trade_id} still open — "
                      f"next check in {SL_MONITOR_INTERVAL}s")
            time.sleep(SL_MONITOR_INTERVAL)

        log.info(f"[Bot] 🔍 SL monitor ended — trade #{trade.trade_id}  "
                 f"polls={poll}")

    # ── Main loop ───────────────────────────────────────────────────────────

    def run(self):
        self.wait_for_session()
        log.info("[Bot] Entering main trading loop...")
        cycle = 0

        while self.state.running:
            cycle += 1
            log.debug(f"[Bot] ── Cycle #{cycle} ────────────────────────────")

            # Session end guard
            if self.strategy.past_session_end():
                log.info("[Bot] Session end reached — wrapping up")
                if self.state.active_trade:
                    log.info("[Bot] Active trade found — squaring off")
                    self.position.exit_trade(self.state.active_trade,
                                             "SESSION_END_SQUAREOFF")
                break

            # Active trade still open — wait
            if self.state.active_trade and self.state.active_trade.is_open():
                log.debug("[Bot] Active trade still open — sleeping 30s")
                time.sleep(30)
                continue

            # Re-entry / cooldown gate
            if not self.strategy.can_reenter():
                log.debug("[Bot] Re-entry blocked — sleeping 60s")
                time.sleep(60)
                continue

            # ── Fetch market data ──────────────────────────────────────────
            log.info(f"[Bot] ── Cycle #{cycle}: fetching market data ────────")
            try:
                spot       = self.data.get_btc_spot()
                atm_strike = self.feature.atm_strike(spot)
                expiry_str = self.data.get_today_expiry_str()
                log.info(f"[Bot] spot=${spot:,.2f}  "
                         f"atm_strike={atm_strike}  expiry={expiry_str}")
                chain      = self.data.get_option_chain(expiry_str)
            except Exception as e:
                log.error(f"[Bot] Market data error: {e} — retrying in 30s",
                          exc_info=True)
                time.sleep(30)
                continue

            # ── ATM lookup ─────────────────────────────────────────────────
            call_prod, put_prod = self.data.find_atm_products(chain, atm_strike)
            if not call_prod or not put_prod:
                log.warning(f"[Bot] ATM {atm_strike} not found in chain — "
                            "sleeping 60s")
                time.sleep(60)
                continue

            call_sym = call_prod["symbol"]
            put_sym  = put_prod["symbol"]
            log.info(f"[Bot] ATM contracts: CALL={call_sym}  PUT={put_sym}")
            log.debug(f"[Bot] call_prod={call_prod}")
            log.debug(f"[Bot] put_prod={put_prod}")

            # ── Signal check ───────────────────────────────────────────────
            call_mark = float(call_prod.get("mark_price") or 0)
            put_mark  = float(put_prod.get("mark_price") or 0)
            series          = self.fetch_straddle_series(
                call_sym, put_sym, call_mark=call_mark, put_mark=put_mark)
            enter, _, slope = self.strategy.should_enter(series)

            if enter:
                trade_id = self.state.trade_count + 1
                log.info(f"[Bot] 🚀 Opening straddle — trade #{trade_id}")
                trade = self.position.open_straddle(call_prod, put_prod, trade_id)

                try:
                    self.orders.live_sell(call_prod["product_id"], call_sym, QUANTITY)
                    self.orders.live_sell(put_prod["product_id"],  put_sym,  QUANTITY)
                except Exception as e:
                    log.error(f"[Bot] ❌ Order placement failed: {e} — "
                              "aborting trade, attempting to flatten any filled leg",
                              exc_info=True)
                    # Best-effort: buy back both legs to flatten
                    for leg in [trade.call_leg, trade.put_leg]:
                        try:
                            self.orders.live_buy(leg.product_id, leg.symbol, QUANTITY)
                        except Exception as be:
                            log.error(f"[Bot] ❌ Flatten buy failed for "
                                      f"{leg.symbol}: {be}", exc_info=True)
                    self.position.exit_trade(trade, "ORDER_ERROR")
                    time.sleep(60)
                    continue

                log.info(f"[Bot] Starting SL monitor thread for trade #{trade_id}")
                t = threading.Thread(target=self.monitor_loop,
                                     args=(trade,), daemon=True)
                t.start()
                t.join()
                log.info(f"[Bot] Monitor thread joined — "
                         f"trades_so_far={self.state.trade_count}")
            else:
                log.info(f"[Bot] No entry this cycle (slope={slope:.6f}) — "
                         f"sleeping {CANDLE_POLL_INTERVAL}s")
                time.sleep(CANDLE_POLL_INTERVAL)

        self.analytics.print_summary()
        log.info("[Bot] Session complete. Goodbye.")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot = ShortStraddleBot()
    bot.run()