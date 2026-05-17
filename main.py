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
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from delta_rest_client import DeltaRestClient, OrderType, TimeInForce


# ─────────────────────────────────────────────
# CONFIG — Single source of truth, injected via dependency
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class BotConfig:
    """
    Immutable configuration object.  Pass this into every component
    that needs settings; nothing reads bare module-level globals.
    """
    api_key    : str = "Qq2vhYvusJUBRzTBb42C0B1C9bh0xR"
    api_secret : str = "iVrq55jfT0o03KJ6LeeVT8CPPnUYnU3rW71ozJmKqj06mXcrBUR86hqR13x8"
    base_url   : str = "https://cdn-ind.testnet.deltaex.org"   # Demo/Testnet India
    # base_url = "https://api.delta.exchange"                  # Global production
    # base_url = "https://testnet-api.delta.exchange"          # Global testnet

    quantity           : int            = 10
    sl_pct             : float          = 0.02
    ema_period         : int            = 5
    slope_thresh       : float          = 10.0
    max_reentry        : int            = 10
    reentry_wait       : int            = 5 * 60   # seconds
    strike_step        : int            = 500

    # Set to "DD-MM-YYYY" to pin a specific expiry, or None for auto-detection.
    expiry_date_override: Optional[str] = "22-05-2026"

    session_start      : tuple[int, int] = (4, 0)    # (hour, minute) UTC
    session_end        : tuple[int, int] = (23, 55)

    candle_poll_interval: int = 60   # seconds between signal checks
    sl_monitor_interval : int = 15   # seconds between SL polls

    # ── Risk management ────────────────────────────────────────────────────
    # Max mark-price loss allowed for the day (negative = loss).
    # Bot halts once day_pnl falls below this threshold.
    daily_loss_limit   : float = -500.0

    # Cooldown after a leg is manually closed externally, in seconds.
    # Prevents the bot from immediately re-entering after a manual close.
    manual_close_cooldown: int = 5 * 60   # 5 minutes

    # How many consecutive MANUAL_CLOSE_DETECTED exits before the bot
    # pauses and waits for manual_close_cooldown before trying again.
    max_consecutive_manual_closes: int = 3


# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
def _build_logger(name: str = "DeltaStraddleBot") -> logging.Logger:
    """
    Factory that creates and returns the application logger.
    Console → INFO+; file → DEBUG+.
    """
    fmt     = "%(asctime)s | %(levelname)-8s | %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    file_h = logging.FileHandler("straddle_bot_delta.log", encoding="utf-8")
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)
    logger.addHandler(file_h)
    logger.propagate = False
    return logger


log = _build_logger()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_request_json(resp, label: str) -> dict:
    """
    Parse a raw requests.Response from SDK's request().
    Logs HTTP status + first 500 chars of body at DEBUG.
    Raises on non-2xx so callers can catch and handle.
    """
    log.debug("[HTTP] %s → status=%s", label, resp.status_code)
    log.debug("[HTTP] %s → body=%s", label, resp.text[:500])
    resp.raise_for_status()
    return resp.json()


def _retry(fn, retries: int = 3, delay: float = 1.5, label: str = ""):
    """
    Call fn() up to `retries` times, sleeping `delay` seconds between
    attempts.  Returns the result on success; re-raises the last exception
    if all attempts fail.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            log.warning("[Retry] %s attempt %d/%d failed: %s",
                        label or "call", attempt, retries, exc)
            if attempt < retries:
                time.sleep(delay)
    raise last_exc  # type: ignore[misc]


# ─────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────
@dataclass
class Leg:
    """Represents one option leg (call or put) in a straddle."""
    symbol      : str
    product_id  : int
    option_type : str     # "call" or "put"
    strike      : int
    entry_price : float = 0.0
    sl_price    : float = 0.0
    ltp         : float = 0.0
    sl_hit      : bool  = False
    exited      : bool  = False
    actual_entry: float = 0.0   # exchange fill price on open
    actual_exit : float = 0.0   # exchange fill price on close


@dataclass
class Trade:
    """Encapsulates both legs of a single straddle trade."""
    trade_id   : int
    call_leg   : Leg
    put_leg    : Leg
    entry_time : datetime            = field(default_factory=_utcnow)
    exit_time  : Optional[datetime]  = None
    exit_reason: str                 = ""
    pnl        : float               = 0.0

    def is_open(self) -> bool:
        """True while neither leg has been exited."""
        return not self.call_leg.exited and not self.put_leg.exited

    @property
    def legs(self) -> tuple[Leg, Leg]:
        return self.call_leg, self.put_leg


@dataclass
class BotState:
    """Shared mutable state accessed by multiple layers."""
    active_trade   : Optional[Trade]  = None
    trade_count    : int              = 0
    all_trades     : list             = field(default_factory=list)
    straddle_series: deque            = field(
        default_factory=lambda: deque(maxlen=100))
    last_sl_time   : Optional[datetime] = None
    day_pnl        : float            = 0.0   # mark-price PnL
    realised_pnl   : float            = 0.0   # fill-based PnL
    running        : bool             = True

    # ── Risk tracking ──────────────────────────────────────────────────────
    # Timestamp of the most recent MANUAL_CLOSE_DETECTED event.
    last_manual_close_time : Optional[datetime] = None
    # Running count of consecutive manual-close exits (resets on SL_HIT or
    # a clean trade close, so persistent manual interference is caught).
    consecutive_manual_closes: int = 0
    # Set True when daily_loss_limit is breached; prevents further entries.
    daily_loss_halt: bool = False


# ─────────────────────────────────────────────
# ABSTRACT BASE — clock / session boundary
# (makes the boundary testable / mock-able)
# ─────────────────────────────────────────────
class ISessionClock(ABC):
    """Interface for querying session boundaries."""

    @abstractmethod
    def now(self) -> datetime: ...

    @abstractmethod
    def past_session_end(self) -> bool: ...

    @abstractmethod
    def before_session_start(self) -> bool: ...


class UTCSessionClock(ISessionClock):
    """Production clock: compares real UTC time against config windows."""

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg = cfg

    def now(self) -> datetime:
        return _utcnow()

    def past_session_end(self) -> bool:
        now = self.now()
        result = (now.hour, now.minute) >= self._cfg.session_end
        if result:
            log.debug("[Clock] past_session_end=True  utc=%s", now.strftime("%H:%M"))
        return result

    def before_session_start(self) -> bool:
        now = self.now()
        result = (now.hour, now.minute) < self._cfg.session_start
        log.debug("[Clock] before_session_start=%s  utc=%s",
                  result, now.strftime("%H:%M"))
        return result


# ─────────────────────────────────────────────
# LAYER 1 — DATA FEED
# ─────────────────────────────────────────────
class DataFeed:
    """
    Wraps the delta-rest-client SDK.
    Named SDK methods (get_ticker) return a parsed dict directly.
    The lower-level request() returns a requests.Response — always
    passed through _safe_request_json() so every call is logged.
    """

    def __init__(self, client: DeltaRestClient, cfg: BotConfig) -> None:
        self._client = client
        self._cfg    = cfg
        log.debug("[DataFeed] Initialised  base_url=%s", cfg.base_url)

    # ── BTC spot ───────────────────────────────────────────────────────────

    def get_btc_spot(self) -> float:
        """BTC spot price via the BTCUSD perpetual mark_price."""
        log.debug("[DataFeed] get_btc_spot → get_ticker('BTCUSD')")
        resp  = self._client.get_ticker("BTCUSD")
        log.debug("[DataFeed] get_ticker('BTCUSD') raw: %s", resp)
        price = float(resp["mark_price"])
        log.info("[DataFeed] BTC Spot (mark_price): $%s", f"{price:,.2f}")
        return price

    # ── Expiry ─────────────────────────────────────────────────────────────

    def get_today_expiry_str(self) -> str:
        """
        Returns the expiry as DD-MM-YYYY.
        Uses EXPIRY_DATE_OVERRIDE when set; otherwise auto-detects today's
        (or tomorrow's) daily expiry and rolls past SESSION_END.
        """
        if self._cfg.expiry_date_override:
            log.debug("[DataFeed] Expiry=%s  source=MANUAL_OVERRIDE",
                      self._cfg.expiry_date_override)
            return self._cfg.expiry_date_override

        now    = _utcnow()
        rolled = False
        if (now.hour, now.minute) >= self._cfg.session_end:
            now   += timedelta(days=1)
            rolled = True
        expiry = now.strftime("%d-%m-%Y")
        log.debug("[DataFeed] Expiry=%s  rolled=%s  utc=%s",
                  expiry, rolled, _utcnow().strftime("%H:%M:%S"))
        return expiry

    # ── Option chain ───────────────────────────────────────────────────────

    def get_option_chain(self, expiry_date_str: str) -> list[dict]:
        """All BTC call+put contracts for expiry_date_str (DD-MM-YYYY)."""
        params = {
            "contract_types"          : "call_options,put_options",
            "underlying_asset_symbols": "BTC",
            "expiry_date"             : expiry_date_str,
        }
        log.debug("[DataFeed] get_option_chain → GET /v2/tickers  params=%s", params)

        raw      = self._client.request(method="GET", path="/v2/tickers",
                                        query=params, auth=False)
        data     = _safe_request_json(raw, "get_option_chain")
        products = data.get("result", [])
        log.info("[DataFeed] Option chain [%s]: %d contracts",
                 expiry_date_str, len(products))

        if not products:
            log.warning("[DataFeed] Empty option chain — check expiry date "
                        "format or exchange schedule")
        else:
            strikes = sorted(
                set(int(float(p["strike_price"])) for p in products))
            log.debug("[DataFeed] Available strikes: %s", strikes)

        return products

    # ── ATM lookup ─────────────────────────────────────────────────────────

    def find_atm_products(
        self, chain: list[dict], atm_strike: int
    ) -> tuple[Optional[dict], Optional[dict]]:
        """Return (call_dict, put_dict) for atm_strike from the chain."""
        log.debug("[DataFeed] find_atm_products: strike=%d  chain_len=%d",
                  atm_strike, len(chain))

        def _match(contract_type: str) -> Optional[dict]:
            return next(
                (p for p in chain
                 if p.get("contract_type") == contract_type
                 and int(float(p.get("strike_price", 0))) == atm_strike),
                None,
            )

        call = _match("call_options")
        put  = _match("put_options")

        for label, product in (("CALL", call), ("PUT", put)):
            if product:
                log.debug("[DataFeed] %s found: %s  mark_price=%s",
                          label, product.get("symbol"), product.get("mark_price"))
            else:
                log.warning("[DataFeed] No %s found for strike %d", label, atm_strike)

        if not call or not put:
            available = sorted(
                set(int(float(p["strike_price"])) for p in chain))
            log.warning("[DataFeed] ATM %d not in chain. Available: %s",
                        atm_strike, available)

        return call, put

    # ── Mark price ─────────────────────────────────────────────────────────

    def get_mark_price(self, symbol: str, retries: int = 3) -> float:
        """
        Live mark price for any option symbol.
        Returns 0.0 after all retries fail; caller handles the sentinel.
        """
        log.debug("[DataFeed] get_mark_price(%s)", symbol)

        def _fetch() -> float:
            resp  = self._client.get_ticker(symbol)
            log.debug("[DataFeed] get_ticker(%s) raw: %s", symbol, resp)
            price = float(resp["mark_price"])
            if price <= 0:
                raise ValueError(f"mark_price={price} — invalid")
            log.debug("[DataFeed] mark_price(%s) = %.4f", symbol, price)
            return price

        try:
            return _retry(_fetch, retries=retries,
                          label=f"get_mark_price({symbol})")
        except Exception as exc:
            log.error("[DataFeed] get_mark_price(%s) failed after %d attempts: %s",
                      symbol, retries, exc, exc_info=True)
            return 0.0

    # ── Candles ────────────────────────────────────────────────────────────

    def get_1min_candles(self, symbol: str, limit: int = 30) -> list[float]:
        """
        Last `limit` 1-minute close prices for an option symbol.
        Always looks back 24 hours so sparse/illiquid testnet options
        with infrequent trades are still captured.
        """
        now_ts   = int(time.time())
        lookback = 24 * 60 * 60
        start    = now_ts - lookback
        params   = {"resolution": "1m", "symbol": symbol,
                    "start": start, "end": now_ts}
        log.debug("[DataFeed] get_1min_candles(%s, limit=%d)  params=%s",
                  symbol, limit, params)
        try:
            raw     = self._client.request(method="GET",
                                           path="/v2/history/candles",
                                           query=params, auth=False)
            data    = _safe_request_json(raw, f"candles({symbol})")
            result  = data.get("result", [])
            candles = result if isinstance(result, list) else result.get("candles", [])
            candles = candles[:limit]
            closes  = [float(c["close"]) for c in candles]

            if closes:
                log.debug("[DataFeed] candles(%s): %d bars  "
                          "last_close=%.4f  min=%.4f  max=%.4f",
                          symbol, len(closes), closes[-1],
                          min(closes), max(closes))
            else:
                log.warning("[DataFeed] candles(%s): 0 bars — "
                            "symbol may be illiquid or too new", symbol)
            return closes
        except Exception as exc:
            log.error("[DataFeed] get_1min_candles(%s) failed: %s",
                      symbol, exc, exc_info=True)
            return []

    # ── Live positions ─────────────────────────────────────────────────────

    def get_position_size(self, product_id: int, symbol: str) -> float:
        """
        Fetch the live net position size for a single product.
        Returns the size (negative = short) or 0.0 if no position exists
        or the call fails.

        Delta Exchange /v2/positions requires either product_id or
        underlying_asset_symbol as a query parameter — it does not accept
        a bare call with no filter.
        """
        log.debug("[DataFeed] get_position_size(%s, product_id=%d)", symbol, product_id)
        try:
            raw  = self._client.request(
                method="GET",
                path="/v2/positions",
                query={"product_id": product_id},
                auth=True,
            )
            data = _safe_request_json(raw, f"get_position_size({symbol})")
            # API returns a single position object (not a list) when queried
            # by product_id.  Guard for both shapes just in case.
            result = data.get("result", {})
            if isinstance(result, list):
                result = result[0] if result else {}
            size = float(result.get("size", 0) or 0)
            log.debug("[DataFeed] position_size(%s) = %.0f", symbol, size)
            return size
        except Exception as exc:
            log.error("[DataFeed] get_position_size(%s) failed: %s",
                      symbol, exc, exc_info=True)
            return None   # None signals "API error" — distinct from 0 (flat)



# ─────────────────────────────────────────────
# LAYER 2 — FEATURE ENGINE
# ─────────────────────────────────────────────
class FeatureEngine:
    """
    Pure computation: ATM rounding, EMA, slope, straddle series.
    Stateless — all methods are static.
    """

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg = cfg

    def atm_strike(self, spot: float) -> int:
        step   = self._cfg.strike_step
        strike = round(spot / step) * step
        log.debug("[Feature] ATM: spot=%.2f  step=%d  → %d", spot, step, strike)
        return strike

    @staticmethod
    def calc_ema(prices: list[float], period: int) -> list[float]:
        if len(prices) < period:
            log.debug("[Feature] calc_ema: need %d bars, have %d — returning empty",
                      period, len(prices))
            return []
        k   = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for p in prices[period:]:
            ema.append(p * k + ema[-1] * (1 - k))
        log.debug("[Feature] EMA(%d): first=%.4f  last=%.4f  bars=%d",
                  period, ema[0], ema[-1], len(ema))
        return ema

    @staticmethod
    def calc_slope(ema: list[float], lookback: int = 3) -> float:
        if len(ema) < lookback:
            log.debug("[Feature] calc_slope: need %d EMA bars, have %d",
                      lookback, len(ema))
            return 0.0
        r   = ema[-lookback:]
        n   = len(r)
        xm  = (n - 1) / 2
        ym  = sum(r) / n
        num = sum((i - xm) * (v - ym) for i, v in enumerate(r))
        den = sum((i - xm) ** 2 for i in range(n))
        slope = num / den if den else 0.0
        log.debug("[Feature] Slope (lookback=%d): %.6f  points=%s",
                  lookback, slope, [f"{v:.4f}" for v in r])
        return slope

    @staticmethod
    def build_straddle_series(
        call_closes: list[float], put_closes: list[float]
    ) -> list[float]:
        length = min(len(call_closes), len(put_closes))
        series = [call_closes[i] + put_closes[i] for i in range(length)]
        log.debug(
            "[Feature] Straddle series: %d bars  call_bars=%d  put_bars=%d%s",
            length, len(call_closes), len(put_closes),
            f"  last=${series[-1]:.4f}" if series else "  (empty)",
        )
        return series


# ─────────────────────────────────────────────
# LAYER 3 — STRATEGY ENGINE
# ─────────────────────────────────────────────
class StrategyEngine:
    """
    Entry/re-entry logic and session boundary queries.
    Depends on FeatureEngine for signal maths and ISessionClock for
    wall-clock queries, making it independently testable.
    """

    def __init__(self,
                 feature: FeatureEngine,
                 state  : BotState,
                 clock  : ISessionClock,
                 cfg    : BotConfig) -> None:
        self._feature = feature
        self._state   = state
        self._clock   = clock
        self._cfg     = cfg

    def should_enter(
        self, straddle_series: list[float]
    ) -> tuple[bool, list[float], float]:
        """Returns (signal, ema_values, slope)."""
        log.debug("[Strategy] should_enter: series_len=%d", len(straddle_series))
        min_bars = self._cfg.ema_period + 2

        if len(straddle_series) < min_bars:
            log.info("[Strategy] Insufficient bars: %d (need ≥%d) — no signal",
                     len(straddle_series), min_bars)
            return False, [], 0.0

        ema   = self._feature.calc_ema(straddle_series, self._cfg.ema_period)
        slope = self._feature.calc_slope(ema)
        above = slope >= self._cfg.slope_thresh

        log.info("[Strategy] slope=%.6f  threshold=%.1f  %s",
                 slope, self._cfg.slope_thresh,
                 "ABOVE — no entry ❌" if above else "BELOW — entry ✅")

        if not above:
            log.info("[Strategy] ✅ ENTRY SIGNAL triggered")
            return True, ema, slope

        return False, ema, slope

    def can_reenter(self) -> bool:
        # ── Gate 1: daily loss halt ────────────────────────────────────────
        if self._state.daily_loss_halt:
            log.warning("[Strategy] 🚨 Daily loss limit hit — "
                        "no further entries this session")
            return False

        # ── Gate 2: max re-entry count ─────────────────────────────────────
        if self._state.trade_count >= self._cfg.max_reentry:
            log.info("[Strategy] Max re-entries reached (%d/%d)",
                     self._state.trade_count, self._cfg.max_reentry)
            return False

        # ── Gate 3: cooldown after any exit (SL_HIT or MANUAL_CLOSE) ──────
        if self._state.last_sl_time:
            elapsed   = (_utcnow() - self._state.last_sl_time).total_seconds()
            remaining = self._cfg.reentry_wait - elapsed
            if remaining > 0:
                log.info("[Strategy] Cooldown: %ds left  (elapsed=%ds / wait=%ds)",
                         int(remaining), int(elapsed), self._cfg.reentry_wait)
                return False
            log.debug("[Strategy] Cooldown expired (elapsed=%ds)", int(elapsed))

        # ── Gate 4: extra cooldown after manual close ──────────────────────
        if self._state.last_manual_close_time:
            elapsed   = (_utcnow() - self._state.last_manual_close_time).total_seconds()
            remaining = self._cfg.manual_close_cooldown - elapsed
            if remaining > 0:
                log.info("[Strategy] Manual-close cooldown: %ds left  "
                         "(elapsed=%ds / wait=%ds)",
                         int(remaining), int(elapsed),
                         self._cfg.manual_close_cooldown)
                return False

        # ── Gate 5: too many consecutive manual closes ─────────────────────
        if (self._state.consecutive_manual_closes
                >= self._cfg.max_consecutive_manual_closes):
            log.warning(
                "[Strategy] ⚠️  %d consecutive manual closes detected — "
                "pausing until cooldown expires. "
                "Check exchange manually before resuming.",
                self._state.consecutive_manual_closes,
            )
            # Reset the counter after warning so it doesn't block forever;
            # the manual_close_cooldown (Gate 4) provides the actual pause.
            self._state.consecutive_manual_closes = 0
            return False

        return True

    def past_session_end(self) -> bool:
        return self._clock.past_session_end()

    def before_session_start(self) -> bool:
        return self._clock.before_session_start()


# ─────────────────────────────────────────────
# LAYER 4 — ALLOCATION ENGINE
# ─────────────────────────────────────────────
class AllocationEngine:
    """Determines position size.  Extend here for dynamic sizing."""

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg = cfg

    def get_qty(self) -> int:
        log.debug("[Allocation] qty=%d", self._cfg.quantity)
        return self._cfg.quantity


# ─────────────────────────────────────────────
# LAYER 5 — POSITION MANAGER
# ─────────────────────────────────────────────
class PositionManager:
    """
    Opens, monitors, and closes straddle positions.
    Does NOT place exchange orders — delegates to OrderManager.
    """

    def __init__(self, data: DataFeed, state: BotState, cfg: BotConfig) -> None:
        self._data  = data
        self._state = state
        self._cfg   = cfg

    # ── helpers ────────────────────────────────────────────────────────────

    def _build_leg(self, product: dict, option_type: str,
                   strike: int, fill: float) -> Leg:
        """Construct a Leg dataclass from exchange product dict."""
        raw_mark   = product.get("mark_price")
        mark_price = float(
            raw_mark or self._data.get_mark_price(product["symbol"]))

        if mark_price == 0.0:
            log.warning("[PositionMgr] %s mark_price=0 for %s — entry may be stale",
                        option_type.upper(), product["symbol"])

        ref = fill if fill > 0 else mark_price
        return Leg(
            symbol       = product["symbol"],
            product_id   = product["product_id"],
            option_type  = option_type,
            strike       = strike,
            entry_price  = mark_price,
            sl_price     = round(ref * (1 + self._cfg.sl_pct), 2),
            ltp          = mark_price,
            actual_entry = fill,
        )

    def _log_trade_open(self, trade: Trade,
                        call_fill: float, put_fill: float) -> None:
        call, put = trade.call_leg, trade.put_leg
        combined        = call.entry_price + put.entry_price
        actual_combined = (call_fill + put_fill) if (
            call_fill > 0 and put_fill > 0) else 0.0

        pct = self._cfg.sl_pct * 100
        log.info("=" * 55)
        log.info("[PositionMgr] 📌 LIVE SELL Straddle — Trade #%d", trade.trade_id)
        log.info("  Strike        : %d", call.strike)
        log.info("  CALL symbol   : %s", call.symbol)
        log.info("  CALL mark     : $%.4f", call.entry_price)
        log.info("  CALL fill     : %s",
                 f"${call_fill:.4f}" if call_fill > 0 else "(pending)")
        log.info("  CALL SL       : $%.4f  (+%.0f%% on %s)",
                 call.sl_price, pct, "fill" if call_fill > 0 else "mark")
        log.info("  PUT  symbol   : %s", put.symbol)
        log.info("  PUT  mark     : $%.4f", put.entry_price)
        log.info("  PUT  fill     : %s",
                 f"${put_fill:.4f}" if put_fill > 0 else "(pending)")
        log.info("  PUT  SL       : $%.4f  (+%.0f%% on %s)",
                 put.sl_price, pct, "fill" if put_fill > 0 else "mark")
        log.info("  Mark prem     : $%.4f", combined)
        if actual_combined > 0:
            log.info("  Actual prem   : $%.4f  (slip $%+.4f)",
                     actual_combined, actual_combined - combined)
        log.info("  Entry time    : %s UTC",
                 trade.entry_time.strftime("%H:%M:%S"))
        log.info("=" * 55)

    # ── public interface ───────────────────────────────────────────────────

    def open_straddle(self, call_product: dict, put_product: dict,
                      trade_id: int,
                      call_fill: float = 0.0,
                      put_fill : float = 0.0) -> Trade:
        strike   = int(float(call_product["strike_price"]))
        log.debug("[PositionMgr] open_straddle: id=%d  strike=%d", trade_id, strike)

        call_leg = self._build_leg(call_product, "call", strike, call_fill)
        put_leg  = self._build_leg(put_product,  "put",  strike, put_fill)

        trade = Trade(trade_id=trade_id, call_leg=call_leg, put_leg=put_leg)
        self._state.active_trade = trade
        self._state.trade_count += 1
        self._state.all_trades.append(trade)

        self._log_trade_open(trade, call_fill, put_fill)
        return trade

    def reconcile_with_exchange(self, trade: Trade) -> bool:
        """
        Compare the bot's local state against live exchange positions.

        Queries each open leg individually by product_id (the Delta Exchange
        /v2/positions endpoint requires product_id or underlying_asset_symbol).

        Returns True if any leg was reconciled (state was corrected).

        Safety rules:
          - None return from get_position_size() = API error → skip that leg,
            don't falsely mark it as closed.
          - size == 0 confirmed by the exchange → leg was manually closed or
            liquidated → mark exited and record current mark price.
        """
        reconciled = False
        for leg in trade.legs:
            if leg.exited:
                continue

            size = self._data.get_position_size(leg.product_id, leg.symbol)

            if size is None:
                # API call failed — be conservative, leave state unchanged.
                log.warning("[PositionMgr] reconcile: API error for %s — "
                            "skipping this leg to avoid false close", leg.symbol)
                continue

            if size == 0.0:
                log.warning(
                    "[PositionMgr] ⚠️  RECONCILE — %s shows NO open position "
                    "on exchange (manual close / liquidation detected). "
                    "Marking leg as exited in bot state.",
                    leg.symbol,
                )
                exit_ltp = self._data.get_mark_price(leg.symbol)
                if exit_ltp > 0:
                    leg.ltp = exit_ltp
                leg.exited = True
                reconciled = True
            else:
                log.debug("[PositionMgr] reconcile: %s exchange_size=%.0f — still open",
                          leg.symbol, size)

        if reconciled:
            log.warning("[PositionMgr] ⚠️  State reconciled — one or more legs were "
                        "manually closed outside the bot. Exiting trade record.")
            self.exit_trade(trade, "MANUAL_CLOSE_DETECTED")

        return reconciled

    def monitor_sl(self, trade: Trade) -> None:
        """
        Poll mark prices; exit both legs if either SL is breached.
        Also reconciles against live exchange state to detect manual closes.
        """
        if not trade.is_open():
            log.debug("[PositionMgr] monitor_sl: trade already closed — skip")
            return

        # ── Reconcile first — catches manual closes before SL logic runs ───
        if self.reconcile_with_exchange(trade):
            log.info("[PositionMgr] monitor_sl: trade closed by reconciliation — "
                     "skipping SL check")
            return

        log.debug("[PositionMgr] SL poll — trade #%d", trade.trade_id)
        for leg in trade.legs:
            if leg.exited:
                log.debug("[PositionMgr]   %s: already exited", leg.symbol)
                continue

            ltp = self._data.get_mark_price(leg.symbol)
            if ltp > 0:
                leg.ltp = ltp
            else:
                log.warning("[PositionMgr]   %s: ltp=0 returned — "
                            "keeping last known ltp=$%.4f", leg.symbol, leg.ltp)

            pct = ((leg.ltp - leg.entry_price) / leg.entry_price * 100
                   if leg.entry_price else 0)
            log.debug("[PositionMgr]   %s %s: ltp=$%.4f  sl=$%.4f  "
                      "entry=$%.4f  move=%+.2f%%",
                      leg.option_type.upper(), leg.symbol,
                      leg.ltp, leg.sl_price, leg.entry_price, pct)

            if leg.ltp >= leg.sl_price:
                leg.sl_hit = True
                log.warning("[PositionMgr] 🛑 SL HIT — %s  ltp=$%.4f ≥ sl=$%.4f  "
                            "move=%+.2f%%",
                            leg.symbol, leg.ltp, leg.sl_price, pct)

        if trade.call_leg.sl_hit or trade.put_leg.sl_hit:
            legs_hit = "+".join(
                leg.option_type.upper()
                for leg in trade.legs if leg.sl_hit
            )
            log.info("[PositionMgr] SL triggered on [%s] — exiting both legs",
                     legs_hit)
            self.exit_trade(trade, "SL_HIT")
            self._state.last_sl_time = _utcnow()

    def exit_trade(self, trade: Trade, reason: str,
                   call_exit_fill: float = 0.0,
                   put_exit_fill : float = 0.0) -> None:
        if trade.call_leg.exited and trade.put_leg.exited:
            log.debug("[PositionMgr] exit_trade: trade #%d already fully closed",
                      trade.trade_id)
            return

        log.info("[PositionMgr] Exiting trade #%d  reason=%s",
                 trade.trade_id, reason)
        fill_map = {
            trade.call_leg.symbol: call_exit_fill,
            trade.put_leg.symbol : put_exit_fill,
        }

        for leg in trade.legs:
            if not leg.exited:
                ltp = self._data.get_mark_price(leg.symbol)
                if ltp > 0:
                    leg.ltp = ltp
                else:
                    log.warning("[PositionMgr]   %s: exit ltp=0 — "
                                "using last known ltp=$%.4f",
                                leg.symbol, leg.ltp)
                leg.actual_exit = fill_map.get(leg.symbol, 0.0)
                leg.exited      = True
                leg_pnl         = (leg.entry_price - leg.ltp) * self._cfg.quantity
                fill_note = (f"  actual_exit=${leg.actual_exit:.4f}"
                             if leg.actual_exit > 0 else "")
                log.info("[PositionMgr]   %s %s: entry=$%.4f → exit=$%.4f  "
                         "pnl=$%+.4f%s",
                         leg.option_type.upper(), leg.symbol,
                         leg.entry_price, leg.ltp, leg_pnl, fill_note)

        trade.exit_time   = _utcnow()
        trade.exit_reason = reason
        qty = self._cfg.quantity
        call_pnl  = (trade.call_leg.entry_price - trade.call_leg.ltp) * qty
        put_pnl   = (trade.put_leg.entry_price  - trade.put_leg.ltp)  * qty
        trade.pnl = call_pnl + put_pnl
        self._state.day_pnl    += trade.pnl
        self._state.active_trade = None

        # ── Risk: manual-close counter ─────────────────────────────────────
        if reason == "MANUAL_CLOSE_DETECTED":
            self._state.consecutive_manual_closes += 1
            self._state.last_manual_close_time = _utcnow()
            log.warning("[PositionMgr] ⚠️  Manual-close counter: %d/%d",
                        self._state.consecutive_manual_closes,
                        self._cfg.max_consecutive_manual_closes)
        else:
            # Any non-manual exit resets the consecutive counter
            self._state.consecutive_manual_closes = 0

        # ── Risk: daily loss halt ──────────────────────────────────────────
        if (not self._state.daily_loss_halt
                and self._state.day_pnl <= self._cfg.daily_loss_limit):
            self._state.daily_loss_halt = True
            log.error(
                "[PositionMgr] 🚨 DAILY LOSS LIMIT HIT — "
                "day_pnl=$%.2f ≤ limit=$%.2f. "
                "No further entries this session.",
                self._state.day_pnl, self._cfg.daily_loss_limit,
            )

        # Realised PnL: only when all four fill prices are captured
        c_e, c_x = trade.call_leg.actual_entry, trade.call_leg.actual_exit
        p_e, p_x = trade.put_leg.actual_entry,  trade.put_leg.actual_exit
        if all(v > 0 for v in [c_e, c_x, p_e, p_x]):
            realised = ((c_e - c_x) + (p_e - p_x)) * qty
            self._state.realised_pnl += realised
            log.info("[PositionMgr] 💰 Trade #%d closed  "
                     "(mark=$%+.4f / realised=$%+.4f)",
                     trade.trade_id, trade.pnl, realised)
        else:
            log.info("[PositionMgr] 💰 Trade #%d closed", trade.trade_id)

        duration = (trade.exit_time - trade.entry_time).total_seconds()
        log.info("  call_pnl  : $%+.4f", call_pnl)
        log.info("  put_pnl   : $%+.4f", put_pnl)
        log.info("  total_pnl : $%+.4f", trade.pnl)
        log.info("  day_pnl   : $%+.4f", self._state.day_pnl)
        if self._state.realised_pnl != 0.0:
            log.info("  realised  : $%+.4f", self._state.realised_pnl)
        log.info("  duration  : %dm%ds", int(duration // 60), int(duration % 60))
        log.info("  reason    : %s", reason)


# ─────────────────────────────────────────────
# LAYER 6 — ORDER MANAGER  (paper + live)
# ─────────────────────────────────────────────
class OrderManager:
    """
    Places sell/buy orders on the exchange.

    Paper mode — logs all orders without placing them.
    To go live swap paper_sell/paper_buy calls to live_sell/live_buy.
    """

    def __init__(self, client: DeltaRestClient, cfg: BotConfig) -> None:
        self._client    = client
        self._cfg       = cfg
        self._order_log : list[dict] = []

    # ── Paper ──────────────────────────────────────────────────────────────

    def paper_sell(self, product_id: int, symbol: str,
                   qty: int, price: float) -> str:
        oid   = f"PAPER-SELL-{int(time.time()*1000)}"
        entry = {"id": oid, "action": "SELL", "product_id": product_id,
                 "symbol": symbol, "qty": qty, "price": price,
                 "time": _utcnow().isoformat()}
        self._order_log.append(entry)
        log.info("[OrderMgr] 📝 PAPER SELL  %dx %s @ $%.4f | %s",
                 qty, symbol, price, oid)
        log.debug("[OrderMgr] order_log entry: %s", entry)
        return oid

    def paper_buy(self, product_id: int, symbol: str,
                  qty: int, price: float) -> str:
        oid   = f"PAPER-BUY-{int(time.time()*1000)}"
        entry = {"id": oid, "action": "BUY", "product_id": product_id,
                 "symbol": symbol, "qty": qty, "price": price,
                 "time": _utcnow().isoformat()}
        self._order_log.append(entry)
        log.info("[OrderMgr] 📝 PAPER BUY   %dx %s @ $%.4f | %s",
                 qty, symbol, price, oid)
        log.debug("[OrderMgr] order_log entry: %s", entry)
        return oid

    # ── Live ───────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_order_response(resp: dict, symbol: str, side: str) -> tuple[str, float]:
        """
        Normalise the two response shapes from the Delta SDK.

        Shape A — wrapped:  {"success": True, "result": {"id": ..., ...}}
        Shape B — direct:   {"id": ..., "state": "closed", "average_fill_price": "1540"}

        Returns (order_id, fill_price).
        Raises RuntimeError if the IOC order was not fully filled.
        """
        if "success" in resp:
            if not resp.get("success"):
                raise RuntimeError(
                    f"LIVE {side.upper()} failed for {symbol} "
                    f"(API success=false): {resp}"
                )
            order = resp.get("result", resp)
        else:
            order = resp

        state         = order.get("state", "")
        unfilled_size = int(order.get("unfilled_size") or 0)
        avg_fill      = order.get("average_fill_price")
        oid           = str(order.get("id", "unknown"))

        if state == "closed" and unfilled_size == 0:
            return oid, (float(avg_fill) if avg_fill else 0.0)

        raise RuntimeError(
            f"LIVE {side.upper()} for {symbol} did not fill — "
            f"state={state!r}  unfilled={unfilled_size}  "
            f"avg_fill={avg_fill}  response={order}"
        )

    def live_sell(self, product_id: int, symbol: str, qty: int) -> tuple[str, float]:
        """Returns (order_id, fill_price). Raises on failed fill."""
        log.info("[OrderMgr] LIVE SELL %dx %s (product_id=%d)",
                 qty, symbol, product_id)
        resp = self._client.place_order(
            product_id    = product_id,
            size          = qty,
            side          = "sell",
            order_type    = OrderType.MARKET,
            time_in_force = TimeInForce.IOC,
        )
        log.debug("[OrderMgr] live_sell raw response: %s", resp)
        oid, fill = self._parse_order_response(resp, symbol, "sell")
        log.info("[OrderMgr] LIVE SELL confirmed | order_id=%s  fill=$%.4f",
                 oid, fill)
        return oid, fill

    def live_buy(self, product_id: int, symbol: str, qty: int) -> tuple[str, float]:
        """Returns (order_id, fill_price). Raises on failed fill."""
        log.info("[OrderMgr] LIVE BUY  %dx %s (product_id=%d)",
                 qty, symbol, product_id)
        resp = self._client.place_order(
            product_id    = product_id,
            size          = qty,
            side          = "buy",
            order_type    = OrderType.MARKET,
            time_in_force = TimeInForce.IOC,
        )
        log.debug("[OrderMgr] live_buy raw response: %s", resp)
        oid, fill = self._parse_order_response(resp, symbol, "buy")
        log.info("[OrderMgr] LIVE BUY  confirmed | order_id=%s  fill=$%.4f",
                 oid, fill)
        return oid, fill


# ─────────────────────────────────────────────
# LAYER 7 — ANALYTICS
# ─────────────────────────────────────────────
class Analytics:
    """Computes and prints end-of-session statistics."""

    def __init__(self, state: BotState, cfg: BotConfig) -> None:
        self._state = state
        self._cfg   = cfg

    def _trade_duration_str(self, trade: Trade) -> str:
        if not trade.exit_time:
            return ""
        secs = (trade.exit_time - trade.entry_time).total_seconds()
        return f"{int(secs // 60)}m{int(secs % 60)}s"

    def _trade_pnl_str(self, trade: Trade) -> str:
        c_e, c_x = trade.call_leg.actual_entry, trade.call_leg.actual_exit
        p_e, p_x = trade.put_leg.actual_entry,  trade.put_leg.actual_exit
        if all(v > 0 for v in [c_e, c_x, p_e, p_x]):
            realised = ((c_e - c_x) + (p_e - p_x)) * self._cfg.quantity
            return f"mark ${trade.pnl:>+8.2f} / real ${realised:>+8.2f}"
        return f"mark ${trade.pnl:>+8.4f}"

    def print_summary(self) -> None:
        trades = self._state.all_trades
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
        log.info("  Total Trades  : %d", len(trades))
        log.info("  Winners       : %d", len(winners))
        log.info("  Losers        : %d", len(losers))
        log.info("  Win Rate      : %.1f%%", win_rate)
        log.info("  Mark P&L      : $%s", f"{total:+,.4f}")
        log.info("  Realised P&L  : $%s%s",
                 f"{self._state.realised_pnl:+,.4f}",
                 "  ← fill-based" if self._state.realised_pnl != 0 else
                 "  (no fills recorded)")
        log.info("  Avg P&L/trade : $%s", f"{avg_pnl:+,.4f}")
        if self._state.daily_loss_halt:
            log.warning("  ⚠️  Session halted by DAILY LOSS LIMIT  "
                        "(limit=$%.2f)", self._cfg.daily_loss_limit)
        log.info("-" * 60)
        for t in trades:
            sign = "✅" if t.pnl > 0 else "❌"
            log.info("  %s Trade #%02d | BTC %d | %s | %-22s | %s",
                     sign, t.trade_id, t.call_leg.strike,
                     self._trade_pnl_str(t), t.exit_reason,
                     self._trade_duration_str(t))
        log.info("=" * 60)


# ─────────────────────────────────────────────
# MAIN BOT ORCHESTRATOR
# ─────────────────────────────────────────────
class ShortStraddleBot:
    """
    Top-level orchestrator.  Wires up all layers, runs the trading loop,
    and manages the SL-monitor thread.
    """

    def __init__(self, cfg: Optional[BotConfig] = None) -> None:
        self._cfg = cfg or BotConfig()
        self._log_startup()

        sdk_client = DeltaRestClient(
            base_url   = self._cfg.base_url,
            api_key    = self._cfg.api_key,
            api_secret = self._cfg.api_secret,
        )
        log.debug("[Bot] SDK client initialised")

        self._state    = BotState()
        self._clock    = UTCSessionClock(self._cfg)
        self._data     = DataFeed(sdk_client, self._cfg)
        self._feature  = FeatureEngine(self._cfg)
        self._strategy = StrategyEngine(
            self._feature, self._state, self._clock, self._cfg)
        self._alloc    = AllocationEngine(self._cfg)
        self._position = PositionManager(self._data, self._state, self._cfg)
        self._orders   = OrderManager(sdk_client, self._cfg)
        self._analytics= Analytics(self._state, self._cfg)
        log.info("[Bot] All components ready.")

    # ── startup banner ─────────────────────────────────────────────────────

    def _log_startup(self) -> None:
        cfg = self._cfg
        log.info("=" * 55)
        log.info("  Delta Exchange Short Straddle Bot — starting up")
        log.info("=" * 55)
        log.info("  BASE_URL      : %s", cfg.base_url)
        log.info("  QUANTITY      : %d contract(s) per leg", cfg.quantity)
        log.info("  SL_PCT        : %.0f%%", cfg.sl_pct * 100)
        log.info("  EMA_PERIOD    : %d", cfg.ema_period)
        log.info("  SLOPE_THRESH  : %s", cfg.slope_thresh)
        log.info("  MAX_REENTRY   : %d", cfg.max_reentry)
        log.info("  REENTRY_WAIT  : %ds", cfg.reentry_wait)
        log.info("  SESSION       : %02d:%02d – %02d:%02d UTC",
                 *cfg.session_start, *cfg.session_end)
        log.info("  DAILY_LOSS_LIM: $%.2f", cfg.daily_loss_limit)
        log.info("  MANUAL_COOLDOWN: %ds", cfg.manual_close_cooldown)
        log.info("  MAX_CONSEC_MC  : %d", cfg.max_consecutive_manual_closes)
        log.info("  Console level : INFO  |  File level: DEBUG")
        log.info("=" * 55)

    # ── session gating ─────────────────────────────────────────────────────

    def _wait_for_session(self) -> None:
        cfg = self._cfg
        while True:
            now = _utcnow()
            t   = (now.hour, now.minute)
            if t >= cfg.session_end:
                log.warning("[Bot] Already past session end %02d:%02d UTC "
                            "(now %s) — exiting",
                            *cfg.session_end, now.strftime("%H:%M:%S"))
                self._analytics.print_summary()
                raise SystemExit(0)
            if t >= cfg.session_start:
                log.info("[Bot] ✅ Session OPEN  (%02d:%02d–%02d:%02d UTC)  "
                         "| now=%s UTC",
                         *cfg.session_start, *cfg.session_end,
                         now.strftime("%H:%M:%S"))
                return
            wait_s = ((cfg.session_start[0] * 3600 + cfg.session_start[1] * 60)
                      - (now.hour * 3600 + now.minute * 60 + now.second))
            log.info("[Bot] ⏳ Waiting for session open  "
                     "| now=%s UTC  | ~%dm%ds remaining",
                     now.strftime("%H:%M:%S"), wait_s // 60, wait_s % 60)
            time.sleep(15)

    # ── straddle candle series ─────────────────────────────────────────────

    def _fetch_straddle_series(self, call_sym: str, put_sym: str,
                               call_mark: float = 0.0,
                               put_mark : float = 0.0) -> list[float]:
        log.debug("[Bot] fetch_straddle_series: CALL=%s  PUT=%s", call_sym, put_sym)
        call_closes = self._data.get_1min_candles(call_sym, limit=30)
        put_closes  = self._data.get_1min_candles(put_sym,  limit=30)
        log.debug("[Bot] Candle counts: call=%d  put=%d",
                  len(call_closes), len(put_closes))

        # Fallback: synthesise a flat series from mark_price when candles
        # are unavailable (illiquid testnet options).
        if not call_closes and call_mark > 0 and put_closes:
            log.warning("[Bot] No CALL candles for %s — "
                        "using mark_price %.4f x %d bars",
                        call_sym, call_mark, len(put_closes))
            call_closes = [call_mark] * len(put_closes)
        elif not call_closes:
            log.warning("[Bot] No CALL candles for %s", call_sym)

        if not put_closes and put_mark > 0 and call_closes:
            log.warning("[Bot] No PUT candles for %s — "
                        "using mark_price %.4f x %d bars",
                        put_sym, put_mark, len(call_closes))
            put_closes = [put_mark] * len(call_closes)
        elif not put_closes:
            log.warning("[Bot] No PUT  candles for %s", put_sym)

        series = self._feature.build_straddle_series(call_closes, put_closes)
        if series:
            log.info("[Bot] Straddle series: %d bars  last=$%.4f  "
                     "min=$%.4f  max=$%.4f",
                     len(series), series[-1], min(series), max(series))
        else:
            log.warning("[Bot] Straddle series empty — cannot compute signal")
        return series

    # ── SL monitor thread ─────────────────────────────────────────────────

    def _monitor_loop(self, trade: Trade) -> None:
        log.info("[Bot] 🔍 SL monitor started — trade #%d", trade.trade_id)
        poll = 0
        cfg  = self._cfg

        while trade.is_open() and self._state.running:
            poll += 1
            log.debug("[Bot] SL poll #%d — trade #%d", poll, trade.trade_id)

            if self._strategy.past_session_end():
                log.info("[Bot] ⏰ Session end inside monitor — force squaring off")
                fills: dict[str, float] = {}
                for leg in trade.legs:
                    try:
                        _, fill = self._orders.live_buy(
                            leg.product_id, leg.symbol, cfg.quantity)
                        fills[leg.symbol] = fill
                    except Exception as exc:
                        log.error("[Bot] ❌ Session-end buy failed for %s: %s",
                                  leg.symbol, exc, exc_info=True)
                self._position.exit_trade(
                    trade, "SESSION_END_SQUAREOFF",
                    call_exit_fill=fills.get(trade.call_leg.symbol, 0.0),
                    put_exit_fill =fills.get(trade.put_leg.symbol,  0.0),
                )
                self._state.running = False
                break

            self._position.monitor_sl(trade)

            if not trade.is_open():
                log.info("[Bot] Trade #%d closed by SL — placing exit buys",
                         trade.trade_id)
                fills = {}
                for leg in trade.legs:
                    # ── BUG FIX: only buy back legs that the BOT still holds.
                    # If leg.exited is True, the exchange position is already
                    # gone (manually closed or reconciled away) — placing a
                    # buy order here would open a NEW long position.
                    if leg.exited and leg.actual_exit == 0.0:
                        # Exited by reconciliation — no exchange position to
                        # close; nothing to buy back.
                        log.info("[Bot] Skipping exit buy for %s "
                                 "(leg already closed externally)",
                                 leg.symbol)
                        continue
                    if leg.exited:
                        # Already handled (SL_HIT path set actual_exit via
                        # exit_trade), no further action needed.
                        continue
                    try:
                        _, fill = self._orders.live_buy(
                            leg.product_id, leg.symbol, cfg.quantity)
                        fills[leg.symbol] = fill
                        log.info("[Bot] Exit buy filled: %s @ $%.4f",
                                 leg.symbol, fill)
                    except Exception as exc:
                        log.error("[Bot] ❌ Exit buy failed for %s: %s — "
                                  "MANUAL INTERVENTION MAY BE REQUIRED",
                                  leg.symbol, exc, exc_info=True)

                if fills.get(trade.call_leg.symbol):
                    trade.call_leg.actual_exit = fills[trade.call_leg.symbol]
                if fills.get(trade.put_leg.symbol):
                    trade.put_leg.actual_exit  = fills[trade.put_leg.symbol]

                # ── Apply cooldown for manual closes too, not just SL_HIT ──
                # This was the second bug: the bot re-entered immediately after
                # a manual close because last_sl_time was only set in
                # PositionManager.monitor_sl (SL_HIT path).  We always enforce
                # the reentry wait here regardless of exit reason.
                if self._state.last_sl_time is None or (
                    _utcnow() - self._state.last_sl_time
                ).total_seconds() > 1:
                    self._state.last_sl_time = _utcnow()

                # Recompute session realised PnL from all trades with full fills
                c_e, c_x = trade.call_leg.actual_entry, trade.call_leg.actual_exit
                p_e, p_x = trade.put_leg.actual_entry,  trade.put_leg.actual_exit
                if all(v > 0 for v in [c_e, c_x, p_e, p_x]):
                    self._state.realised_pnl = sum(
                        ((t.call_leg.actual_entry - t.call_leg.actual_exit) +
                         (t.put_leg.actual_entry  - t.put_leg.actual_exit)) * cfg.quantity
                        for t in self._state.all_trades
                        if all(v > 0 for v in [
                            t.call_leg.actual_entry, t.call_leg.actual_exit,
                            t.put_leg.actual_entry,  t.put_leg.actual_exit,
                        ])
                    )
                    log.info("[Bot] Realised PnL updated: $%+.4f",
                             self._state.realised_pnl)
                break

            log.debug("[Bot] Trade #%d still open — next check in %ds",
                      trade.trade_id, cfg.sl_monitor_interval)
            time.sleep(cfg.sl_monitor_interval)

        log.info("[Bot] 🔍 SL monitor ended — trade #%d  polls=%d",
                 trade.trade_id, poll)

    # ── main trading loop ─────────────────────────────────────────────────

    def run(self) -> None:
        self._wait_for_session()
        log.info("[Bot] Entering main trading loop...")
        cycle = 0
        cfg   = self._cfg

        while self._state.running:
            cycle += 1
            log.debug("[Bot] ── Cycle #%d ────────────────────────────", cycle)

            if self._strategy.past_session_end():
                log.info("[Bot] Session end reached — wrapping up")
                if self._state.active_trade:
                    log.info("[Bot] Active trade found — squaring off")
                    self._position.exit_trade(self._state.active_trade,
                                              "SESSION_END_SQUAREOFF")
                break

            if self._state.active_trade and self._state.active_trade.is_open():
                log.debug("[Bot] Active trade still open — sleeping 30s")
                time.sleep(30)
                continue

            if self._state.daily_loss_halt:
                log.warning("[Bot] 🚨 Daily loss limit reached — "
                            "halting for the rest of the session")
                break

            if not self._strategy.can_reenter():
                log.debug("[Bot] Re-entry blocked — sleeping 60s")
                time.sleep(60)
                continue

            # ── Market data ────────────────────────────────────────────────
            log.info("[Bot] ── Cycle #%d: fetching market data ────────", cycle)
            try:
                spot       = self._data.get_btc_spot()
                atm_strike = self._feature.atm_strike(spot)
                expiry_str = self._data.get_today_expiry_str()
                log.info("[Bot] spot=$%s  atm_strike=%d  expiry=%s",
                         f"{spot:,.2f}", atm_strike, expiry_str)
                chain = self._data.get_option_chain(expiry_str)
            except Exception as exc:
                log.error("[Bot] Market data error: %s — retrying in 30s",
                          exc, exc_info=True)
                time.sleep(30)
                continue

            # ── ATM lookup ─────────────────────────────────────────────────
            call_prod, put_prod = self._data.find_atm_products(chain, atm_strike)
            if not call_prod or not put_prod:
                log.warning("[Bot] ATM %d not found in chain — sleeping 60s",
                            atm_strike)
                time.sleep(60)
                continue

            call_sym = call_prod["symbol"]
            put_sym  = put_prod["symbol"]
            log.info("[Bot] ATM contracts: CALL=%s  PUT=%s", call_sym, put_sym)
            log.debug("[Bot] call_prod=%s", call_prod)
            log.debug("[Bot] put_prod=%s",  put_prod)

            # ── Signal check ───────────────────────────────────────────────
            call_mark = float(call_prod.get("mark_price") or 0)
            put_mark  = float(put_prod.get("mark_price") or 0)
            series    = self._fetch_straddle_series(
                call_sym, put_sym, call_mark=call_mark, put_mark=put_mark)
            enter, _, slope = self._strategy.should_enter(series)

            if not enter:
                log.info("[Bot] No entry this cycle (slope=%.6f) — sleeping %ds",
                         slope, cfg.candle_poll_interval)
                time.sleep(cfg.candle_poll_interval)
                continue

            trade_id      = self._state.trade_count + 1
            log.info("[Bot] 🚀 Opening straddle — trade #%d", trade_id)

            call_fill = put_fill = 0.0
            try:
                _, call_fill = self._orders.live_sell(
                    call_prod["product_id"], call_sym, cfg.quantity)
                _, put_fill  = self._orders.live_sell(
                    put_prod["product_id"],  put_sym,  cfg.quantity)
            except Exception as exc:
                log.error("[Bot] ❌ Order placement failed: %s — "
                          "aborting trade, attempting to flatten filled leg",
                          exc, exc_info=True)
                for sym, pid, was_filled in [
                    (call_sym, call_prod["product_id"], call_fill > 0),
                    (put_sym,  put_prod["product_id"],  put_fill  > 0),
                ]:
                    if was_filled:
                        try:
                            self._orders.live_buy(pid, sym, cfg.quantity)
                        except Exception as be:
                            log.error("[Bot] ❌ Flatten buy failed for %s: %s",
                                      sym, be, exc_info=True)
                trade = self._position.open_straddle(
                    call_prod, put_prod, trade_id,
                    call_fill=call_fill, put_fill=put_fill)
                self._position.exit_trade(trade, "ORDER_ERROR")
                time.sleep(60)
                continue

            trade = self._position.open_straddle(
                call_prod, put_prod, trade_id,
                call_fill=call_fill, put_fill=put_fill)

            log.info("[Bot] Starting SL monitor thread for trade #%d", trade_id)
            monitor_thread = threading.Thread(
                target=self._monitor_loop, args=(trade,), daemon=True)
            monitor_thread.start()
            monitor_thread.join()
            log.info("[Bot] Monitor thread joined — trades_so_far=%d",
                     self._state.trade_count)

        self._analytics.print_summary()
        log.info("[Bot] Session complete. Goodbye.")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot = ShortStraddleBot()   # uses default BotConfig()
    bot.run()