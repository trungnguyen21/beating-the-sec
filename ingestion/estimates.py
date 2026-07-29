import asyncio
from datetime import timezone
from zoneinfo import ZoneInfo
import yfinance as yf
from utils.logger import get_logger

ET = ZoneInfo("America/New_York")

logger = get_logger(__name__)


def _next_earnings_row_sync(ticker: str):
    """Blocking yfinance call — the single upcoming earnings_dates row (or None).
    Shared by _fetch_sync (needs EPS estimate + date) and _next_earnings_date_sync
    (needs only the date, for the daily calendar-refresh job) so neither pays for a
    duplicate yfinance call for the same underlying data."""
    t = yf.Ticker(ticker)
    ed = t.earnings_dates
    if ed is None or ed.empty:
        return None
    upcoming = ed[ed["Reported EPS"].isna()]
    if upcoming.empty:
        return None
    return upcoming.iloc[0], upcoming.index[0]


def _fetch_sync(ticker: str) -> dict:
    """Blocking yfinance call — run in a thread via asyncio.to_thread."""
    t = yf.Ticker(ticker)

    eps_est = None
    estimate_date = None
    rev_est = None

    # earnings_dates rows: upcoming quarters have NaN for Reported EPS.
    # The first such row is the current/next quarter being estimated.
    try:
        upcoming = _next_earnings_row_sync(ticker)
        if upcoming is not None:
            row, index = upcoming
            eps_est = float(row["EPS Estimate"]) if row["EPS Estimate"] == row["EPS Estimate"] else None
            # Index is a tz-aware datetime — normalize to UTC date string
            estimate_date = index.astimezone(timezone.utc).strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"earnings_dates fetch failed for {ticker}: {e}")

    # revenue_estimate has no dated equivalent in yfinance — fall back to "0q"
    try:
        rev_df = t.revenue_estimate
        if rev_df is not None and "0q" in rev_df.index:
            rev_est = float(rev_df.loc["0q", "avg"])
    except Exception as e:
        logger.warning(f"revenue_estimate fetch failed for {ticker}: {e}")

    return {
        "eps_estimate": eps_est,
        "revenue_estimate_millions": round(rev_est / 1_000_000, 2) if rev_est else None,
        "estimate_for_date": estimate_date,  # lets caller verify this matches the filing quarter
    }


def _next_earnings_datetime_sync(ticker: str) -> tuple[str | None, str | None]:
    """Blocking yfinance call — date + time-of-day, no revenue_estimate fetch. Used by the
    daily calendar-refresh job (db/refresh_calendar.py), which needs this for 1000+ tickers
    and shouldn't pay for a second yfinance call per ticker it doesn't use.

    Both normalized to America/New_York, not UTC — compared against ET "today"/"now" by
    ingestion/rotation_scheduler.py's CalendarHotSet. (fetch_estimates' estimate_for_date stays
    UTC — unrelated, already-relied-upon behavior elsewhere.)

    Returns (date_str "YYYY-MM-DD", time_str "HH:MM:SS"), either of which may be None if no
    upcoming row exists. yfinance's earnings_dates index carries a real report time (verified:
    JPM/WMT/HD ~06:00-08:00 ET premarket, AAPL/NVDA/MSFT 16:00 ET aftermarket) — not a
    placeholder — so the time is trustworthy when present."""
    try:
        upcoming = _next_earnings_row_sync(ticker)
        if upcoming is None:
            return None, None
        _row, index = upcoming
        local = index.astimezone(ET)
        return local.strftime("%Y-%m-%d"), local.strftime("%H:%M:%S")
    except Exception as e:
        logger.warning(f"earnings_dates fetch failed for {ticker}: {e}")
        return None, None


async def fetch_estimates(ticker: str) -> dict:
    """
    Fetch analyst consensus EPS and revenue estimates for the upcoming quarter.
    Returns estimate_for_date so the caller can confirm it matches the filing being processed.
    """
    try:
        return await asyncio.to_thread(_fetch_sync, ticker)
    except Exception as e:
        logger.warning(f"Could not fetch estimates for {ticker}: {e}")
        return {"eps_estimate": None, "revenue_estimate_millions": None, "estimate_for_date": None}


async def fetch_next_earnings_datetime(ticker: str) -> tuple[str | None, str | None]:
    """(date, time) fetch — both ET — of the next upcoming earnings report, or (None, None) if
    unknown. Lighter than fetch_estimates() — skips the revenue_estimate call entirely."""
    try:
        return await asyncio.to_thread(_next_earnings_datetime_sync, ticker)
    except Exception as e:
        logger.warning(f"Could not fetch next earnings datetime for {ticker}: {e}")
        return None, None


def _ticker_exists_sync(ticker: str) -> bool:
    """True unless yfinance positively confirms the ticker doesn't exist (delisted/typo/etc).
    Only meant to be called for tickers that already had no earnings_dates row — a valid
    ticker with simply no announced upcoming earnings date is NOT "not found" and must never
    hit this check as grounds for removal.

    Defaults to True (assume valid) on any fetch error — only a confirmed empty `.info` with
    no `symbol` field counts as not-found. Never remove a ticker on a transient network blip."""
    try:
        info = yf.Ticker(ticker).info
        return bool(info and info.get("symbol"))
    except Exception as e:
        logger.warning(f"info fetch failed for {ticker}: {e}")
        return True


async def ticker_exists(ticker: str) -> bool:
    try:
        return await asyncio.to_thread(_ticker_exists_sync, ticker)
    except Exception as e:
        logger.warning(f"Could not verify ticker {ticker}: {e}")
        return True
