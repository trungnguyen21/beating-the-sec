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


def _next_earnings_date_sync(ticker: str) -> str | None:
    """Blocking yfinance call — date-only, no revenue_estimate fetch. Used by the daily
    calendar-refresh job (db/refresh_calendar.py), which needs the date for 1000+ tickers
    and shouldn't pay for a second yfinance call per ticker it doesn't use.

    Normalized to America/New_York, not UTC — this date is compared against an ET "today" by
    ingestion/rotation_scheduler.py's CalendarHotSet (the premarket/aftermarket windows are
    ET business hours), so it needs to be an ET calendar date, not a UTC one. (fetch_estimates'
    estimate_for_date stays UTC — unrelated, already-relied-upon behavior elsewhere.)"""
    try:
        upcoming = _next_earnings_row_sync(ticker)
        if upcoming is None:
            return None
        _row, index = upcoming
        return index.astimezone(ET).strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"earnings_dates fetch failed for {ticker}: {e}")
        return None


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


async def fetch_next_earnings_date(ticker: str) -> str | None:
    """Date-only fetch (YYYY-MM-DD, UTC) of the next upcoming earnings date, or None if
    unknown. Lighter than fetch_estimates() — skips the revenue_estimate call entirely."""
    try:
        return await asyncio.to_thread(_next_earnings_date_sync, ticker)
    except Exception as e:
        logger.warning(f"Could not fetch next earnings date for {ticker}: {e}")
        return None
