import asyncio
from datetime import timezone
import yfinance as yf
from utils.logger import get_logger

logger = get_logger(__name__)


def _fetch_sync(ticker: str) -> dict:
    """Blocking yfinance call — run in a thread via asyncio.to_thread."""
    t = yf.Ticker(ticker)

    eps_est = None
    estimate_date = None
    rev_est = None

    # earnings_dates rows: upcoming quarters have NaN for Reported EPS.
    # The first such row is the current/next quarter being estimated.
    try:
        ed = t.earnings_dates
        if ed is not None and not ed.empty:
            upcoming = ed[ed["Reported EPS"].isna()]
            if not upcoming.empty:
                row = upcoming.iloc[0]
                eps_est = float(row["EPS Estimate"]) if row["EPS Estimate"] == row["EPS Estimate"] else None
                # Index is a tz-aware datetime — normalize to UTC date string
                estimate_date = upcoming.index[0].astimezone(timezone.utc).strftime("%Y-%m-%d")
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
