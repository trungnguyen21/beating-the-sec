"""
{
    "ticker": ticker,
    "cik": company["cik"],
    "accession": entry.get('accession-number', ''),
    "index_url": entry.link,
    "pipelines": pipelines,
    "detected_at": datetime.now(timezone.utc).isoformat(),
    "source": "sec_rss",
}
"""

import datetime
from enum import Enum
from typing import Optional
from dataclasses import dataclass


class DetectorSource(str, Enum):
    RSS_POLLER = "rss_poller"
    EDGAR_POLLER = "edgar_poller"

class ItemPipeline(str, Enum):
    """Classify filing type"""
    EARNINGS = "earnings"
    LEADERSHIP_CHANGE = "leadership_change"
    ACQUISITION = "acquisition"
    RESTATEMENT = "restatement"
    BANKRUPTCY = "bankruptcy"
    RESTRUCTURING = "restructuring"

@dataclass
class DetectorResult:
    ticker: str
    cik: str
    accession: Optional[str]
    index_url: str
    detected_at: str                               # UTC ISO-8601 string e.g. "2026-06-24T16:00:00+00:00"
    source: DetectorSource
    filed_at: Optional[datetime.datetime] = None  # tz-aware UTC datetime; None means "just appeared"
    raw_text: Optional[str] = None
    estimates: Optional[dict] = None