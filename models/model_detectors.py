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
    detected_at: datetime.datetime
    source: DetectorSource