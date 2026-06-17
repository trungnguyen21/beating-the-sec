# pipeline/dispatcher.py
import asyncio
from workers.client import AIClient
from workers.extraction_worker import run_extraction
from workers.sentiment_worker import run_sentiment
from workers.risk_worker import run_risk


async def analyze_filing(sections: dict[str, str], client: AIClient,
                         estimates: dict | None = None) -> dict:
    """
    Run all three AI workers concurrently. Each worker receives the section most
    relevant to its task so the prompt stays focused and token-efficient.
    Total time = slowest worker, not sum.
    """
    extraction, sentiment, risk = await asyncio.gather(
        run_extraction(sections["financials"] or sections["full"], client, estimates),
        run_sentiment(sections["header"] or sections["full"], client),
        run_risk(sections["full"], client),
        return_exceptions=True,
    )
    return {
        "extraction": extraction if not isinstance(extraction, Exception) else {},
        "sentiment": sentiment if not isinstance(sentiment, Exception) else {},
        "risk": risk if not isinstance(risk, Exception) else {},
    }
