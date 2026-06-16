# pipeline/dispatcher.py
import asyncio
from workers.client import AIClient
from workers.extraction_worker import run_extraction
from workers.sentiment_worker import run_sentiment
from workers.risk_worker import run_risk

async def analyze_filing(text: str, client: AIClient) -> dict:
    """Run all three AI workers concurrently. Total time = slowest worker, not sum."""
    extraction, sentiment, risk = await asyncio.gather(
        run_extraction(text, client),
        run_sentiment(text, client),
        run_risk(text, client),
        return_exceptions=True,
    )
    return {
        "extraction": extraction if not isinstance(extraction, Exception) else {},
        "sentiment": sentiment if not isinstance(sentiment, Exception) else {},
        "risk": risk if not isinstance(risk, Exception) else {},
    }
