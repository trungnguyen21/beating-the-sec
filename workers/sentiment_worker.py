# workers/sentiment_worker.py
import json

SENTIMENT_PROMPT = """You are a financial analyst AI.
Analyze the tone and language of this earnings press release.
Return ONLY valid JSON, no other text.

{
  "overall_tone": <"bullish" | "neutral" | "cautious" | "bearish">,
  "confidence_score": <0.0 to 1.0>,
  "guidance_tone": <"optimistic" | "neutral" | "cautious" | "withdrawn">,
  "ceo_language_flags": [<list of notable phrases indicating sentiment>],
  "yoy_trend": <"improving" | "stable" | "declining" | "mixed">,
  "headline_signal": <one sentence plain English summary of the beat/miss and tone>,
  "notable_quotes": [<up to 2 direct quotes that capture management tone>]
}

Press release:
"""

from workers.client import invoke

async def run_sentiment(text: str) -> dict:
    return await invoke(SENTIMENT_PROMPT, text)