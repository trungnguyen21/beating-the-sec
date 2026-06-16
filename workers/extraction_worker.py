# workers/extraction_worker.py
from workers.client import AIClient

EXTRACTION_PROMPT = """You are a financial data extraction engine.
Extract the following from this earnings press release.
Return ONLY valid JSON, no other text.

{
  "eps_actual": <float or null>,
  "eps_estimate": <float or null>,
  "eps_beat": <"beat" | "miss" | "in-line" | null>,
  "revenue_actual_millions": <float or null>,
  "revenue_estimate_millions": <float or null>,
  "revenue_beat": <"beat" | "miss" | "in-line" | null>,
  "gross_margin_pct": <float or null>,
  "net_income_millions": <float or null>,
  "guidance_next_quarter_low": <float or null>,
  "guidance_next_quarter_high": <float or null>,
  "guidance_direction": <"raised" | "lowered" | "maintained" | "withdrawn" | null>,
  "fiscal_period": <string, e.g. "Q3 2025">,
  "report_date": <string>
}

Press release:
"""

async def run_extraction(text: str, client: AIClient) -> dict:
    return await client.invoke(EXTRACTION_PROMPT, text)
