# workers/risk_worker.py
from workers.client import AIClient

RISK_PROMPT = """You are a risk analysis AI for institutional investors.
Scan this earnings press release for material risk signals.
Return ONLY valid JSON, no other text.

{
  "risk_level": <"low" | "medium" | "high" | "critical">,
  "flags": [
    {
      "type": <"guidance_cut" | "layoffs" | "restatement" | "legal" |
                "macro_headwind" | "customer_concentration" | "debt" | "other">,
      "description": <string, one sentence>,
      "severity": <"low" | "medium" | "high">
    }
  ],
  "guidance_cut_detected": <boolean>,
  "restructuring_detected": <boolean>,
  "legal_language_detected": <boolean>,
  "going_concern_detected": <boolean>
}

Press release:
"""

async def run_risk(text: str, client: AIClient) -> dict:
    return await client.invoke(RISK_PROMPT, text)
