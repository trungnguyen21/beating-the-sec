# delivery/webhook.py
import hmac
import hashlib
import json
import time
import httpx
from typing import Protocol

from utils.logger import get_logger

logger = get_logger(__name__)

_SIGNAL_COLOR = {
    "🟢 Bullish": 0x2ECC71,
    "🟡 Neutral": 0xF1C40F,
    "🔴 Bearish": 0xE74C3C,
}


class AlertDelivery(Protocol):
    async def send(self, alert: dict) -> None: ...


class WebhookDelivery:
    def __init__(self, endpoint: str, secret: str):
        self.endpoint = endpoint
        self._secret = secret

    async def send(self, alert: dict) -> None:
        body = json.dumps(alert)
        sig = hmac.new(self._secret.encode(), body.encode(), hashlib.sha256).hexdigest()
        async with httpx.AsyncClient() as client:
            await client.post(
                self.endpoint,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Signature": sig,
                    "X-Timestamp": str(int(time.time())),
                },
                timeout=10,
            )


class DiscordWebhook:
    def __init__(self, webhook_url: str):
        self.endpoint = webhook_url

    def _build_embed(self, alert: dict) -> dict:
        ticker = alert["ticker"]
        label = alert.get("signal_label", "🟡 Neutral")
        color = _SIGNAL_COLOR.get(label, 0xF1C40F)

        eps = alert.get("eps", {})
        rev = alert.get("revenue", {})

        def _fmt(val, prefix="$", suffix=""):
            return f"{prefix}{val}{suffix}" if val is not None else "N/A"

        fields = [
            {"name": "Signal",           "value": label,                                    "inline": True},
            {"name": "Tone",             "value": alert.get("tone", "N/A").capitalize(),   "inline": True},
            {"name": "Risk",             "value": alert.get("risk_level", "N/A").capitalize(), "inline": True},
            {"name": "EPS actual",       "value": _fmt(eps.get("actual")),                 "inline": True},
            {"name": "EPS estimate",     "value": _fmt(eps.get("estimate")),               "inline": True},
            {"name": "EPS result",       "value": eps.get("result") or "N/A",              "inline": True},
            {"name": "Revenue actual",   "value": _fmt(rev.get("actual_m"), suffix="M"),   "inline": True},
            {"name": "Revenue estimate", "value": _fmt(rev.get("estimate_m"), suffix="M"), "inline": True},
            {"name": "Revenue result",   "value": rev.get("result") or "N/A",              "inline": True},
            {"name": "Guidance",         "value": alert.get("guidance") or "N/A",          "inline": True},
        ]

        if alert.get("risk_flags"):
            fields.append({
                "name": "Risk flags",
                "value": "\n".join(f"• {f}" for f in alert["risk_flags"]),
                "inline": False,
            })

        return {
            "embeds": [{
                "title": f"{ticker} — Earnings Alert",
                "description": alert.get("headline", ""),
                "color": color,
                "fields": fields,
                "footer": {"text": f"Detected: {alert['detected_at']}  |  Alerted: {alert['alerted_at']}"},
            }]
        }

    async def send(self, alert: dict) -> None:
        payload = self._build_embed(alert)
        async with httpx.AsyncClient() as client:
            r = await client.post(self.endpoint, json=payload, timeout=10)
            if r.status_code not in (200, 204):
                logger.error(f"Discord webhook failed: {r.status_code} — {r.text}")
            else:
                logger.info(f"Discord alert sent for {alert['ticker']}")
