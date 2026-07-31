# aggregator/aggregator.py
from datetime import datetime, timezone

def compute_signal_score(results: dict) -> float:
    """
    Simple signal score: 0.0 (very bearish) to 1.0 (very bullish).
    Adjust weights to your strategy.
    """
    score = 0.5  # Neutral baseline
    
    ext = results.get("extraction", {})
    sent = results.get("sentiment", {})
    risk = results.get("risk", {})
    
    # EPS beat/miss
    if ext.get("eps_beat") == "beat":
        score += 0.15
    elif ext.get("eps_beat") == "miss":
        score -= 0.15
    
    # Revenue beat/miss
    if ext.get("revenue_beat") == "beat":
        score += 0.10
    elif ext.get("revenue_beat") == "miss":
        score -= 0.10
    
    # Guidance
    if ext.get("guidance_direction") == "raised":
        score += 0.15
    elif ext.get("guidance_direction") == "lowered":
        score -= 0.20
    elif ext.get("guidance_direction") == "withdrawn":
        score -= 0.10
    
    # Sentiment — scale tone contribution by confidence (0.0–1.0)
    tone_map = {"bullish": 0.15, "neutral": 0.0, "cautious": -0.05, "bearish": -0.20}
    confidence = 0.1
    if confidence:
        confidence = float(sent.get("confidence_score", 0.5))
    score += tone_map.get(sent.get("overall_tone", "neutral"), 0) * confidence
    
    # Risk
    if risk.get("guidance_cut_detected"):
        score -= 0.15
    if risk.get("going_concern_detected"):
        score -= 0.30
    risk_map = {"low": 0.05, "medium": 0.0, "high": -0.10, "critical": -0.25}
    score += risk_map.get(risk.get("risk_level", "medium"), 0)
    
    return round(max(0.0, min(1.0, score)), 2)

def build_alert(ticker: str, results: dict, detected_at: str) -> dict:
    score = compute_signal_score(results)
    ext = results.get("extraction", {})
    sent = results.get("sentiment", {})
    risk = results.get("risk", {})
    
    return {
        "ticker": ticker,
        "detected_at": detected_at,
        "alerted_at": datetime.now(timezone.utc).isoformat(),
        "signal_score": score,
        "signal_label": "🟢 Bullish" if score >= 0.65 else "🔴 Bearish" if score < 0.40 else "🟡 Neutral",
        "headline": sent.get("headline_signal", ""),
        "eps": {
            "actual": ext.get("eps_actual"),
            "estimate": ext.get("eps_estimate"),
            "result": ext.get("eps_beat"),
        },
        "revenue": {
            "actual_m": ext.get("revenue_actual_millions"),
            "estimate_m": ext.get("revenue_estimate_millions"),
            "result": ext.get("revenue_beat"),
        },
        "guidance": ext.get("guidance_direction"),
        "risk_level": risk.get("risk_level"),
        "risk_flags": [f["description"] for f in risk.get("flags", [])],
        "tone": sent.get("overall_tone"),
    }