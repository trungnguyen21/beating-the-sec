# delivery/webhook.py
import httpx

async def send_webhook(alert: dict, endpoint: str, secret: str):
    import hmac, hashlib, json, time
    body = json.dumps(alert)
    sig = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    
    async with httpx.AsyncClient() as client:
        await client.post(
            endpoint,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Signature": sig,
                "X-Timestamp": str(int(time.time())),
            }
        )