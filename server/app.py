from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Annotated

from delivery.webhook import DiscordWebhook
from server.db import (
    get_all_tickers,
    upsert_user_subscriptions,
    delete_subscriptions_by_webhook,
    get_recent_alerts,
)
from utils.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="BSEC Alerts")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    tickers = await get_all_tickers()
    return templates.TemplateResponse(request, "index.html", {"tickers": tickers})


@app.post("/subscribe", response_class=HTMLResponse)
async def subscribe(
    request: Request,
    name: Annotated[str, Form()],
    webhook_url: Annotated[str, Form()],
    tickers: Annotated[list[str], Form()] = [],
):
    all_tickers = await get_all_tickers()

    if not tickers:
        return templates.TemplateResponse(request, "index.html", {
            "tickers": all_tickers,
            "error": "Select at least one ticker.",
            "name": name,
            "webhook_url": webhook_url,
        })

    await upsert_user_subscriptions(name, webhook_url, tickers)

    # Catch-up: deliver any alerts fired in the last hour for newly subscribed tickers
    catchup_sent = 0
    webhook = DiscordWebhook(webhook_url)
    for ticker in tickers:
        for alert in await get_recent_alerts(ticker, hours=1):
            try:
                await webhook.send(alert)
                catchup_sent += 1
            except Exception as e:
                logger.error(f"Catch-up delivery failed for {ticker}: {e}")

    msg = f"Subscribed to {len(tickers)} ticker(s)."
    if catchup_sent:
        msg += f" {catchup_sent} recent alert(s) delivered to your Discord."

    return templates.TemplateResponse(request, "index.html", {
        "tickers": all_tickers,
        "success": msg,
        "name": name,
        "webhook_url": webhook_url,
        "selected_tickers": tickers,
    })


@app.post("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(
    request: Request,
    webhook_url: Annotated[str, Form()],
):
    removed = await delete_subscriptions_by_webhook(webhook_url)
    tickers = await get_all_tickers()
    return templates.TemplateResponse(request, "index.html", {
        "tickers": tickers,
        "info": "Unsubscribed successfully." if removed else "No subscription found for that webhook URL.",
    })
