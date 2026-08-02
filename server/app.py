import secrets

from fastapi import Depends, FastAPI, HTTPException, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Annotated

from config import ADMIN_PASSWORD, ADMIN_USERNAME
from delivery.webhook import DiscordWebhook
from server.db import (
    get_all_tickers,
    upsert_user_subscriptions,
    delete_subscriptions_by_webhook,
    get_recent_alerts,
    get_all_users,
    create_user,
    update_user,
    delete_user,
    set_ticker_active,
    set_all_tickers_active,
    create_ticker,
    delete_ticker,
)
from utils.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="BSEC Alerts")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

security = HTTPBasic()


def require_admin(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> None:
    if not ADMIN_PASSWORD:
        raise HTTPException(status_code=503, detail="Admin dashboard not configured (ADMIN_PASSWORD unset)")
    valid_user = secrets.compare_digest(credentials.username, ADMIN_USERNAME)
    valid_pass = secrets.compare_digest(credentials.password, ADMIN_PASSWORD)
    if not (valid_user and valid_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


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


# --- Admin dashboard -------------------------------------------------------

@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, _: Annotated[None, Depends(require_admin)]):
    tickers = await get_all_tickers()
    users = await get_all_users()
    return templates.TemplateResponse(request, "admin.html", {"tickers": tickers, "users": users})


@app.post("/admin/tickers/toggle", response_class=RedirectResponse)
async def admin_toggle_ticker(
    _: Annotated[None, Depends(require_admin)],
    ticker: Annotated[str, Form()],
    active: Annotated[str, Form()],
):
    await set_ticker_active(ticker, active == "1")
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/tickers/bulk", response_class=RedirectResponse)
async def admin_bulk_tickers(
    _: Annotated[None, Depends(require_admin)],
    active: Annotated[str, Form()],
):
    await set_all_tickers_active(active == "1")
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/tickers", response_class=RedirectResponse)
async def admin_create_ticker(
    _: Annotated[None, Depends(require_admin)],
    ticker: Annotated[str, Form()],
    cik: Annotated[str, Form()],
    title: Annotated[str, Form()],
):
    created = await create_ticker(ticker, cik, title)
    if not created:
        raise HTTPException(status_code=409, detail="Ticker already exists")
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/tickers/{ticker}/delete", response_class=RedirectResponse)
async def admin_delete_ticker(
    _: Annotated[None, Depends(require_admin)],
    ticker: str,
):
    await delete_ticker(ticker)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/users", response_class=RedirectResponse)
async def admin_create_user(
    _: Annotated[None, Depends(require_admin)],
    name: Annotated[str, Form()],
    webhook_url: Annotated[str, Form()],
    tickers: Annotated[list[str], Form()] = [],
):
    await create_user(name, webhook_url, tickers)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/users/{user_id}", response_class=RedirectResponse)
async def admin_update_user(
    _: Annotated[None, Depends(require_admin)],
    user_id: str,
    name: Annotated[str, Form()],
    webhook_url: Annotated[str, Form()],
    tickers: Annotated[list[str], Form()] = [],
):
    found = await update_user(user_id, name, webhook_url, tickers)
    if not found:
        raise HTTPException(status_code=404, detail="User not found")
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/users/{user_id}/delete", response_class=RedirectResponse)
async def admin_delete_user(
    _: Annotated[None, Depends(require_admin)],
    user_id: str,
):
    await delete_user(user_id)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)
