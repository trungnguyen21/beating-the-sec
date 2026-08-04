import secrets

from fastapi import Depends, FastAPI, HTTPException, Request, Form, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Annotated

from config import ADMIN_PASSWORD, ADMIN_USERNAME
from server.db import (
    get_all_tickers,
    get_bot_stats,
    list_guilds,
    ban_guild,
    unban_guild,
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


# --- Admin dashboard -------------------------------------------------------
# Subscriptions are entirely managed via the Discord bot's /setup and /watch commands now — see
# docs/DISCORD_INTEGRATION.md. This dashboard only offers ticker-universe management, visibility
# into real guilds, and the ban/unban kill switch — no direct editing of a guild's watchlist,
# since that's a Discord-only action for the guild's own admin, not something to do on their
# behalf from here.

@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, _: Annotated[None, Depends(require_admin)]):
    tickers = await get_all_tickers()
    guilds = await list_guilds()
    stats = await get_bot_stats()
    return templates.TemplateResponse(
        request, "admin.html", {"tickers": tickers, "guilds": guilds, "stats": stats}
    )


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


@app.post("/admin/guilds/{guild_id}/ban", response_class=RedirectResponse)
async def admin_ban_guild(
    _: Annotated[None, Depends(require_admin)],
    guild_id: str,
    reason: Annotated[str, Form()] = "",
):
    await ban_guild(guild_id, reason)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/guilds/{guild_id}/unban", response_class=RedirectResponse)
async def admin_unban_guild(
    _: Annotated[None, Depends(require_admin)],
    guild_id: str,
):
    await unban_guild(guild_id)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)
