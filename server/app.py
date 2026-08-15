import asyncio
import secrets
import time
import uuid

from fastapi import Body, Depends, FastAPI, HTTPException, Request, Form, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import Annotated

from config import ADMIN_PASSWORD, ADMIN_USERNAME
from server.db import (
    GuildNotSetUpError,
    get_all_tickers,
    get_api_stats,
    get_bot_stats,
    get_command_stats,
    get_upcoming_calendar,
    get_guild_watchlist,
    add_guild_tickers,
    log_api_request,
    remove_guild_tickers,
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


@app.middleware("http")
async def _log_api_timing(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = int((time.perf_counter() - start) * 1000)
    if request.url.path.startswith("/api/"):
        asyncio.create_task(
            log_api_request(request.method, request.url.path, response.status_code, duration_ms)
        )
    return response

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


# --- Stats dashboard --------------------------------------------------------

_ALLOWED_CONTAINERS = {
    "beating-sec-pipeline-1",
    "beating-sec-bot-1",
    "beating-sec-scheduler-1",
}


@app.get("/admin/stats", response_class=HTMLResponse)
async def admin_stats(request: Request, _: Annotated[None, Depends(require_admin)]):
    api_stats, discord_stats = await asyncio.gather(get_api_stats("1h"), get_command_stats("1h"))
    return templates.TemplateResponse(
        request, "stats.html", {
            "api_stats": api_stats,
            "discord_stats": discord_stats,
            "allowed_containers": sorted(_ALLOWED_CONTAINERS),
        }
    )


@app.get("/admin/stats/data")
async def admin_stats_data(
    _: Annotated[None, Depends(require_admin)],
    granularity: str = "1h",
):
    if granularity not in ("5min", "15min", "1h", "3h", "day"):
        raise HTTPException(status_code=400, detail="granularity must be one of: 5min, 15min, 1h, 3h, day")
    api_stats, discord_stats = await asyncio.gather(
        get_api_stats(granularity), get_command_stats(granularity)
    )
    return {"api": api_stats, "discord": discord_stats}


async def _stream_container_logs(container_name: str):
    try:
        import docker as docker_sdk
    except ImportError:
        yield "data: [error] docker package not installed — add docker>=7.0.0 to dependencies\n\n"
        return

    loop = asyncio.get_event_loop()
    try:
        client = docker_sdk.from_env()
        container = client.containers.get(container_name)
    except Exception as exc:
        yield f"data: [error] {exc}\n\n"
        return

    try:
        log_gen = container.logs(stream=True, follow=True, tail=200)
    except Exception as exc:
        yield f"data: [error] Could not stream logs: {exc}\n\n"
        return

    sentinel = object()
    while True:
        try:
            chunk = await loop.run_in_executor(None, next, log_gen, sentinel)
        except Exception as exc:
            yield f"data: [error] Stream interrupted: {exc}\n\n"
            break
        if chunk is sentinel:
            break
        line = chunk.decode("utf-8", errors="replace").rstrip()
        if line:
            yield f"data: {line}\n\n"


@app.get("/admin/logs/stream")
async def admin_logs_stream(
    _: Annotated[None, Depends(require_admin)],
    container: str = "beating-sec-pipeline-1",
):
    if container not in _ALLOWED_CONTAINERS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown container. Allowed: {sorted(_ALLOWED_CONTAINERS)}",
        )
    return StreamingResponse(
        _stream_container_logs(container),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- Public API endpoints ---------------------------------------------------
# Mirror the Discord slash commands as REST endpoints so guilds can be managed
# programmatically without going through the bot. /setup is intentionally excluded
# (webhook creation requires the Discord API, not just a DB write).

@app.get("/api/calendar")
async def api_calendar(hours: float = 24):
    """Upcoming earnings reports. Default window: next 24h. Max: 168h (7 days)."""
    hours = min(hours, 168)
    return await get_upcoming_calendar(hours)


@app.get("/api/guilds/{guild_id}/watch")
async def api_watch_list(guild_id: str):
    """Return the ticker watchlist for a guild."""
    return await get_guild_watchlist(guild_id)


@app.post("/api/guilds/{guild_id}/watch/add")
async def api_watch_add(
    guild_id: str,
    tickers: Annotated[list[str], Body(embed=True)],
):
    """
    Add tickers to a guild's watchlist.

    Body: `{"tickers": ["NVDA", "MSFT"]}`

    Returns which tickers were added, already watched, or unknown.
    """
    try:
        return await add_guild_tickers(guild_id, tickers)
    except GuildNotSetUpError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.post("/api/guilds/{guild_id}/watch/remove")
async def api_watch_remove(
    guild_id: str,
    tickers: Annotated[list[str], Body(embed=True)],
):
    """
    Remove tickers from a guild's watchlist.

    Body: `{"tickers": ["NVDA"]}`

    Returns the list of tickers actually removed.
    """
    try:
        removed = await remove_guild_tickers(guild_id, tickers)
        return {"removed": removed}
    except GuildNotSetUpError as e:
        raise HTTPException(status_code=409, detail=str(e))
