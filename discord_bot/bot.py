# discord_bot/bot.py
"""
Gateway bot serving /setup and /watch add|remove|list — see docs/DISCORD_INTEGRATION.md for the
full design rationale. Guild-broadcast only: an admin picks a channel, curates the server's
watchlist, and every member in that channel sees every alert. No per-member personalization.
"""
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands

from config import DISCORD_BOT_TOKEN, DISCORD_DEV_GUILD_ID
from server.db import (
    GuildNotSetUpError,
    add_guild_tickers,
    delete_user,
    get_upcoming_calendar,
    get_guild_watchlist,
    is_guild_banned,
    log_command,
    remove_guild_tickers,
    upsert_guild_channel,
)
from utils.logger import get_logger

logger = get_logger(__name__)

_ADMIN_ONLY = app_commands.default_permissions(manage_guild=True)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


@asynccontextmanager
async def _tracked(interaction: discord.Interaction, command: str, args: str):
    """Every command runs through this — the single place that writes to bot_command_log, so
    a new command can't forget to log itself. success=False only on an unhandled exception; a
    friendly "you did something wrong" reply (banned, not set up, unknown ticker) still counts
    as a successfully handled invocation."""
    guild_id = str(interaction.guild_id) if interaction.guild_id else None
    user_id = str(interaction.user.id)
    success = True
    try:
        yield
    except Exception:
        success = False
        raise
    finally:
        try:
            await log_command(guild_id, user_id, command, args, success)
        except Exception:
            logger.exception("Failed to log Discord command")


def _parse_tickers(raw: str) -> list[str]:
    return [p.strip().upper() for p in raw.replace(",", " ").split() if p.strip()]


def _format_add_result(result: dict) -> str:
    lines = []
    if result["added"]:
        lines.append(f"✅ Added: {', '.join(result['added'])}")
    if result["already_watched"]:
        lines.append(f"Already watched: {', '.join(result['already_watched'])}")
    if result["unknown"]:
        lines.append(f"⚠️ Unknown ticker(s), not added: {', '.join(result['unknown'])}")
    return "\n".join(lines) if lines else "Nothing to add."


@bot.tree.command(name="setup", description="Set the channel earnings alerts get posted to")
@app_commands.describe(channel="Channel to post alerts in")
@_ADMIN_ONLY
async def setup_command(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
    guild_id = str(interaction.guild_id)
    async with _tracked(interaction, "/setup", f"channel={channel.id}"):
        if await is_guild_banned(guild_id):
            await interaction.response.send_message(
                "This server has been banned from using this bot.", ephemeral=True
            )
            return

        try:
            webhook = await channel.create_webhook(name="BSEC Earnings Alerts")
        except discord.Forbidden:
            await interaction.response.send_message(
                "I need the 'Manage Webhooks' permission in that channel to do this.",
                ephemeral=True,
            )
            return

        await upsert_guild_channel(guild_id, interaction.guild.name, webhook.url)
        await interaction.response.send_message(
            f"Alerts will be posted to {channel.mention}.", ephemeral=True
        )


_DISCORD_MESSAGE_LIMIT = 2000
_TITLE_COL_WIDTH = 28


def _format_calendar(upcoming: list[dict]) -> str:
    if not upcoming:
        return "No companies in the calendar are reporting in the next 24h."

    rows = []
    for c in upcoming:
        target = datetime.fromisoformat(c["target"]).astimezone(ZoneInfo("America/New_York"))
        when = target.strftime("%a %I:%M %p ET") if c["time_known"] else f"{target.strftime('%a')} TBD"
        title = c["title"] if len(c["title"]) <= _TITLE_COL_WIDTH else c["title"][:_TITLE_COL_WIDTH - 1] + "…"
        rows.append((c["ticker"], title, when))

    ticker_w = max(len("TICKER"), max(len(r[0]) for r in rows))
    title_w = max(len("COMPANY"), max(len(r[1]) for r in rows))
    time_w = max(len("REPORTS"), max(len(r[2]) for r in rows))

    def _fmt_row(a: str, b: str, c: str) -> str:
        return f"{a:<{ticker_w}}  {b:<{title_w}}  {c:<{time_w}}"

    header = f"{len(upcoming)} companies reporting in the next 24h:\n"
    table_lines = [_fmt_row("TICKER", "COMPANY", "REPORTS"), _fmt_row("-" * ticker_w, "-" * title_w, "-" * time_w)]
    table_lines += [_fmt_row(*r) for r in rows]

    def _wrap(lines: list[str]) -> str:
        return header + "```\n" + "\n".join(lines) + "\n```"

    if len(_wrap(table_lines)) <= _DISCORD_MESSAGE_LIMIT:
        return _wrap(table_lines)

    # Truncate to fit Discord's message cap rather than erroring the interaction — still shows
    # the soonest reporters (already sorted, table_lines[:2] is the header/divider) plus a count
    # of what got cut off.
    fixed = table_lines[:2]
    kept = []
    for line in table_lines[2:]:
        omitted = len(table_lines) - 2 - len(kept) - 1
        footer = f"\n… and {omitted} more." if omitted > 0 else ""
        if len(_wrap(fixed + kept + [line])) + len(footer) > _DISCORD_MESSAGE_LIMIT:
            break
        kept.append(line)
    omitted = len(table_lines) - 2 - len(kept)
    return _wrap(fixed + kept) + (f"\n… and {omitted} more." if omitted > 0 else "")


@bot.tree.command(name="calendar", description="Show all companies reporting earnings in the next 24h")
async def calendar_command(interaction: discord.Interaction) -> None:
    async with _tracked(interaction, "/calendar", ""):
        upcoming = await get_upcoming_calendar()
        await interaction.response.send_message(_format_calendar(upcoming), ephemeral=True)


watch_group = app_commands.Group(
    name="watch",
    description="Manage this server's earnings watchlist",
    default_permissions=discord.Permissions(manage_guild=True),
)


@watch_group.command(name="add", description="Add tickers to this server's watchlist")
@app_commands.describe(tickers="Space or comma separated tickers, e.g. AAPL MSFT")
async def watch_add(interaction: discord.Interaction, tickers: str) -> None:
    guild_id = str(interaction.guild_id)
    async with _tracked(interaction, "/watch add", tickers):
        try:
            result = await add_guild_tickers(guild_id, _parse_tickers(tickers))
        except GuildNotSetUpError:
            await interaction.response.send_message(
                "Run /setup first to pick an alerts channel.", ephemeral=True
            )
            return
        await interaction.response.send_message(_format_add_result(result), ephemeral=True)


@watch_group.command(name="remove", description="Remove tickers from this server's watchlist")
@app_commands.describe(tickers="Space or comma separated tickers, e.g. AAPL MSFT")
async def watch_remove(interaction: discord.Interaction, tickers: str) -> None:
    guild_id = str(interaction.guild_id)
    async with _tracked(interaction, "/watch remove", tickers):
        try:
            removed = await remove_guild_tickers(guild_id, _parse_tickers(tickers))
        except GuildNotSetUpError:
            await interaction.response.send_message(
                "Run /setup first to pick an alerts channel.", ephemeral=True
            )
            return
        msg = f"Removed: {', '.join(removed)}" if removed else "None of those were being watched."
        await interaction.response.send_message(msg, ephemeral=True)


@watch_group.command(name="list", description="Show this server's current watchlist")
async def watch_list(interaction: discord.Interaction) -> None:
    guild_id = str(interaction.guild_id)
    async with _tracked(interaction, "/watch list", ""):
        watchlist = await get_guild_watchlist(guild_id)
        msg = ", ".join(watchlist) if watchlist else "No tickers watched yet — try /watch add."
        await interaction.response.send_message(msg, ephemeral=True)


bot.tree.add_command(watch_group)


def _build_help_embed() -> discord.Embed:
    embed = discord.Embed(
        title="BSEC Earnings Alerts — Commands",
        description="Low-latency 8-K earnings alerts, posted straight to a channel here.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="🔧 Setup (admin only)",
        value="`/setup #channel` — pick where alerts get posted. Run this first.",
        inline=False,
    )
    embed.add_field(
        name="👀 Watchlist (admin only)",
        value=(
            "`/watch add AAPL MSFT` — add tickers\n"
            "`/watch remove AAPL` — remove tickers\n"
            "`/watch list` — show what's currently watched"
        ),
        inline=False,
    )
    embed.add_field(
        name="📅 Calendar",
        value="`/calendar` — every company reporting earnings in the next 24h",
        inline=False,
    )
    embed.add_field(name="❓ Help", value="`/help` — show this message again", inline=False)
    return embed


@app_commands.command(name="help", description="Show available BSEC bot commands")
async def help_command(interaction: discord.Interaction) -> None:
    async with _tracked(interaction, "/help", ""):
        await interaction.response.send_message(embed=_build_help_embed(), ephemeral=True)


bot.tree.add_command(help_command)


def _find_welcome_channel(guild: discord.Guild) -> discord.TextChannel | None:
    """Prefers the guild's configured system channel (where Discord itself posts join messages)
    if we can post there, otherwise falls back to the first text channel we have send perms in.
    Returns None if we can't post anywhere yet — plausible right after invite, before any
    channel permission is granted to the bot's role."""
    if guild.system_channel and guild.system_channel.permissions_for(guild.me).send_messages:
        return guild.system_channel
    for channel in guild.text_channels:
        if channel.permissions_for(guild.me).send_messages:
            return channel
    return None


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    """Best-effort welcome message so an admin doesn't have to discover /setup by guessing —
    silently does nothing if we can't find a postable channel (still fully usable via /help)."""
    channel = _find_welcome_channel(guild)
    if channel is None:
        logger.info(f"Joined guild {guild.id} ({guild.name}) — no postable channel for a welcome message")
        return
    try:
        await channel.send(
            content="👋 Thanks for adding **BSEC Earnings Alerts**! An admin should run `/setup` to get started.",
            embed=_build_help_embed(),
        )
    except discord.Forbidden:
        logger.info(f"Joined guild {guild.id} ({guild.name}) — welcome message forbidden")


@bot.event
async def on_guild_remove(guild: discord.Guild) -> None:
    """Does NOT touch banned_guilds — a kicked bot must not clear a ban, since dodging a ban by
    kicking and re-inviting the bot is exactly the abuse case the ban exists for."""
    removed = await delete_user(str(guild.id))
    if removed:
        logger.info(f"Removed from guild {guild.id} ({guild.name}) — deactivated its watchlist")


@bot.event
async def on_ready() -> None:
    if DISCORD_DEV_GUILD_ID:
        guild_obj = discord.Object(id=int(DISCORD_DEV_GUILD_ID))
        bot.tree.copy_global_to(guild=guild_obj)
        await bot.tree.sync(guild=guild_obj)
        logger.info(f"Synced commands to dev guild {DISCORD_DEV_GUILD_ID} (instant propagation)")
    else:
        await bot.tree.sync()
        logger.info("Synced commands globally (may take up to ~1hr to propagate)")
    logger.info(f"Logged in as {bot.user} — connected to {len(bot.guilds)} guild(s)")


async def run_bot() -> None:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN not set — see docs/DISCORD_INTEGRATION.md")
    async with bot:
        await bot.start(DISCORD_BOT_TOKEN)
