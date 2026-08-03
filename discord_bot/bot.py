# discord_bot/bot.py
"""
Gateway bot serving /setup and /watch add|remove|list — see docs/DISCORD_INTEGRATION.md for the
full design rationale. Guild-broadcast only: an admin picks a channel, curates the server's
watchlist, and every member in that channel sees every alert. No per-member personalization.
"""
from contextlib import asynccontextmanager

import discord
from discord import app_commands
from discord.ext import commands

from config import DISCORD_BOT_TOKEN, DISCORD_DEV_GUILD_ID
from server.db import (
    GuildNotSetUpError,
    add_guild_tickers,
    delete_user,
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
