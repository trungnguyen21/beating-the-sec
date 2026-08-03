"""
E2E test: Discord-bot-driven delivery — a guild subscription (as /setup + /watch add would
create it) fanned out via delivery/router.py's SQLiteSubscriptionRouter to a real Discord
webhook. See docs/DISCORD_INTEGRATION.md for the full design.

Discord delivery is opt-in and never targets real subscriber webhooks: set
TEST_DISCORD_WEBHOOK_URL to a dedicated test channel's webhook to exercise it. Without that var
set, this test is skipped outright — there's nothing else to assert here besides the send
itself. When it does send, the alert's headline is prefixed with a 🧪 TEST marker so it's
unmistakable even in a shared channel.

Uses a dedicated throwaway ticker (TEST_TICKER), not a real one like AAPL: fanout() delivers to
every guild subscribed to the ticker, and a real guild (e.g. one you tested /watch add AAPL
with by hand) would receive this test alert too if the test used a ticker any real guild might
actually be watching.
"""
import os
from datetime import datetime, timezone

import pytest
from dotenv import load_dotenv

load_dotenv()

from delivery.router import SQLiteSubscriptionRouter
from server.db import (
    add_guild_tickers,
    create_ticker,
    delete_ticker,
    delete_user,
    upsert_guild_channel,
)

TEST_DISCORD_WEBHOOK_URL = os.environ.get("TEST_DISCORD_WEBHOOK_URL")
TEST_GUILD_ID = "test-e2e-discord-bot-delivery-guild"  # fake Discord guild ID, not a real one
TEST_TICKER = "ZZTEST"  # dedicated throwaway ticker — never a real company, never a real subscriber


@pytest.mark.e2e
async def test_e2e_discord_bot_delivery():
    if not TEST_DISCORD_WEBHOOK_URL:
        pytest.skip(
            "Set TEST_DISCORD_WEBHOOK_URL to a dedicated test channel's webhook to verify "
            "bot-driven Discord delivery end-to-end — this test never fans out to real "
            "subscriber webhooks"
        )

    alert = {
        "ticker": TEST_TICKER,
        "filed_at": "2026-08-03T16:01:00+00:00",
        "detected_at": "2026-08-03T16:01:04+00:00",
        "alerted_at": datetime.now(timezone.utc).isoformat(),
        "feed_to_detection_seconds": 4.0,
        "signal_score": 0.78,
        "signal_label": "🟢 Bullish",
        "headline": (
            f"🧪 TEST ALERT — DO NOT ACT ON THIS — "
            f"{TEST_TICKER} beats on EPS and revenue, raises guidance"
        ),
        "eps": {"actual": 1.85, "estimate": 1.70, "result": "beat"},
        "revenue": {"actual_m": 94500, "estimate_m": 91200, "result": "beat"},
        "guidance": "raised",
        "risk_level": "low",
        "risk_flags": [],
        "tone": "bullish",
    }

    try:
        # Seed exactly what /setup + /watch add would produce, so this exercises the real
        # guild-subscription -> fanout path rather than a hand-built subscriber map. The ticker
        # itself is throwaway too (create_ticker), so fanout can't reach any real subscriber.
        await create_ticker(TEST_TICKER, cik="9999999999", title="Test Ticker (not a real company)")
        await upsert_guild_channel(
            TEST_GUILD_ID, "test-e2e-discord-bot-delivery", TEST_DISCORD_WEBHOOK_URL
        )
        await add_guild_tickers(TEST_GUILD_ID, [TEST_TICKER])

        router = SQLiteSubscriptionRouter()
        await router.fanout(TEST_TICKER, alert)
    finally:
        # Don't leave the seeded test guild/ticker in the shared bsec.db. delete_ticker cascades
        # subscriptions; delete_user removes the guild row itself.
        await delete_user(TEST_GUILD_ID)
        await delete_ticker(TEST_TICKER)
