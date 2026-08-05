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

import pytest
from dotenv import load_dotenv

load_dotenv()

from aggregator.aggregator import build_alert
from delivery.router import SQLiteSubscriptionRouter
from server.db import (
    add_guild_tickers,
    create_ticker,
    delete_ticker,
    delete_user,
    upsert_guild_channel,
)
from tests.alert_helpers import assert_valid_alert, sample_results

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

    # Built via the real build_alert() — not a hand-copied literal — so this test always sends
    # whatever the current alert schema actually is. A hardcoded dict here would keep passing
    # (and keep posting a stale message to Discord) even after the schema changes, which is
    # exactly the bug this test exists to catch.
    alert = build_alert(
        ticker=TEST_TICKER,
        results=sample_results(),
        detected_at="2026-08-03T16:01:04+00:00",
        filed_at="2026-08-03T16:01:00+00:00",
        filing_url="https://www.sec.gov/Archives/edgar/data/0000000000/000000000000000000/0000000000-26-000000-index.htm",
    )
    assert_valid_alert(alert)
    alert["headline"] = f"🧪 TEST ALERT — DO NOT ACT ON THIS — {alert['headline']}"

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
        # Don't leave the seeded test guild/ticker in the shared Postgres DB. delete_ticker cascades
        # subscriptions; delete_user removes the guild row itself.
        await delete_user(TEST_GUILD_ID)
        await delete_ticker(TEST_TICKER)
