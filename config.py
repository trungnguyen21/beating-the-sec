import logging
import os
from dotenv import load_dotenv

load_dotenv()

HEADERS = {
    "User-Agent": "BSEC example@email.com",  # REQUIRED by SEC
}

MAX_FILING_AGE_HOURS = 1       # reject filings older than this at detection time
POST_FILING_COOLDOWN_HOURS = 4  # pause between detections per company

# SEC fair-access guideline is ~10 req/sec total from one IP. Split explicitly rather than
# letting the rotation scheduler silently consume the whole budget and starve document
# fetches / the backstop poller.
DETECTION_BUDGET = 7                  # req/sec reserved for rotation_scheduler.run_rotation
FETCH_BUDGET_HEADROOM = 3             # req/sec reserved for fetch_filing_document + backstop poller
BACKSTOP_POLL_INTERVAL_SECONDS = 45   # low-cadence secondary confirmation via edgar_poller

# Per-company poll budget inside a single rotation tick. asyncio.gather waits for the slowest
# task in the batch, so one slow/hanging SEC response otherwise stalls every other company in
# that tick — bounding it here keeps a single company's slowness from dragging down the batch,
# well below sec_rss.py's own 10s httpx request timeout (that timeout guards the request itself;
# this one guards the tick's cadence).
ROTATION_POLL_TIMEOUT_SECONDS = 3.0

LOGGING_LEVEL = logging.INFO

CONSUMER_CONCURRENCY = 4  # max filings processed simultaneously (each uses 3 AI calls)

KAFKA_BOOTSTRAP    = os.getenv("CONFLUENT_BOOTSTRAP")
KAFKA_API_KEY      = os.getenv("CONFLUENT_API_KEY")
KAFKA_API_SECRET   = os.getenv("CONFLUENT_API_SECRET")
KAFKA_RAW_TOPIC    = "raw_filings"
KAFKA_RESULTS_TOPIC = "filing_results"
KAFKA_DLQ_TOPIC    = "filing_dlq"
KAFKA_AI_GROUP     = "ai_workers"
KAFKA_DELIVERY_GROUP = "delivery_consumers"

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://bsec:bsec@localhost:5433/beatingthesec")

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
# Optional: a Discord guild ID to sync commands to instantly during development, instead of a
# global sync (which can take up to ~1hr to propagate). Unset in production — a global sync is
# required for the bot to work in servers other than this one.
DISCORD_DEV_GUILD_ID = os.getenv("DISCORD_DEV_GUILD_ID")