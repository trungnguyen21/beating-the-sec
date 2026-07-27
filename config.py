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

LOGGING_LEVEL = logging.INFO

CONSUMER_CONCURRENCY = 4  # max filings processed simultaneously (each uses 3 AI calls)

KAFKA_BOOTSTRAP    = os.getenv("CONFLUENT_BOOTSTRAP")
KAFKA_API_KEY      = os.getenv("CONFLUENT_API_KEY")
KAFKA_API_SECRET   = os.getenv("CONFLUENT_API_SECRET")
KAFKA_RAW_TOPIC    = "raw_filings"
KAFKA_RESULTS_TOPIC = "filing_results"
KAFKA_DLQ_TOPIC    = "filing_dlq"
KAFKA_AI_GROUP     = "ai_workers"
KAFKA_DELIVERY_GROUP = "delivery-consumers"

DB_PATH = os.getenv("DB_PATH", "bsec.db")