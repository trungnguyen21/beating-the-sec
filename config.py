import logging
import os
from dotenv import load_dotenv

load_dotenv()

HEADERS = {
    "User-Agent": "BSEC example@email.com",  # REQUIRED by SEC
}

MAX_FILING_AGE_HOURS = 1       # reject filings older than this at detection time
POST_FILING_COOLDOWN_HOURS = 4  # pause between detections per company

LOGGING_LEVEL = logging.INFO

CONSUMER_CONCURRENCY = 4  # max filings processed simultaneously (each uses 3 AI calls)

KAFKA_BOOTSTRAP    = os.getenv("CONFLUENT_BOOTSTRAP")
KAFKA_API_KEY      = os.getenv("CONFLUENT_API_KEY")
KAFKA_API_SECRET   = os.getenv("CONFLUENT_API_SECRET")
KAFKA_RAW_TOPIC    = "raw-filings"
KAFKA_RESULTS_TOPIC = "filing-results"
KAFKA_DLQ_TOPIC    = "filing-dlq"
KAFKA_AI_GROUP     = "ai-workers"
KAFKA_DELIVERY_GROUP = "delivery-consumers"