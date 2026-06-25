import logging

HEADERS = {
    "User-Agent": "BSEC example@email.com",  # REQUIRED by SEC
}

MAX_FILING_AGE_HOURS = 1       # reject filings older than this at detection time
POST_FILING_COOLDOWN_HOURS = 4  # pause between detections per company

LOGGING_LEVEL = logging.INFO