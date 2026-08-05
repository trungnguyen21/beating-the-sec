"""
Docker-native replacement for the host crontab entry in docs/DEPLOYMENT_PLAN.md: runs
db.refresh_calendar once at startup (so a fresh DB isn't empty until tomorrow) and then once a
day at 05:00 ET, forever.

    python -m db.scheduler_loop
"""

import asyncio
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from config import DATABASE_URL
from db.refresh_calendar import refresh
from utils.logger import get_logger

logger = get_logger(__name__)

ET = ZoneInfo("America/New_York")
RUN_AT = time(5, 0)


def _seconds_until_next_run(now: datetime) -> float:
    target = datetime.combine(now.date(), RUN_AT, tzinfo=ET)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def main() -> None:
    await refresh(DATABASE_URL)
    while True:
        wait = _seconds_until_next_run(datetime.now(ET))
        logger.info(f"Next earnings-calendar refresh in {wait / 3600:.1f}h")
        await asyncio.sleep(wait)
        await refresh(DATABASE_URL)


if __name__ == "__main__":
    asyncio.run(main())
