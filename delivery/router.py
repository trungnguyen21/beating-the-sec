"""
Fan out delivery service for all users who subscribed to a ticker
"""

from typing import List

from delivery.webhook import DiscordWebhook
from utils.logger import get_logger
from models.users import User

logger = get_logger(__name__)

class SubscriptionRouter:
    def __init__(self, subscription_map: dict[str, List[User]]):
        self._map = subscription_map

    async def fanout(self, ticker: str, alert: dict) -> None:
        subscribers = self._map.get(ticker, [])
        if not subscribers:
            logger.warning(f"No subscribers for {ticker}")
            return
        for sub in subscribers:
            try:
                await DiscordWebhook(sub.webhook).send(alert)
                logger.info(f"  ✓ {sub.id}")
            except Exception as e:
                logger.debug(str(e))
                logger.error(f"  ✗ {sub.id}: {e}")
