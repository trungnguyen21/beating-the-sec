"""
Fan out delivery service for all users who subscribed to a ticker
"""

from typing import List

from delivery.webhook import DiscordWebhook, WebhookGoneError
from utils.logger import get_logger
from models.users import User
from server.db import delete_user, get_subscribers_for_ticker, save_alert

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


class SQLiteSubscriptionRouter:
    """Subscription routing backed by the SQLite subscribers table — same source of truth
    pipeline/kafka_consumer.py's run_delivery_consumer uses in production, so main.py's
    single-process dev mode fans out to the same real subscribers instead of file-based
    users.json. Also persists via save_alert so subscribers who join later can catch up,
    matching the production ai-worker path."""

    async def fanout(self, ticker: str, alert: dict) -> None:
        await save_alert(ticker, alert)
        subscribers = await get_subscribers_for_ticker(ticker)
        if not subscribers:
            logger.warning(f"No subscribers for {ticker}")
            return
        for sub in subscribers:
            try:
                await DiscordWebhook(sub["webhook_url"]).send(alert)
                logger.info(f"  ✓ {sub['id']}")
            except WebhookGoneError:
                logger.warning(f"  ✗ {sub['id']}: webhook gone — deactivating guild")
                await delete_user(sub["id"])
            except Exception as e:
                logger.debug(str(e))
                logger.error(f"  ✗ {sub['id']}: {e}")
