"""
Fan out delivery service for all users who subscribed to a ticker
"""

from typing import List
import threading

from webhook import DiscordWebhook
from utils.logger import get_logger
from models.users import User

logger = get_logger(__name__)

class SubscriptionRouter:
    def __init__(self, subscribers: List[User]):
        self.subscribers = subscribers

    def fanout(self, alert: dict) -> None:
        for sub in self.subscribers:
            webhook = DiscordWebhook(sub.webhook)
            thread = threading.Thread(target=self._send_webhook, args=(webhook, alert, sub.id))
            thread.daemon = True
            thread.start()

    def _send_webhook(self, webhook: DiscordWebhook, alert: dict, user_id: str) -> None:
        try:
            webhook.send(alert)
            logger.info(f"Delivered successfully for user {user_id}")
        except Exception as e:
            logger.debug(str(e))
            logger.info(f"Failed to deliver to user {user_id}")
