import json
from utils.logger import get_logger

logger = get_logger(__name__)

# TODO: Initialize your AI client here. For example:
# from anthropic import AsyncAnthropic
# client = AsyncAnthropic()
client = None

async def invoke(prompt: str, text: str, max_tokens: int = 1000) -> dict:
    """
    Unified method to call the AI model and return parsed JSON.
    """
    if client is None:
        logger.warning("AI client is not initialized. Using a mock response.")
        return {}

    try:
        # If using `AsyncAnthropic`, this should be `await client.messages.create(...)`
        # If using a synchronous client, it will block the event loop.
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt + text[:8000]}]
        )
        return json.loads(response.content[0].text)
    except Exception as e:
        logger.error(f"AI extraction failed: {e}")
        return {}
