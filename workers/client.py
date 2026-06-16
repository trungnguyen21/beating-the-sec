import json
import os
from typing import Protocol, runtime_checkable
from google import genai
from utils.logger import get_logger

logger = get_logger(__name__)


@runtime_checkable
class AIClient(Protocol):
    async def invoke(self, prompt: str, text: str, max_tokens: int = 1000) -> dict: ...


class GeminiClient:
    def __init__(self, api_key: str | None = None, model: str = "gemini-2.5-flash"):
        self._client = genai.Client(api_key=api_key or os.environ.get("GEMINI_API_KEY"))
        self._model = model

    async def invoke(self, prompt: str, text: str, max_tokens: int = 1000) -> dict:
        try:
            response = await self._client.aio.models.generate_content(
                model=self._model,
                contents=prompt + text[:8000],
                config=genai.types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    response_mime_type="application/json",
                ),
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Gemini invocation failed: {e}")
            return {}
