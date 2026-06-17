import json
import os
from typing import Protocol, runtime_checkable
from google import genai
from utils.logger import get_logger

logger = get_logger(__name__)


@runtime_checkable
class AIClient(Protocol):
    async def invoke(self, prompt: str, text: str, max_tokens: int = 1000) -> dict: ...

    async def aclose(self) -> None:
        await self._client.aio.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_) -> None:
        await self.aclose()


class GeminiClient(AIClient):
    def __init__(self, api_key: str | None = None, model: str = "gemini-2.5-flash"):
        self._client = genai.Client(api_key=api_key or os.environ.get("GEMINI_API_KEY"))
        self._model = model

    async def invoke(self, prompt: str, text: str, max_tokens: int = 4096) -> dict:
        try:
            response = await self._client.aio.models.generate_content(
                model=self._model,
                contents=prompt + text[:8000],
                config=genai.types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    response_mime_type="application/json",
                ),
            )
            raw = response.text.strip()
            # Strip markdown code fences if the model adds them despite response_mime_type
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0]
            return json.loads(raw)
        except Exception as e:
            logger.error(f"Gemini invocation failed: {e}")
            return {}
