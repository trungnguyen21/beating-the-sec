import pytest
from unittest.mock import AsyncMock, patch

from workers.extraction_worker import run_extraction
from workers.sentiment_worker import run_sentiment
from workers.risk_worker import run_risk

SAMPLE_PRESS_RELEASE = """
MegaCorp Inc. Reports Q3 2025 Financial Results

SAN JOSE, Calif., Sept 15, 2025 — MegaCorp Inc. today reported financial results for its third quarter ended August 31, 2025.

Revenue was $10.2 billion, beating analyst estimates of $9.8 billion.
Earnings per share (EPS) was $1.20, beating estimates of $1.15.
Net income was $1.5 billion.

"We are thrilled with our strong performance this quarter, driven by unprecedented demand for our AI products," said John Doe, CEO. "However, we remain cautious about macroeconomic headwinds in Europe."

Guidance for the next quarter is raised to $10.5 - $11.0 billion in revenue.
Due to restructuring in our hardware division, we expect to lay off approximately 5% of our workforce.
"""

@pytest.fixture
def mocked_client():
    with patch("workers.client.AIClient.invoke", new_callable=AsyncMock) as client:
        client.invoke.return_value = {"result": "your_mocked_value"}
        yield client


@pytest.mark.e2e
async def test_extraction_worker(mocked_client):
    result = await run_extraction(SAMPLE_PRESS_RELEASE, mocked_client)
    assert isinstance(result, dict)


@pytest.mark.e2e
async def test_sentiment_worker(mocked_client):
    result = await run_sentiment(SAMPLE_PRESS_RELEASE, mocked_client)
    assert isinstance(result, dict)


@pytest.mark.e2e
async def test_risk_worker(mocked_client):
    result = await run_risk(SAMPLE_PRESS_RELEASE, mocked_client)
    assert isinstance(result, dict)
