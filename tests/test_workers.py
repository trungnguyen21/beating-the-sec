import asyncio
import sys
import os
import json

# Add root directory to python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.extraction_worker import run_extraction
from workers.sentiment_worker import run_sentiment
from workers.risk_worker import run_risk

async def test_ai_workers():
    sample_press_release = """
    MegaCorp Inc. Reports Q3 2025 Financial Results
    
    SAN JOSE, Calif., Sept 15, 2025 — MegaCorp Inc. today reported financial results for its third quarter ended August 31, 2025.
    
    Revenue was $10.2 billion, beating analyst estimates of $9.8 billion.
    Earnings per share (EPS) was $1.20, beating estimates of $1.15.
    Net income was $1.5 billion.
    
    "We are thrilled with our strong performance this quarter, driven by unprecedented demand for our AI products," said John Doe, CEO. "However, we remain cautious about macroeconomic headwinds in Europe."
    
    Guidance for the next quarter is raised to $10.5 - $11.0 billion in revenue.
    Due to restructuring in our hardware division, we expect to lay off approximately 5% of our workforce.
    """
    
    print("Testing Extraction Worker...")
    try:
        ext_res = await run_extraction(sample_press_release)
        print(json.dumps(ext_res, indent=2))
    except Exception as e:
        print(f"Extraction failed: {e}")
        
    print("\nTesting Sentiment Worker...")
    try:
        sent_res = await run_sentiment(sample_press_release)
        print(json.dumps(sent_res, indent=2))
    except Exception as e:
        print(f"Sentiment failed: {e}")

    print("\nTesting Risk Worker...")
    try:
        risk_res = await run_risk(sample_press_release)
        print(json.dumps(risk_res, indent=2))
    except Exception as e:
        print(f"Risk failed: {e}")

if __name__ == "__main__":
    # Note: Ensure your workers/client.py is configured properly before running this!
    # If client is None, it will return the mocked empty dicts.
    asyncio.run(test_ai_workers())
