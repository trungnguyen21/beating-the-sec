import os
import sys

# Repo root on sys.path so tests can `import ingestion.X`, `import pipeline.Y`, etc.
# without each test file needing its own sys.path.append boilerplate.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
