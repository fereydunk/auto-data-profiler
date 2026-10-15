"""
Adds classifier-service, kafka-pipeline, and profiler to sys.path so tests
can import from all three without installing them as packages.

Note: profiler/main.py and review-api/main.py share the module name "main".
      test_profiler_api.py and test_review_api.py each reload the module cache
      locally via their own _load_*() helpers to avoid conflicts.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "classifier-service"))
sys.path.insert(0, str(ROOT / "kafka-pipeline"))
