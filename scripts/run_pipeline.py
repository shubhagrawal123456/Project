#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bing_ads_etl.run import run_pipeline


if __name__ == "__main__":
    counts = run_pipeline()
    print(counts)
