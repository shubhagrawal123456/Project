from __future__ import annotations

from pathlib import Path

import pandas as pd

from bing_ads_etl.config import Settings, get_settings

EXPECTED_COLUMNS = [
    "Date",
    "Customer",
    "Account number",
    "Account name",
    "Account status",
    "Campaign name",
    "Campaign status",
    "Ad group ID",
    "Ad group",
    "Ad group status",
    "Ad ID",
    "Ad description",
    "Ad distribution",
    "Ad status",
    "Ad title",
    "Ad type",
    "Tracking Template",
    "Custom Parameters",
    "Final Mobile URL",
    "Final URL",
    "Top vs. other",
    "Display URL",
    "Final App URL",
    "Destination URL",
    "Device type",
    "Device OS",
    "Delivered match type",
    "BidMatchType",
    "Language",
    "Network",
    "Currency code",
    "Impressions",
    "Clicks",
    "Spend",
    "Avg. position",
    "Conversions",
    "Assists",
]


def extract(settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    settings.staging_dir.mkdir(parents=True, exist_ok=True)

    if not settings.source_csv.exists():
        raise FileNotFoundError(f"Source file not found: {settings.source_csv}")

    frame = pd.read_csv(settings.source_csv, dtype=str, keep_default_na=False)
    missing = [col for col in EXPECTED_COLUMNS if col not in frame.columns]
    if missing:
        raise ValueError(f"Source file is missing required columns: {missing}")

    frame = frame[EXPECTED_COLUMNS].copy()
    frame.to_pickle(settings.extracted_path)
    return settings.extracted_path
