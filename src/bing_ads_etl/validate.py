from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bing_ads_etl.config import Settings, get_settings

REQUIRED_KEYS = [
    "Date",
    "Customer",
    "Account number",
    "Campaign name",
    "Ad group ID",
    "Ad ID",
    "Device type",
    "Device OS",
    "Delivered match type",
    "BidMatchType",
    "Network",
    "Top vs. other",
    "Language",
]

NUMERIC_COLUMNS = {
    "Impressions": "int",
    "Clicks": "int",
    "Spend": "float",
    "Avg. position": "float",
    "Conversions": "int",
    "Assists": "int",
}


def _to_numeric(series: pd.Series, kind: str) -> pd.Series:
    cleaned = series.replace("", pd.NA)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    if kind == "int":
        return numeric.astype("Int64")
    return numeric.astype("Float64")


def validate(settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    frame = pd.read_pickle(settings.extracted_path)
    issues: list[dict[str, object]] = []

    empty_required = {
        col: int((frame[col].astype(str).str.strip() == "").sum())
        for col in REQUIRED_KEYS
    }
    for col, count in empty_required.items():
        if count:
            issues.append({"severity": "error", "field": col, "empty_count": count})

    parsed_dates = pd.to_datetime(frame["Date"], format="%m/%d/%Y", errors="coerce")
    bad_dates = int(parsed_dates.isna().sum())
    if bad_dates:
        issues.append({"severity": "error", "field": "Date", "invalid_count": bad_dates})

    for col, kind in NUMERIC_COLUMNS.items():
        numeric = _to_numeric(frame[col], kind)
        invalid = int(numeric.isna().sum() - (frame[col].astype(str).str.strip() == "").sum())
        if col != "Avg. position" and int(numeric.isna().sum()):
            issues.append({"severity": "error", "field": col, "invalid_or_empty": int(numeric.isna().sum())})
        elif invalid:
            issues.append({"severity": "error", "field": col, "invalid_count": invalid})
        frame[col] = numeric

        if kind == "int":
            negatives = int((numeric.fillna(0) < 0).sum())
            if negatives:
                issues.append({"severity": "error", "field": col, "negative_count": negatives})

    clicks_gt_impr = int((frame["Clicks"].fillna(0) > frame["Impressions"].fillna(0)).sum())
    if clicks_gt_impr:
        issues.append({"severity": "warning", "field": "Clicks", "clicks_gt_impressions": clicks_gt_impr})

    conversions_gt_clicks = int((frame["Conversions"].fillna(0) > frame["Clicks"].fillna(0)).sum())
    if conversions_gt_clicks:
        issues.append(
            {
                "severity": "warning",
                "field": "Conversions",
                "conversions_gt_clicks": conversions_gt_clicks,
                "note": "Possible because conversions can lag clicks; kept for the Insights Agent.",
            }
        )

    errors = [issue for issue in issues if issue["severity"] == "error"]
    report = {
        "row_count": int(len(frame)),
        "issues": issues,
        "passed": not errors,
    }
    report_path = settings.staging_dir / "validation_report.json"
    report_path.write_text(json.dumps(report, indent=2))

    if errors:
        raise ValueError(f"Validation failed. See {report_path}")

    frame["report_date"] = parsed_dates.dt.date
    frame.to_pickle(settings.validated_path)
    return settings.validated_path
