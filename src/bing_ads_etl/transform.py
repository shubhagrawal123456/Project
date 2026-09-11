from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from bing_ads_etl.config import Settings, get_settings

BRACKETED_ID = re.compile(r"^\[(.*)\]$")


def _natural_id(value: object) -> str:
    text = str(value).strip()
    match = BRACKETED_ID.match(text)
    return match.group(1) if match else text


def _blank_to_none(series: pd.Series) -> pd.Series:
    stripped = series.astype(str).str.strip()
    return stripped.mask(stripped.eq(""), None)


def transform(settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    frame = pd.read_pickle(settings.validated_path).copy()
    out_dir = settings.transformed_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    frame["account_nk"] = frame["Account number"].str.strip()
    frame["ad_group_nk"] = frame["Ad group ID"].map(_natural_id)
    frame["ad_nk"] = frame["Ad ID"].map(_natural_id)
    frame["customer_id"] = frame["Customer"].str.strip()
    frame["campaign_name"] = frame["Campaign name"].str.strip()
    frame["account_name"] = frame["Account name"].str.strip()
    frame["ad_group_name"] = frame["Ad group"].str.strip()

    for col in [
        "Account status",
        "Campaign status",
        "Ad group status",
        "Ad status",
        "Ad description",
        "Ad distribution",
        "Ad title",
        "Ad type",
        "Tracking Template",
        "Custom Parameters",
        "Final Mobile URL",
        "Final URL",
        "Display URL",
        "Final App URL",
        "Destination URL",
        "Device type",
        "Device OS",
        "Delivered match type",
        "BidMatchType",
        "Language",
        "Network",
        "Top vs. other",
        "Currency code",
    ]:
        frame[col] = _blank_to_none(frame[col])

    impressions = frame["Impressions"].astype("float")
    avg_position = frame["Avg. position"].astype("float")
    frame["position_weighted_sum"] = impressions * avg_position

    dim_date = _build_dim_date(frame["report_date"])
    dim_account = (
        frame[["customer_id", "account_nk", "account_name", "Account status"]]
        .drop_duplicates()
        .rename(columns={"Account status": "account_status"})
        .sort_values("account_nk")
        .reset_index(drop=True)
    )
    dim_campaign = (
        frame[["account_nk", "campaign_name", "Campaign status"]]
        .drop_duplicates()
        .rename(columns={"Campaign status": "campaign_status"})
        .sort_values(["account_nk", "campaign_name"])
        .reset_index(drop=True)
    )
    dim_ad_group = (
        frame[["ad_group_nk", "ad_group_name", "Ad group status", "account_nk", "campaign_name"]]
        .drop_duplicates()
        .rename(columns={"Ad group status": "ad_group_status"})
        .sort_values("ad_group_nk")
        .reset_index(drop=True)
    )
    dim_ad = (
        frame[
            [
                "ad_nk",
                "ad_group_nk",
                "Ad title",
                "Ad description",
                "Ad type",
                "Ad status",
                "Ad distribution",
                "Tracking Template",
                "Custom Parameters",
                "Final URL",
                "Final Mobile URL",
                "Final App URL",
                "Destination URL",
                "Display URL",
            ]
        ]
        .drop_duplicates(subset=["ad_nk"])
        .rename(
            columns={
                "Ad title": "ad_title",
                "Ad description": "ad_description",
                "Ad type": "ad_type",
                "Ad status": "ad_status",
                "Ad distribution": "ad_distribution",
                "Tracking Template": "tracking_template",
                "Custom Parameters": "custom_parameters",
                "Final URL": "final_url",
                "Final Mobile URL": "final_mobile_url",
                "Final App URL": "final_app_url",
                "Destination URL": "destination_url",
                "Display URL": "display_url",
            }
        )
        .sort_values("ad_nk")
        .reset_index(drop=True)
    )
    dim_device = (
        frame[["Device type", "Device OS"]]
        .drop_duplicates()
        .rename(columns={"Device type": "device_type", "Device OS": "device_os"})
        .sort_values(["device_type", "device_os"])
        .reset_index(drop=True)
    )
    dim_placement = (
        frame[
            [
                "Network",
                "Top vs. other",
                "Language",
                "Delivered match type",
                "BidMatchType",
                "Currency code",
            ]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "Network": "network",
                "Top vs. other": "auction_slot",
                "Language": "language",
                "Delivered match type": "delivered_match_type",
                "BidMatchType": "bid_match_type",
                "Currency code": "currency_code",
            }
        )
        .sort_values(["network", "auction_slot", "delivered_match_type", "bid_match_type"])
        .reset_index(drop=True)
    )

    fact = frame[
        [
            "report_date",
            "account_nk",
            "campaign_name",
            "ad_group_nk",
            "ad_nk",
            "Device type",
            "Device OS",
            "Network",
            "Top vs. other",
            "Language",
            "Delivered match type",
            "BidMatchType",
            "Currency code",
            "Impressions",
            "Clicks",
            "Spend",
            "Conversions",
            "Assists",
            "position_weighted_sum",
        ]
    ].rename(
        columns={
            "Device type": "device_type",
            "Device OS": "device_os",
            "Network": "network",
            "Top vs. other": "auction_slot",
            "Language": "language",
            "Delivered match type": "delivered_match_type",
            "BidMatchType": "bid_match_type",
            "Currency code": "currency_code",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "Spend": "spend",
            "Conversions": "conversions",
            "Assists": "assists",
        }
    )

    tables = {
        "dim_date": dim_date,
        "dim_account": dim_account,
        "dim_campaign": dim_campaign,
        "dim_ad_group": dim_ad_group,
        "dim_ad": dim_ad,
        "dim_device": dim_device,
        "dim_placement": dim_placement,
        "fact_ad_performance": fact,
    }
    for name, table in tables.items():
        table.to_pickle(out_dir / f"{name}.pkl")

    return out_dir


def _build_dim_date(dates: pd.Series) -> pd.DataFrame:
    unique_dates = pd.to_datetime(sorted(set(dates)))
    return pd.DataFrame(
        {
            "date_key": unique_dates.strftime("%Y%m%d").astype(int),
            "full_date": unique_dates.date,
            "year": unique_dates.year,
            "quarter": unique_dates.quarter,
            "month": unique_dates.month,
            "month_name": unique_dates.strftime("%B"),
            "week_of_year": unique_dates.isocalendar().week.astype(int),
            "day": unique_dates.day,
            "day_name": unique_dates.strftime("%A"),
            "is_weekend": unique_dates.dayofweek.isin([5, 6]),
        }
    )
