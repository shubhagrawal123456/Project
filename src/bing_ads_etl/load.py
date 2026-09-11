from __future__ import annotations

from typing import Any, Iterable

import pandas as pd
import pymysql
from pymysql.connections import Connection

from bing_ads_etl.config import Settings, get_settings

DIM_LOAD_ORDER = [
    "dim_date",
    "dim_account",
    "dim_campaign",
    "dim_ad_group",
    "dim_ad",
    "dim_device",
    "dim_placement",
]


def connect(settings: Settings) -> Connection:
    return pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def apply_schema(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    statements = _split_sql(settings.schema_sql.read_text())
    conn = connect(settings)
    try:
        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        conn.commit()
    finally:
        conn.close()


def _split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for raw_line in sql.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        buffer.append(raw_line)
        if line.endswith(";"):
            statement = "\n".join(buffer).strip().rstrip(";")
            if statement:
                statements.append(statement)
            buffer = []
    trailing = "\n".join(buffer).strip().rstrip(";")
    if trailing:
        statements.append(trailing)
    return statements


def load(settings: Settings | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    transformed = settings.transformed_dir
    conn = connect(settings)
    counts: dict[str, int] = {}
    try:
        with conn.cursor() as cursor:
            for table in DIM_LOAD_ORDER:
                frame = pd.read_pickle(transformed / f"{table}.pkl")
                counts[table] = _upsert_dimension(cursor, table, frame)
            fact = pd.read_pickle(transformed / "fact_ad_performance.pkl")
            counts["fact_ad_performance"] = _replace_facts(cursor, fact)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return counts


def _upsert_dimension(cursor: Any, table: str, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    upsert = {
        "dim_date": (
            """
            INSERT INTO dim_date (
                date_key, full_date, year, quarter, month, month_name,
                week_of_year, day, day_name, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                year = VALUES(year),
                quarter = VALUES(quarter),
                month = VALUES(month),
                month_name = VALUES(month_name),
                week_of_year = VALUES(week_of_year),
                day = VALUES(day),
                day_name = VALUES(day_name),
                is_weekend = VALUES(is_weekend)
            """,
            [
                "date_key",
                "full_date",
                "year",
                "quarter",
                "month",
                "month_name",
                "week_of_year",
                "day",
                "day_name",
                "is_weekend",
            ],
        ),
        "dim_account": (
            """
            INSERT INTO dim_account (customer_id, account_nk, account_name, account_status)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                customer_id = VALUES(customer_id),
                account_name = VALUES(account_name),
                account_status = VALUES(account_status)
            """,
            ["customer_id", "account_nk", "account_name", "account_status"],
        ),
        "dim_campaign": (
            """
            INSERT INTO dim_campaign (account_nk, campaign_name, campaign_status)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE campaign_status = VALUES(campaign_status)
            """,
            ["account_nk", "campaign_name", "campaign_status"],
        ),
        "dim_ad_group": (
            """
            INSERT INTO dim_ad_group (
                ad_group_nk, ad_group_name, ad_group_status, account_nk, campaign_name
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ad_group_name = VALUES(ad_group_name),
                ad_group_status = VALUES(ad_group_status),
                account_nk = VALUES(account_nk),
                campaign_name = VALUES(campaign_name)
            """,
            ["ad_group_nk", "ad_group_name", "ad_group_status", "account_nk", "campaign_name"],
        ),
        "dim_ad": (
            """
            INSERT INTO dim_ad (
                ad_nk, ad_group_nk, ad_title, ad_description, ad_type, ad_status,
                ad_distribution, tracking_template, custom_parameters, final_url,
                final_mobile_url, final_app_url, destination_url, display_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ad_group_nk = VALUES(ad_group_nk),
                ad_title = VALUES(ad_title),
                ad_description = VALUES(ad_description),
                ad_type = VALUES(ad_type),
                ad_status = VALUES(ad_status),
                ad_distribution = VALUES(ad_distribution),
                tracking_template = VALUES(tracking_template),
                custom_parameters = VALUES(custom_parameters),
                final_url = VALUES(final_url),
                final_mobile_url = VALUES(final_mobile_url),
                final_app_url = VALUES(final_app_url),
                destination_url = VALUES(destination_url),
                display_url = VALUES(display_url)
            """,
            [
                "ad_nk",
                "ad_group_nk",
                "ad_title",
                "ad_description",
                "ad_type",
                "ad_status",
                "ad_distribution",
                "tracking_template",
                "custom_parameters",
                "final_url",
                "final_mobile_url",
                "final_app_url",
                "destination_url",
                "display_url",
            ],
        ),
        "dim_device": (
            """
            INSERT INTO dim_device (device_type, device_os)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE device_type = VALUES(device_type)
            """,
            ["device_type", "device_os"],
        ),
        "dim_placement": (
            """
            INSERT INTO dim_placement (
                network, auction_slot, language, delivered_match_type, bid_match_type, currency_code
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE currency_code = VALUES(currency_code)
            """,
            [
                "network",
                "auction_slot",
                "language",
                "delivered_match_type",
                "bid_match_type",
                "currency_code",
            ],
        ),
    }
    sql, columns = upsert[table]
    rows = [_row_values(frame, columns, idx) for idx in frame.index]
    cursor.executemany(sql, rows)
    return len(rows)


def _replace_facts(cursor: Any, frame: pd.DataFrame) -> int:
    cursor.execute("DELETE FROM fact_ad_performance")
    cursor.execute("DELETE FROM stg_fact_ad_performance")
    columns = [
        "report_date",
        "account_nk",
        "campaign_name",
        "ad_group_nk",
        "ad_nk",
        "device_type",
        "device_os",
        "network",
        "auction_slot",
        "language",
        "delivered_match_type",
        "bid_match_type",
        "currency_code",
        "impressions",
        "clicks",
        "spend",
        "conversions",
        "assists",
        "position_weighted_sum",
    ]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_stg = f"INSERT INTO stg_fact_ad_performance ({', '.join(columns)}) VALUES ({placeholders})"
    rows = [_row_values(frame, columns, idx) for idx in frame.index]
    cursor.executemany(insert_stg, rows)
    cursor.execute(
        """
        INSERT INTO fact_ad_performance (
            date_key, account_key, campaign_key, ad_group_key, ad_key,
            device_key, placement_key, impressions, clicks, spend,
            conversions, assists, position_weighted_sum
        )
        SELECT
            d.date_key,
            a.account_key,
            c.campaign_key,
            g.ad_group_key,
            ad.ad_key,
            dv.device_key,
            p.placement_key,
            s.impressions,
            s.clicks,
            s.spend,
            s.conversions,
            s.assists,
            s.position_weighted_sum
        FROM stg_fact_ad_performance s
        JOIN dim_date d ON d.full_date = s.report_date
        JOIN dim_account a ON a.account_nk = s.account_nk
        JOIN dim_campaign c
            ON c.account_nk = s.account_nk AND c.campaign_name = s.campaign_name
        JOIN dim_ad_group g ON g.ad_group_nk = s.ad_group_nk
        JOIN dim_ad ad ON ad.ad_nk = s.ad_nk
        JOIN dim_device dv
            ON dv.device_type = s.device_type AND dv.device_os = s.device_os
        JOIN dim_placement p
            ON p.network = s.network
           AND p.auction_slot = s.auction_slot
           AND p.language = s.language
           AND p.delivered_match_type = s.delivered_match_type
           AND p.bid_match_type = s.bid_match_type
           AND p.currency_code = s.currency_code
        """
    )
    cursor.execute("SELECT COUNT(*) AS n FROM fact_ad_performance")
    loaded = cursor.fetchone()["n"]
    if loaded != len(frame):
        raise RuntimeError(
            f"Fact load lookup failed: expected {len(frame)} rows, inserted {loaded}. "
            "A dimension natural key did not match."
        )
    return loaded


def _row_values(frame: pd.DataFrame, columns: Iterable[str], idx: Any) -> tuple[Any, ...]:
    return tuple(_py(frame.at[idx, col]) for col in columns)


def _py(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value
