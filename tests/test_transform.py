from __future__ import annotations

import pandas as pd

from bing_ads_etl.transform import _natural_id, transform
from bing_ads_etl.validate import validate


def test_natural_id_strips_bing_brackets():
    assert _natural_id("[2709719083]") == "2709719083"
    assert _natural_id("2709719083") == "2709719083"


def test_validate_and_transform(tmp_path, monkeypatch):
    from bing_ads_etl.config import Settings

    source = tmp_path / "source.csv"
    source.write_text(
        "Date,Customer,Account number,Account name,Account status,Campaign name,"
        "Campaign status,Ad group ID,Ad group,Ad group status,Ad ID,Ad description,"
        "Ad distribution,Ad status,Ad title,Ad type,Tracking Template,Custom Parameters,"
        "Final Mobile URL,Final URL,Top vs. other,Display URL,Final App URL,Destination URL,"
        "Device type,Device OS,Delivered match type,BidMatchType,Language,Network,"
        "Currency code,Impressions,Clicks,Spend,Avg. position,Conversions,Assists\n"
        "1/11/2026,43235,X00086N2,atlanta-ATT,Active,Brand,Active,[2709719083],AT&T,Active,"
        "[6772783690],We're hiring,Search,Active,Jobs,Text ad,,,,http://example.com,"
        "Bing and Yahoo! search - Top,att.jobs,,,Computer,Windows,Exact,Exact,English,"
        "Bing and Yahoo! search,USD,10,2,1.5,1.2,1,0\n"
    )
    settings = Settings(
        project_root=tmp_path,
        source_csv=source,
        staging_dir=tmp_path / "staging",
        schema_sql=tmp_path / "schema.sql",
        mysql_host="127.0.0.1",
        mysql_port=3307,
        mysql_user="ads",
        mysql_password="ads",
        mysql_database="bing_ads_dw",
    )
    settings.staging_dir.mkdir()
    extracted = pd.read_csv(source, dtype=str, keep_default_na=False)
    extracted.to_pickle(settings.extracted_path)

    validate(settings)
    transform(settings)

    fact = pd.read_pickle(settings.transformed_dir / "fact_ad_performance.pkl")
    ads = pd.read_pickle(settings.transformed_dir / "dim_ad.pkl")
    assert len(fact) == 1
    assert ads.iloc[0]["ad_nk"] == "6772783690"
    assert float(fact.iloc[0]["position_weighted_sum"]) == 12.0
