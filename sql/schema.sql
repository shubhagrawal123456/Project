-- Bing Ads star schema
-- Grain of fact_ad_performance:
--   one row per date + ad + device + placement (network, auction slot, match types, language, currency)
-- That matches the source file: 12,497 rows and 12,497 distinct grain keys.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT NOT NULL,
    full_date       DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         TINYINT NOT NULL,
    month           TINYINT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week_of_year    TINYINT NOT NULL,
    day             TINYINT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      TINYINT(1) NOT NULL,
    PRIMARY KEY (date_key),
    UNIQUE KEY uk_dim_date_full_date (full_date)
);

CREATE TABLE IF NOT EXISTS dim_account (
    account_key     INT NOT NULL AUTO_INCREMENT,
    customer_id     VARCHAR(32) NOT NULL,
    account_nk      VARCHAR(64) NOT NULL,
    account_name    VARCHAR(255) NOT NULL,
    account_status  VARCHAR(64) NOT NULL,
    PRIMARY KEY (account_key),
    UNIQUE KEY uk_dim_account_nk (account_nk)
);

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key      INT NOT NULL AUTO_INCREMENT,
    account_nk        VARCHAR(64) NOT NULL,
    campaign_name     VARCHAR(255) NOT NULL,
    campaign_status   VARCHAR(64) NOT NULL,
    PRIMARY KEY (campaign_key),
    UNIQUE KEY uk_dim_campaign (account_nk, campaign_name)
);

CREATE TABLE IF NOT EXISTS dim_ad_group (
    ad_group_key      INT NOT NULL AUTO_INCREMENT,
    ad_group_nk       VARCHAR(64) NOT NULL,
    ad_group_name     VARCHAR(255) NOT NULL,
    ad_group_status   VARCHAR(64) NOT NULL,
    account_nk        VARCHAR(64) NOT NULL,
    campaign_name     VARCHAR(255) NOT NULL,
    PRIMARY KEY (ad_group_key),
    UNIQUE KEY uk_dim_ad_group_nk (ad_group_nk)
);

CREATE TABLE IF NOT EXISTS dim_ad (
    ad_key              INT NOT NULL AUTO_INCREMENT,
    ad_nk               VARCHAR(64) NOT NULL,
    ad_group_nk         VARCHAR(64) NOT NULL,
    ad_title            VARCHAR(512) NULL,
    ad_description      VARCHAR(1024) NULL,
    ad_type             VARCHAR(64) NULL,
    ad_status           VARCHAR(64) NULL,
    ad_distribution     VARCHAR(64) NULL,
    tracking_template   VARCHAR(1024) NULL,
    custom_parameters   VARCHAR(1024) NULL,
    final_url           VARCHAR(2048) NULL,
    final_mobile_url    VARCHAR(2048) NULL,
    final_app_url       VARCHAR(2048) NULL,
    destination_url     VARCHAR(2048) NULL,
    display_url         VARCHAR(512) NULL,
    PRIMARY KEY (ad_key),
    UNIQUE KEY uk_dim_ad_nk (ad_nk)
);

CREATE TABLE IF NOT EXISTS dim_device (
    device_key    INT NOT NULL AUTO_INCREMENT,
    device_type   VARCHAR(64) NOT NULL,
    device_os     VARCHAR(64) NOT NULL,
    PRIMARY KEY (device_key),
    UNIQUE KEY uk_dim_device (device_type, device_os)
);

CREATE TABLE IF NOT EXISTS dim_placement (
    placement_key           INT NOT NULL AUTO_INCREMENT,
    network                 VARCHAR(128) NOT NULL,
    auction_slot            VARCHAR(128) NOT NULL,
    language                VARCHAR(64) NOT NULL,
    delivered_match_type    VARCHAR(64) NOT NULL,
    bid_match_type          VARCHAR(64) NOT NULL,
    currency_code           CHAR(3) NOT NULL,
    PRIMARY KEY (placement_key),
    UNIQUE KEY uk_dim_placement (
        network, auction_slot, language, delivered_match_type, bid_match_type, currency_code
    )
);

CREATE TABLE IF NOT EXISTS fact_ad_performance (
    fact_key                BIGINT NOT NULL AUTO_INCREMENT,
    date_key                INT NOT NULL,
    account_key             INT NOT NULL,
    campaign_key            INT NOT NULL,
    ad_group_key            INT NOT NULL,
    ad_key                  INT NOT NULL,
    device_key              INT NOT NULL,
    placement_key           INT NOT NULL,
    impressions             INT NOT NULL,
    clicks                  INT NOT NULL,
    spend                   DECIMAL(18,4) NOT NULL,
    conversions             INT NOT NULL,
    assists                 INT NOT NULL,
    position_weighted_sum   DECIMAL(18,4) NULL,
    PRIMARY KEY (fact_key),
    UNIQUE KEY uk_fact_grain (
        date_key, ad_key, device_key, placement_key
    ),
    KEY idx_fact_date (date_key),
    KEY idx_fact_account (account_key),
    KEY idx_fact_campaign (campaign_key),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_account FOREIGN KEY (account_key) REFERENCES dim_account (account_key),
    CONSTRAINT fk_fact_campaign FOREIGN KEY (campaign_key) REFERENCES dim_campaign (campaign_key),
    CONSTRAINT fk_fact_ad_group FOREIGN KEY (ad_group_key) REFERENCES dim_ad_group (ad_group_key),
    CONSTRAINT fk_fact_ad FOREIGN KEY (ad_key) REFERENCES dim_ad (ad_key),
    CONSTRAINT fk_fact_device FOREIGN KEY (device_key) REFERENCES dim_device (device_key),
    CONSTRAINT fk_fact_placement FOREIGN KEY (placement_key) REFERENCES dim_placement (placement_key)
);

CREATE TABLE IF NOT EXISTS stg_fact_ad_performance (
    report_date             DATE NOT NULL,
    account_nk              VARCHAR(64) NOT NULL,
    campaign_name           VARCHAR(255) NOT NULL,
    ad_group_nk             VARCHAR(64) NOT NULL,
    ad_nk                   VARCHAR(64) NOT NULL,
    device_type             VARCHAR(64) NOT NULL,
    device_os               VARCHAR(64) NOT NULL,
    network                 VARCHAR(128) NOT NULL,
    auction_slot            VARCHAR(128) NOT NULL,
    language                VARCHAR(64) NOT NULL,
    delivered_match_type    VARCHAR(64) NOT NULL,
    bid_match_type          VARCHAR(64) NOT NULL,
    currency_code           CHAR(3) NOT NULL,
    impressions             INT NOT NULL,
    clicks                  INT NOT NULL,
    spend                   DECIMAL(18,4) NOT NULL,
    conversions             INT NOT NULL,
    assists                 INT NOT NULL,
    position_weighted_sum   DECIMAL(18,4) NULL
);

CREATE OR REPLACE VIEW vw_ad_performance AS
SELECT
    d.full_date,
    d.year,
    d.month,
    d.day_name,
    a.customer_id,
    a.account_nk,
    a.account_name,
    c.campaign_name,
    c.campaign_status,
    g.ad_group_name,
    ad.ad_title,
    ad.ad_type,
    ad.display_url,
    dv.device_type,
    dv.device_os,
    p.network,
    p.auction_slot,
    p.delivered_match_type,
    p.bid_match_type,
    p.currency_code,
    f.impressions,
    f.clicks,
    f.spend,
    f.conversions,
    f.assists,
    CASE WHEN f.impressions > 0 THEN f.clicks / f.impressions END AS ctr,
    CASE WHEN f.clicks > 0 THEN f.spend / f.clicks END AS cpc,
    CASE WHEN f.conversions > 0 THEN f.spend / f.conversions END AS cost_per_conversion,
    CASE WHEN f.impressions > 0 THEN f.position_weighted_sum / f.impressions END AS avg_position
FROM fact_ad_performance f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_account a ON a.account_key = f.account_key
JOIN dim_campaign c ON c.campaign_key = f.campaign_key
JOIN dim_ad_group g ON g.ad_group_key = f.ad_group_key
JOIN dim_ad ad ON ad.ad_key = f.ad_key
JOIN dim_device dv ON dv.device_key = f.device_key
JOIN dim_placement p ON p.placement_key = f.placement_key;
