CREATE DATABASE IF NOT EXISTS airflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON bing_ads_dw.* TO 'airflow'@'%';
GRANT ALL PRIVILEGES ON bing_ads_dw.* TO 'ads'@'%';
FLUSH PRIVILEGES;
