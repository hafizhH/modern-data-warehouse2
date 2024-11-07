-- Create database for currency analytics
CREATE DATABASE IF NOT EXISTS currency_analytics;
USE currency_analytics;

-- Create table for raw currency rates
CREATE EXTERNAL TABLE IF NOT EXISTS currency_rates (
    timestamp TIMESTAMP,
    rate DOUBLE
)
PARTITIONED BY (currency STRING)
STORED AS PARQUET
LOCATION '/user/warehouse/currency_rates';

-- Create table for currency forecasts
CREATE EXTERNAL TABLE IF NOT EXISTS currency_forecasts (
    timestamp TIMESTAMP,
    actual_rate DOUBLE,
    predicted_rate DOUBLE,
    currency STRING
)
STORED AS PARQUET
LOCATION '/user/warehouse/forecasts';

-- Create table for XML data
CREATE EXTERNAL TABLE IF NOT EXISTS xml_data (
    -- Add columns based on your XML structure
    xml_id STRING,
    xml_timestamp TIMESTAMP,
    xml_content STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/user/warehouse/xml_data';

-- Refresh partition metadata
MSCK REPAIR TABLE currency_rates;
MSCK REPAIR TABLE currency_forecasts;
MSCK REPAIR TABLE xml_data;

-- Create views for analytics

-- Daily average rates
CREATE VIEW daily_avg_rates AS
SELECT 
    currency,
    DATE(timestamp) as date,
    AVG(rate) as avg_rate,
    MIN(rate) as min_rate,
    MAX(rate) as max_rate,
    STDDEV(rate) as rate_volatility
FROM currency_rates
GROUP BY currency, DATE(timestamp);

-- Weekly trending analysis
CREATE VIEW weekly_trends AS
SELECT 
    currency,
    DATE_SUB(DATE(timestamp), POSEXPLODE(SPLIT(SPACE(6), ' '))) as date,
    AVG(rate) as avg_rate
FROM currency_rates
GROUP BY currency, DATE(timestamp)
HAVING date >= DATE_SUB(CURRENT_DATE, 7);

-- Forecast accuracy analysis
CREATE VIEW forecast_accuracy AS
SELECT 
    f.currency,
    DATE(f.timestamp) as date,
    f.actual_rate,
    f.predicted_rate,
    ABS(f.actual_rate - f.predicted_rate) as prediction_error,
    (ABS(f.actual_rate - f.predicted_rate) / f.actual_rate) * 100 as error_percentage
FROM currency_forecasts f;

-- Sample analytics queries

-- Get daily volatility ranking
SELECT 
    currency,
    date,
    rate_volatility,
    RANK() OVER (PARTITION BY date ORDER BY rate_volatility DESC) as volatility_rank
FROM daily_avg_rates
WHERE date >= DATE_SUB(CURRENT_DATE, 30);

-- Get correlation between currencies
SELECT 
    a.currency as currency1,
    b.currency as currency2,
    CORR(a.rate, b.rate) as correlation
FROM currency_rates a
JOIN currency_rates b 
ON DATE(a.timestamp) = DATE(b.timestamp)
WHERE a.currency < b.currency
GROUP BY a.currency, b.currency
HAVING correlation IS NOT NULL
ORDER BY correlation DESC;

-- Get forecast accuracy metrics
SELECT 
    currency,
    AVG(error_percentage) as avg_error_percentage,
    STDDEV(error_percentage) as error_std_dev,
    MIN(error_percentage) as min_error,
    MAX(error_percentage) as max_error
FROM forecast_accuracy
GROUP BY currency
ORDER BY avg_error_percentage;