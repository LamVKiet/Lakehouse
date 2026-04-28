-- App health analysis: error rates by app version
-- Run against Trino: lakehouse catalog, gold schema

-- Error rate comparison across app versions (yesterday)
SELECT
    app_version,
    SUM(total_count)                                    AS total_events,
    SUM(error_count)                                    AS errors,
    ROUND(SUM(error_count) * 1.0 / SUM(total_count), 4) AS error_rate
FROM lakehouse.gold.app_health_hourly
WHERE date_trunc('day', hour) = current_date - INTERVAL '1' DAY
GROUP BY app_version
ORDER BY error_rate DESC;


-- Payment failure rate by app version
SELECT
    app_version,
    SUM(total_count) AS payment_attempts,
    SUM(error_count) AS failures,
    ROUND(SUM(error_count) * 1.0 / SUM(total_count), 4) AS failure_rate
FROM lakehouse.gold.app_health_hourly
WHERE event_type = 'payment_failed'
  AND date_trunc('day', hour) >= current_date - INTERVAL '7' DAY
GROUP BY app_version
ORDER BY failure_rate DESC;


-- Hourly error spike detection (past 24 hours)
SELECT
    hour,
    app_version,
    event_type,
    total_count,
    error_count,
    ROUND(error_count * 1.0 / total_count, 4) AS error_rate
FROM lakehouse.gold.app_health_hourly
WHERE date_trunc('day', hour) = current_date - INTERVAL '1' DAY
  AND error_count > 0
ORDER BY error_rate DESC
LIMIT 20;
