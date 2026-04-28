-- Funnel drop-off analysis: where do users abandon the journey?
-- Run against Trino: lakehouse catalog, gold schema

-- Yesterday's funnel overview
SELECT
    event_type,
    SUM(event_count)                                    AS total_events,
    SUM(success_count)                                  AS successful,
    ROUND(SUM(success_count) * 1.0 / SUM(event_count), 4) AS success_rate,
    SUM(unique_users)                                   AS unique_users
FROM lakehouse.gold.funnel_hourly
WHERE date_trunc('day', hour) = current_date - INTERVAL '1' DAY
GROUP BY event_type
ORDER BY total_events DESC;


-- Funnel by app version (identify which version converts best)
SELECT
    app_version,
    event_type,
    SUM(event_count)  AS total_events,
    SUM(unique_users) AS unique_users
FROM lakehouse.gold.funnel_hourly
WHERE date_trunc('day', hour) = current_date - INTERVAL '1' DAY
GROUP BY app_version, event_type
ORDER BY app_version, total_events DESC;


-- iOS vs Android conversion comparison
SELECT
    device_os,
    event_type,
    SUM(event_count)  AS total_events,
    SUM(unique_users) AS unique_users
FROM lakehouse.gold.funnel_hourly
WHERE date_trunc('day', hour) >= current_date - INTERVAL '7' DAY
GROUP BY device_os, event_type
ORDER BY device_os, total_events DESC;
