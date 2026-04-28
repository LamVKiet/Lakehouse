-- Revenue ranking: which movies earn the most?
-- Run against Trino: lakehouse catalog, gold schema

-- Top movies by revenue (past 7 days)
SELECT
    movie_name,
    SUM(tickets_sold)  AS total_tickets,
    SUM(total_revenue) AS revenue_vnd,
    SUM(unique_buyers) AS unique_buyers
FROM lakehouse.gold.movie_revenue_hourly
WHERE hour >= current_date - INTERVAL '7' DAY
GROUP BY movie_name
ORDER BY revenue_vnd DESC;


-- Hourly revenue pattern (identify peak hours for Flash Sales)
SELECT
    HOUR(hour)         AS hour_of_day,
    SUM(tickets_sold)  AS total_tickets,
    SUM(total_revenue) AS revenue_vnd
FROM lakehouse.gold.movie_revenue_hourly
WHERE date_trunc('day', hour) = current_date - INTERVAL '1' DAY
GROUP BY HOUR(hour)
ORDER BY hour_of_day;


-- Revenue trend over the past 30 days
SELECT
    CAST(date_trunc('day', hour) AS DATE) AS day,
    SUM(total_revenue) AS daily_revenue,
    SUM(tickets_sold)  AS daily_tickets
FROM lakehouse.gold.movie_revenue_hourly
WHERE hour >= current_date - INTERVAL '30' DAY
GROUP BY date_trunc('day', hour)
ORDER BY day;
