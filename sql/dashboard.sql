-- Datasets for a Databricks AI/BI (Lakeview) dashboard or Power BI DirectQuery.
-- Replace rx_prod.supply with your catalog.schema.

-- name: kpi_tiles
SELECT SUM(units_shipped) / SUM(units_ordered)                     AS unit_fill_rate,
       SUM(on_time_rate * order_lines) / SUM(order_lines)          AS on_time_rate,
       SUM(revenue)                                                AS revenue,
       SUM(orders)                                                 AS orders
FROM rx_prod.supply.gold_agg_daily_kpis
WHERE order_date >= date_sub(current_date(), 30);

-- name: daily_trend_by_dc
SELECT order_date, dc_id, unit_fill_rate, on_time_rate, revenue
FROM rx_prod.supply.gold_agg_daily_kpis
ORDER BY order_date, dc_id;

-- name: fill_rate_anomalies  (3 sigma below the DC's trailing 28-day mean, at least 3 points)
WITH s AS (
  SELECT order_date, dc_id, order_lines, unit_fill_rate,
         AVG(unit_fill_rate)         OVER w AS trailing_mean,
         STDDEV_SAMP(unit_fill_rate) OVER w AS trailing_std,
         COUNT(*)                    OVER w AS history_days,
         AVG(order_lines)            OVER w AS avg_lines
  FROM rx_prod.supply.gold_agg_daily_kpis
  WINDOW w AS (PARTITION BY dc_id ORDER BY order_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING)
)
SELECT order_date, dc_id, unit_fill_rate, trailing_mean,
       (unit_fill_rate - trailing_mean) / trailing_std AS z_score
FROM s
WHERE history_days >= 14 AND trailing_std > 0 AND order_lines >= 0.6 * avg_lines
  AND trailing_mean - unit_fill_rate >= 0.03
  AND (unit_fill_rate - trailing_mean) / trailing_std < -3;

-- name: revenue_by_therapeutic_class  (star-schema join)
SELECT p.therapeutic_class, d.year, d.month, SUM(f.shipped_amount) AS revenue
FROM rx_prod.supply.gold_fact_order_line f
JOIN rx_prod.supply.gold_dim_product p ON p.product_key = f.product_key
JOIN rx_prod.supply.gold_dim_date d    ON d.date_key    = f.order_date_key
GROUP BY ALL;

-- name: stockout_watchlist  (ranked by revenue at risk, not units)
SELECT r.dc_id, r.ndc, r.product_name, r.therapeutic_class, r.requires_cold_chain,
       r.days_of_supply, r.risk_level,
       r.avg_daily_demand * p.unit_cost AS daily_cost_at_risk
FROM rx_prod.supply.gold_agg_stockout_risk r
JOIN rx_prod.supply.gold_dim_product p USING (ndc)
WHERE r.risk_level <> 'LOW'
ORDER BY daily_cost_at_risk DESC;

-- name: data_quality_quarantine
SELECT reason, COUNT(*) AS lines
FROM rx_prod.supply.silver_order_lines_quarantine
LATERAL VIEW explode(dq_reasons) AS reason
GROUP BY reason ORDER BY lines DESC;
