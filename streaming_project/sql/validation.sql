SELECT * FROM <catalog>.<schema>.stream_bronze_trade_events
ORDER BY _ingested_at DESC;

SELECT
  symbol,
  COUNT(*) AS trades,
  SUM(trade_value) AS gross_value
FROM <catalog>.<schema>.stream_silver_trades
GROUP BY symbol
ORDER BY gross_value DESC;

DESCRIBE HISTORY <catalog>.<schema>.stream_silver_trades;
