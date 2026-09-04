-- Streaming dashboard queries
SELECT COUNT(*) AS total_events FROM <catalog>.<schema>.stream_silver_trades;

SELECT ROUND(SUM(trade_value),2) AS gross_value
FROM <catalog>.<schema>.stream_silver_trades;

SELECT symbol, COUNT(*) AS events, ROUND(SUM(trade_value),2) AS gross_value
FROM <catalog>.<schema>.stream_silver_trades
GROUP BY symbol ORDER BY gross_value DESC LIMIT 10;

SELECT window.start AS window_start, window.end AS window_end,
       symbol, COUNT(*) AS trades, ROUND(SUM(trade_value),2) AS gross_value
FROM <catalog>.<schema>.stream_gold_symbol_metrics
GROUP BY window.start, window.end, symbol
ORDER BY window.start, gross_value DESC;

SELECT side, COUNT(*) AS events, ROUND(SUM(trade_value),2) AS value
FROM <catalog>.<schema>.stream_silver_trades
GROUP BY side;
