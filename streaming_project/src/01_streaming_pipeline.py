from pyspark.sql import functions as F
from pyspark.sql.types import *

SOURCE = "/Volumes/<catalog>/<schema>/<volume>/streaming_project/data/landing"
BRONZE = "<catalog>.<schema>.stream_bronze_trade_events"
SILVER = "<catalog>.<schema>.stream_silver_trades"
GOLD = "<catalog>.<schema>.stream_gold_symbol_metrics"
CHECKPOINT = "/Volumes/<catalog>/<schema>/<volume>/checkpoints/trades"

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("trade_id", StringType()),
    StructField("account_id", StringType()),
    StructField("symbol", StringType()),
    StructField("side", StringType()),
    StructField("quantity", DoubleType()),
    StructField("price", DoubleType()),
    StructField("currency", StringType()),
    StructField("event_type", StringType())
])

# Auto Loader = cloudFiles + Structured Streaming
raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format","json")
       .schema(schema)
       .load(SOURCE)
       .withColumn("_ingested_at", F.current_timestamp())
       .withColumn("_source_file", F.input_file_name()))

q1 = (raw.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", CHECKPOINT + "/bronze")
      .trigger(availableNow=True)
      .toTable(BRONZE))

# Silver stream: parse event time, watermark, deduplicate
bronze_stream = spark.readStream.table(BRONZE)

silver = (bronze_stream
    .withColumn("event_time", F.to_timestamp("event_ts"))
    .withColumn("trade_value", F.col("quantity") * F.col("price"))
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"]))

q2 = (silver.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", CHECKPOINT + "/silver")
      .trigger(availableNow=True)
      .toTable(SILVER))

# Gold aggregation for monitoring
gold = (spark.readStream.table(SILVER)
    .groupBy(F.window("event_time","5 minutes"), "symbol")
    .agg(
        F.count("*").alias("trade_count"),
        F.sum("trade_value").alias("gross_value"),
        F.sum(F.when(F.col("side")=="BUY", F.col("trade_value")).otherwise(0)).alias("buy_value"),
        F.sum(F.when(F.col("side")=="SELL", F.col("trade_value")).otherwise(0)).alias("sell_value")
    ))

q3 = (gold.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", CHECKPOINT + "/gold")
      .trigger(availableNow=True)
      .toTable(GOLD))
