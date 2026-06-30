"""Smoke test: read the sales_events stream from Redpanda and print it to the console.

This proves Spark can connect to the broker and parse our JSON events. It writes
nowhere yet -- that comes in the next steps.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

# IMPORTANT: Spark runs INSIDE Docker, so it uses Redpanda's INTERNAL address.
# (Your producer on the laptop uses localhost:19092 -- same broker, different door.)
KAFKA_BOOTSTRAP = "redpanda-0:9092"
TOPIC = "sales_events"

# Schema of each JSON event -- must match scripts/produce_events.py
schema = StructType([
    StructField("event_id",     StringType()),
    StructField("event_type",   StringType()),
    StructField("timestamp",    StringType()),
    StructField("order_id",     StringType()),
    StructField("customer_id",  StringType()),
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("quantity",     IntegerType()),
    StructField("unit_price",   DoubleType()),
    StructField("total_amount", DoubleType()),
])

spark = SparkSession.builder.appName("stream-to-console").getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # quiet down Spark's noisy INFO logs

# Read the stream. Kafka hands us key/value as raw bytes.
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")   # replay everything already in the topic
    .load()
)

# Turn the JSON bytes in `value` into proper columns using our schema.
events = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("e"))
    .select("e.*")
)

# Print each micro-batch as a table.
query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()