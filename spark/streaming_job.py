"""Structured Streaming trip aggregation with explicit schema and state controls."""
import os
from pyspark.sql import SparkSession, functions as F, types as T

spark=SparkSession.builder.appName("trip-streaming-platform").getOrCreate()
schema=T.StructType([
 T.StructField("schema_version",T.IntegerType(),False),T.StructField("event_id",T.StringType(),False),
 T.StructField("trip_id",T.StringType(),False),T.StructField("event_time",T.TimestampType(),False),
 T.StructField("city",T.StringType(),False),T.StructField("fare_usd",T.DoubleType(),False),
 T.StructField("distance_km",T.DoubleType(),False)])
raw=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")).option("subscribe",os.getenv("TRIP_TOPIC","trip-events")).option("startingOffsets","earliest").load())
parsed=raw.select(F.from_json(F.col("value").cast("string"),schema).alias("e")).select("e.*")
valid=parsed.filter((F.col("schema_version")==1)&F.col("event_id").isNotNull()&F.col("trip_id").isNotNull()&(F.col("fare_usd")>=0)&(F.col("distance_km")>=0))
metrics=(valid.withWatermark("event_time","10 minutes").dropDuplicates(["event_id","event_time"]).groupBy(F.window("event_time","5 minutes"),"city").agg(F.count("*").alias("trip_count"),F.round(F.sum("fare_usd"),2).alias("gross_booking_value_usd"),F.round(F.avg("distance_km"),2).alias("avg_distance_km")))
query=(metrics.writeStream.outputMode("update").format("console").option("truncate","false").option("checkpointLocation",os.getenv("CHECKPOINT_PATH","/tmp/checkpoints/trip-metrics")).start())
query.awaitTermination()
