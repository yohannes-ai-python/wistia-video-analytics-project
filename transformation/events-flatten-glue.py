from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import boto3
import json

s3 = boto3.client("s3")

spark = SparkSession.builder.appName("event_flatten").getOrCreate()

event_source_path = "s3://wistia-video-analytics-yoh/bronze-wistia-yoh/events/"
event_target_path = "s3://wistia-video-analytics-yoh/silver-wistia-yoh/events/"

checkpoint_bucket = "wistia-video-analytics-yoh"
checkpoint_key = "checkpoints/last_event_silver_run.json"

# Read checkpoint
try:
    obj = s3.get_object(Bucket=checkpoint_bucket, Key=checkpoint_key)
    checkpoint = json.loads(obj["Body"].read().decode("utf-8"))
    last_silver_run_time = checkpoint["last_silver_run_time"]
except:
    last_silver_run_time = "1900-01-01T00:00:00"

# Read bronze Delta
df_event_bronze = spark.read.format("delta").load(event_source_path)

# Incremental filter: process records newer than last checkpoint
df_incremental_data = df_event_bronze.filter(
    col("ingestion_time") > lit(last_silver_run_time)
)

if df_incremental_data.first() is not None:

    df_events_flattened = df_incremental_data.select(
        col("event_key"),
        col("received_at"),
        col("visitor_key"),
        col("ip"),
        col("country"),
        col("region"),
        col("city"),
        col("lat"),
        col("lon"),
        col("org"),
        col("email"),
        col("media_id"),
        col("media_name"),
        col("percent_viewed"),
        col("conversion_type"),
        col("embed_url"),
        col("user_agent_details.browser").alias("browser"),
        col("user_agent_details.platform").alias("platform"),
        col("user_agent_details.mobile").alias("is_mobile"),
        col("ingestion_time")
    )

    df_flattened_dedup = df_events_flattened.dropDuplicates(["event_key"])

    if DeltaTable.isDeltaTable(spark, event_target_path):
        target_event = DeltaTable.forPath(spark, event_target_path)

        target_event.alias("te").merge(
            df_flattened_dedup.alias("fe"),
            "te.event_key = fe.event_key"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    else:
        df_flattened_dedup.write.format("delta").mode("overwrite").save(event_target_path)

    # Update checkpoint only after successful write
    max_ingestion_time = df_incremental_data.agg(
        {"ingestion_time": "max"}
    ).collect()[0][0]

    checkpoint_data = {
        "last_silver_run_time": max_ingestion_time.isoformat()
    }

    s3.put_object(
        Bucket=checkpoint_bucket,
        Key=checkpoint_key,
        Body=json.dumps(checkpoint_data)
    )