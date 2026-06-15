from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, lit, max as spark_max
from delta.tables import DeltaTable
import boto3
import json

spark = SparkSession.builder.appName("dim_media_gold").getOrCreate()

s3 = boto3.client("s3")

checkpoint_bucket = "wistia-video-analytics-yoh"
checkpoint_key = "checkpoints/media_last_curated_run_time.json"

media_metadata_src = "s3://wistia-video-analytics-yoh/silver-wistia-yoh/media_metadata_flattened/"
media_gold_target = "s3://wistia-video-analytics-yoh/curated-wistia-yoh/dim_media/"

# Read checkpoint
try:
    obj = s3.get_object(Bucket=checkpoint_bucket, Key=checkpoint_key)
    checkpoint = json.loads(obj["Body"].read().decode("utf-8"))
    media_last_run_time = checkpoint["media_last_curated_run_time"]
except:
    media_last_run_time = "1900-01-01T00:00:00"

# Read silver media Delta table
df_media_metadata = spark.read.format("delta").load(media_metadata_src)

# Read only new silver records
df_incremental = df_media_metadata.filter(
    col("ingestion_time") > lit(media_last_run_time)
)

if df_incremental.first() is not None:

    # Create dim_media from silver flattened data
    # Since silver has multiple rows per media because of assets,
    # I drop duplicates by media_id.
    df_dim_media = df_incremental.select(
        col("media_id"),
        col("media_name").alias("title"),
        col("thumbnail_url"),
        col("duration"),
        col("status"),
        col("folder_name"),
        col("subfolder_name"),

        when(lower(col("media_name")).contains("facebook"), "Facebook")
        .when(lower(col("media_name")).contains("fb"), "Facebook")
        .when(lower(col("media_name")).contains("youtube"), "YouTube")
        .when(lower(col("media_name")).contains("yt"), "YouTube")
        .otherwise("Unknown")
        .alias("channel"),

        col("created_at"),
        col("updated_at"),
        col("ingestion_time")
    ).dropDuplicates(["media_id"])

    # Merge into gold Delta table
    if DeltaTable.isDeltaTable(spark, media_gold_target):

        target = DeltaTable.forPath(spark, media_gold_target)

        target.alias("t").merge(
            df_dim_media.alias("s"),
            "t.media_id = s.media_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    else:
        df_dim_media.write.format("delta").mode("overwrite").save(media_gold_target)

    # Update checkpoint after successful merge/write
    max_ingestion_time = df_incremental.agg(
        spark_max("ingestion_time")
    ).collect()[0][0]

    checkpoint_data = {
        "media_last_curated_run_time": max_ingestion_time.isoformat()
    }

    s3.put_object(
        Bucket=checkpoint_bucket,
        Key=checkpoint_key,
        Body=json.dumps(checkpoint_data)
    )

else:
    print("No new media records to process.")


