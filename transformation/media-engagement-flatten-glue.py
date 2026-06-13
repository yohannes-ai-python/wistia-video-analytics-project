from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, current_timestamp, lit, concat_ws
from delta.tables import DeltaTable
import boto3
import json

spark = SparkSession.builder.appName("media_metadata_flatten").getOrCreate()

s3 = boto3.client("s3")

media_bronze_source = "s3://wistia-video-analytics-yoh/bronze-wistia-yoh/media_metadata/"
media_silver_target = "s3://wistia-video-analytics-yoh/silver-wistia-yoh/media_metadata_flattened/"

checkpoint_bucket = "wistia-video-analytics-yoh"
checkpoint_key = "checkpoints/last_media_silver_run_time.json"

# Read checkpoint
try:
    obj = s3.get_object(Bucket=checkpoint_bucket, Key=checkpoint_key)
    checkpoint = json.loads(obj["Body"].read().decode("utf-8"))
    last_silver_run_time = checkpoint["last_silver_run_time"]
except:
    last_silver_run_time = "1900-01-01T00:00:00"


# Read bronze Delta table
df_media = spark.read.format("delta").load(media_bronze_source)

# Only process new bronze records
df_media_incremental = df_media.filter(
    col("ingestion_time") > lit(last_silver_run_time)
)

if df_media_incremental.first() is not None:

    df_media_flattened = df_media_incremental.select(
        col("id").alias("wistia_media_id"),
        col("hashed_id").alias("media_id"),
        col("name").alias("media_name"),
        col("duration"),
        col("created").alias("created_at"),
        col("updated").alias("updated_at"),
        col("status"),

        col("folder.id").alias("folder_id"),
        col("folder.hashed_id").alias("folder_hashed_id"),
        col("folder.name").alias("folder_name"),

        col("subfolder.hashed_id").alias("subfolder_hashed_id"),
        col("subfolder.name").alias("subfolder_name"),

        col("thumbnail.url").alias("thumbnail_url"),
        col("thumbnail.width").alias("thumbnail_width"),
        col("thumbnail.height").alias("thumbnail_height"),

        explode_outer(col("assets")).alias("asset"),

        col("ingestion_time")
    ).select(
        col("wistia_media_id"),
        col("media_id"),
        col("media_name"),
        col("duration"),
        col("created_at"),
        col("updated_at"),
        col("status"),
        col("folder_id"),
        col("folder_hashed_id"),
        col("folder_name"),
        col("subfolder_hashed_id"),
        col("subfolder_name"),
        col("thumbnail_url"),
        col("thumbnail_width"),
        col("thumbnail_height"),

        col("asset.width").alias("asset_width"),
        col("asset.height").alias("asset_height"),
        col("asset.type").alias("asset_type"),
        col("asset.file_size").alias("asset_file_size"),
        col("asset.content_type").alias("asset_content_type"),
        col("asset.url").alias("asset_url"),

        col("ingestion_time")
    )

    # Create unique key because one media_id can have many assets
    df_media_flattened = df_media_flattened.withColumn(
        "media_asset_key",
        concat_ws("_", col("media_id"), col("asset_type"), col("asset_width"), col("asset_height"))
    )

    df_media_dedup = df_media_flattened.dropDuplicates(["media_asset_key"])

    if DeltaTable.isDeltaTable(spark, media_silver_target):

        media_target = DeltaTable.forPath(spark, media_silver_target)

        media_target.alias("mt").merge(
            df_media_dedup.alias("ms"),
            "mt.media_asset_key = ms.media_asset_key"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    else:
        df_media_dedup.write.format("delta").mode("overwrite").save(media_silver_target)

    # Update checkpoint
    checkpoint_data = {
        "last_silver_run_time": df_media_incremental.agg(
            {"ingestion_time": "max"}
        ).collect()[0][0].isoformat()
    }

    s3.put_object(
        Bucket=checkpoint_bucket,
        Key=checkpoint_key,
        Body=json.dumps(checkpoint_data)
    )