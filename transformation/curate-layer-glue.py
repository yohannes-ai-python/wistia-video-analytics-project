from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower

spark = SparkSession.builder.appName("etl_to_curated").getOrCreate()

# Read media metadata
media_metadata = "s3://wistia-video-analytics-yoh/raw-wistia-yoh/media_metadata/"
df_media_metadata = spark.read.option("multiline", "true").json(media_metadata)

# Read refined media summary
media_summary = "s3://wistia-video-analytics-yoh/refined-wistia-yoh/media_summary/"
df_media_summary = spark.read.parquet(media_summary)

# Read refined events data
events_data = "s3://wistia-video-analytics-yoh/refined-wistia-yoh/events_flattened/"
df_events = spark.read.parquet(events_data)

# Create dim_media
dim_media = df_media_metadata.select(
    col("hashed_id").alias("media_id"),
    col("name").alias("title"),
    col("thumbnail.url").alias("url"),
    when(lower(col("name")).contains("facebook"), "Facebook")
    .when(lower(col("name")).contains("fb"), "Facebook")
    .when(lower(col("name")).contains("youtube"), "YouTube")
    .when(lower(col("name")).contains("yt"), "YouTube")
    .otherwise("Unknown")
    .alias("channel"),
    col("created").alias("created_at")
).dropDuplicates(["media_id"])

dim_media.write.mode("overwrite").parquet(
    "s3://wistia-video-analytics-yoh/curated-wistia-yoh/dim_media/"
)

# Create dim_visitor from refined events
dim_visitor = df_events.select(
    col("visitor_key").alias("visitor_id"),
    col("ip").alias("ip_address"),
    col("country"),
    col("region"),
    col("city"),
    col("browser"),
    col("platform"),
    col("is_mobile")
).dropDuplicates(["visitor_id"])

dim_visitor.write.mode("overwrite").parquet(
    "s3://wistia-video-analytics-yoh/curated-wistia-yoh/dim_visitor/"
)

# Create fact_media_engagement from refined media summary
fact_media_engagement = df_media_summary.select(
    col("media_id"),
    col("load_count"),
    col("play_count"),
    col("play_rate"),
    col("hours_watched").alias("total_hours_watched"),
    col("engagement").alias("average_engagement"),
    col("visitors").alias("unique_visitors")
).dropDuplicates(["media_id"])

fact_media_engagement.write.mode("overwrite").parquet(
    "s3://wistia-video-analytics-yoh/curated-wistia-yoh/fact_media_engagement/"
)
# create fact_visitor_engagement

# create fact_visitor_engagement

fact_visitor_engagement = df_events.select(
    col("event_key"),
    col("media_id"),
    col("visitor_key").alias("visitor_id"),
    col("received_at").alias("event_timestamp"),
    col("percent_viewed"),
    col("browser").alias("browser"),
    col("country"),
    col("embed_url")
).dropDuplicates(["event_key"])

fact_visitor_engagement.write.mode("overwrite").parquet(
    "s3://wistia-video-analytics-yoh/curated-wistia-yoh/fact_visitor_engagement/"
)