from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("visitor_detail_flatten_all").getOrCreate()

visitor_json_file = "s3://wistia-video-analytics-yoh/raw-wistia-yoh/visitor_detail/"

df = spark.read.option("multiline", "true").json(visitor_json_file)

flat_df = df.select(
    col("visitor_key"),
    col("created_at"),
    col("last_active_at"),
    col("last_event_key"),
    col("load_count"),
    col("play_count"),

    col("visitor_identity.name").alias("visitor_name"),
    col("visitor_identity.email").alias("visitor_email"),
    col("visitor_identity.org.name").alias("org_name"),
    col("visitor_identity.org.title").alias("org_title"),

    col("user_agent_details.browser").alias("browser"),
    col("user_agent_details.browser_version").alias("browser_version"),
    col("user_agent_details.platform").alias("platform"),
    col("user_agent_details.mobile").alias("mobile")
)

flat_df.write.mode("overwrite").parquet(
    "s3://wistia-video-analytics-yoh/refined-wistia-yoh/visitor_detail_flattened/"
)