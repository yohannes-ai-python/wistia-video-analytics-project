from pyspark.sql.functions import col
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("level_flat").getOrCreate()

df_events=spark.read.option("multiline","true").json("s3://wistia-video-analytics-yoh/raw-wistia-yoh/events/")

df_events_flattened = df_events.select(

    col("event_key"),

    col("received_at"),

    col("visitor_key"),

    col("ip"),

    col("country"),

    col("region"),

    col("city"),

    col("media_id"),

    col("media_name"),

    col("percent_viewed"),

    col("embed_url"),

    col("user_agent_details.browser").alias("browser"),

    col("user_agent_details.platform").alias("platform"),

    col("user_agent_details.mobile").alias("is_mobile")

)

df_events_flattened.write.mode("overwrite").parquet("s3://wistia-video-analytics-yoh/refined-wistia-yoh/events_flattened/")
