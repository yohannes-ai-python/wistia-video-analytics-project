from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("visitor_list_flat").getOrCreate()

visitor_list="s3://wistia-video-analytics-yoh/raw-wistia-yoh/visitor_list/"

df_visitor=spark.read.option("multiline","true").json(visitor_list)

df_flat_visitor_list = df_visitor.select(
    col("created_at"),
    col("last_active_at"),
    col("play_count"),

    col("user_agent_details.browser").alias("browser"),
    col("user_agent_details.browser_version").alias("browser_version"),
    col("user_agent_details.platform").alias("platform"),
    col("user_agent_details.mobile").alias("mobile"),

    col("visitor_identity.name").alias("visitor_name"),
    col("visitor_identity.org").alias("organization"),
    col("page").alias("page_number")
)

df_flat_visitor_list.write.mode("overwrite").parquet("s3://wistia-video-analytics-yoh/refined-wistia-yoh/visitor_list/")
