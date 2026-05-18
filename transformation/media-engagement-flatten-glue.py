from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("media_engagement_flatten").getOrCreate()

media_eng_json = "s3://wistia-video-analytics-yoh/raw-wistia-yoh/media_engagement/"

df_media_json = spark.read.option("multiline", "true").json(media_eng_json)

df_flat_enga_data_list = df_media_json.withColumn(
    "engagement_value",
    explode(col("engagement_data"))
)

df_media_enga_flat = df_flat_enga_data_list.select(
    col("engagement"),
    col("engagement_value"),
    col("media_id")
)

df_media_enga_flat.write.mode("overwrite").parquet(
    "s3://wistia-video-analytics-yoh/refined-wistia-yoh/media_engagement/"
)