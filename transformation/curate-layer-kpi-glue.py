from pyspark.sql import SparkSession
from pyspark.sql.functions import col,countDistinct

spark = SparkSession.builder.appName("kpi_creation").getOrCreate()

#read Fact_visitor_engagement
fact_visitor_engagement=spark.read.option("multiline","true").parquet("s3://wistia-video-analytics-yoh/curated-wistia-yoh/fact_visitor_engagement/")

#read fact_media_engagement
fact_media_engagement=spark.read.option("multiline","true").parquet("s3://wistia-video-analytics-yoh/curated-wistia-yoh/fact_media_engagement/")

#read dim_media 

dim_media=spark.read.option("multiline","true").parquet("s3://wistia-video-analytics-yoh/curated-wistia-yoh/dim_media/")

visitors_in_each_browser=fact_visitor_engagement.groupBy(
    "media_id",
    "channel"
    ).agg(countDistinct("visitor_id").alias("number_of_visitors")).orderBy(col("number_of_visitors").desc())

visitors_in_each_browser.write.mode("overwrite").parquet("s3://wistia-video-analytics-yoh/curated-wistia-yoh/visitors_in_each_browser/")

# Top_videos kpi_creation
Top_videos=fact_media_engagement.join(dim_media, "media_id").select(
    col("media_id"),
    col("title"),
    col("channel"),
    col("total_hours_watched"),
    col("play_count")
    ).orderBy(col("play_count").desc())
    
Top_videos.write.mode("overwrite").parquet("s3://wistia-video-analytics-yoh/curated-wistia-yoh/top_videos/")
