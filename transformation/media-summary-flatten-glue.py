from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

spark=SparkSession.builder.appName("medi_summary_flat").getOrCreate()

media_summary_json ="s3://wistia-video-analytics-yoh/raw-wistia-yoh/media_summary/"

df_media_summary=spark.read.option("multiline","true").json(media_summary_json)

#the json file here is simpl json 

df_media_summary_flat=df_media_summary.select("*")

df_media_summary_flat.write.mode("overwrite").parquet("s3://wistia-video-analytics-yoh/refined-wistia-yoh/media_summary/")
