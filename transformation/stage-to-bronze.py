from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col,current_timestamp,input_file_name,row_number
from datetime import time 
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("to_bronze_transform").getOrCreate()

#source files
visitor_stage_path="s3://wistia-video-analytics-yoh/raw-wistia-yoh/visitor_detail/"
media_stage_path="s3://wistia-video-analytics-yoh/raw-wistia-yoh/media_metadata/"
event_stage_path="s3://wistia-video-analytics-yoh/raw-wistia-yoh/events/"

#target path 

visitor_bronze_path="s3://wistia-video-analytics-yoh/bronze-wistia-yoh/visitor_detail/"
media_bronze_path="s3://wistia-video-analytics-yoh/bronze-wistia-yoh/media_metadata/"
event_bronze_path="s3://wistia-video-analytics-yoh/bronze-wistia-yoh/events/"


#read source_file

df_visitor=spark.read.option("multiline",True).json(visitor_stage_path)
df_media=spark.read.option("multiline",True).json(media_stage_path)
df_event=spark.read.option("multiline",True).json(event_stage_path)


#target files

#visitor file

df_bronze_visitor=(df_visitor
    .withColumn("ingestion_time",current_timestamp())
    .withColumn("source_file",input_file_name())
     )
w_visitor=Window.partitionBy("visitor_key").orderBy(col("last_active_at").desc())

df_bronze_visitor_dedup=df_bronze_visitor.withColumn(
            "rnv", row_number().over(w_visitor)).filter(
                col("rnv") == 1
                ).drop("rnv")
            
if DeltaTable.isDeltaTable(spark,visitor_bronze_path):
    target_visitor=DeltaTable.forPath(spark,visitor_bronze_path)
    
    target_visitor.alias("tv").merge(
        df_bronze_visitor_dedup.alias("sv"),
        "tv.visitor_key=sv.visitor_key"
        ).whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_bronze_visitor_dedup.write.format("delta").mode("overwrite").save(visitor_bronze_path)


# media data 
 
df_bronze_media=(df_media
        .withColumn("ingestion_time",current_timestamp())
        .withColumn("source_file",input_file_name())
        )

w_media=Window.partitionBy("hashed_id").orderBy(col("updated").desc())

df_bronze_media_dedup=df_bronze_media.withColumn(
            "rnm", row_number().over(w_media)
            ).filter(
                col("rnm") == 1 
                ).drop("rnm")


if DeltaTable.isDeltaTable(spark,media_bronze_path):
    
    target_media=DeltaTable.forPath(spark,media_bronze_path)
    
    target_media.alias("tm").merge(
        df_bronze_media_dedup.alias("sm"),
        "tm.hashed_id=sm.hashed_id"
        ).whenMatchedUpdateAll()\
         .whenNotMatchedInsertAll()\
         .execute()
else:
    df_bronze_media_dedup.write.format("delta").mode("overwrite").save(media_bronze_path)
    
#event data transformation to bronze-wistia-yoh
df_bronze_event=(df_event
                .withColumn("ingestion_time",current_timestamp())
                .withColumn("source_file",input_file_name())
                )
w_event=Window.partitionBy("event_key").orderBy(col("received_at").desc())

df_bronze_event_dedup=df_bronze_event.withColumn(
            "cre", row_number().over(w_event)
            ).filter(
                col("cre") == 1
                ).drop("cre")
    
if DeltaTable.isDeltaTable(spark,event_bronze_path):
    
    target_event=DeltaTable.forPath(spark,event_bronze_path)
    
    target_event.alias("te").merge(
        df_bronze_event_dedup.alias("es"),
        "te.event_key=es.event_key"
        ).whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_bronze_event_dedup.write.format("delta").mode("overwrite").save(event_bronze_path)
        
    
    