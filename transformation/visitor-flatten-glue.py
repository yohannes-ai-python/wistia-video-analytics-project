from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp, lit
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import boto3
import json

spark = SparkSession.builder.appName("bronze_to_silver_visitor").getOrCreate()

bronze_path = "s3://wistia-video-analytics-yoh/bronze-wistia-yoh/visitor_detail/"
silver_path = "s3://wistia-video-analytics-yoh/silver-wistia-yoh/visitor_list/"

checkpoint_bucket = "wistia-video-analytics-yoh"
checkpoint_key = "checkpoints/visitor_silver_checkpoint.json"

s3 = boto3.client("s3")

# Read last successful silver load timestamp
try:
    obj = s3.get_object(Bucket=checkpoint_bucket, Key=checkpoint_key)
    checkpoint = json.loads(obj["Body"].read().decode("utf-8"))
    last_silver_run_time = checkpoint["last_silver_run_time"]
except:
    last_silver_run_time = "1900-01-01 00:00:00"

# Read bronze
df_bronze = spark.read.format("delta").load(bronze_path)

# Read only new/updated bronze records
df_incremental = df_bronze.filter(
    col("ingestion_time") > lit(last_silver_run_time)
)

# Flatten only incremental records
df_flat = df_incremental.select(
    col("visitor_key"),
    col("created_at"),
    col("last_active_at"),
    col("last_event_key"),
    col("load_count"),
    col("play_count"),
    col("visitor_identity.name").alias("visitor_name"),
    col("visitor_identity.email").alias("visitor_email"),
    col("visitor_identity.org.title").alias("visitor_title"),
    col("visitor_identity.org.name").alias("organization_name"),
    col("user_agent_details.browser").alias("browser"),
    col("user_agent_details.browser_version").alias("browser_version"),
    col("user_agent_details.platform").alias("platform"),
    col("user_agent_details.mobile").alias("is_mobile"),
    col("ingestion_time")
)

# Deduplicate incremental records
w = Window.partitionBy("visitor_key").orderBy(col("last_active_at").desc())

df_dedup = (
    df_flat
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Merge into silver
if DeltaTable.isDeltaTable(spark, silver_path):

    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("target")
        .merge(
            df_dedup.alias("source"),
            "target.visitor_key = source.visitor_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    (
        df_dedup
        .write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

# Update checkpoint only after successful write/merge
new_checkpoint_time = df_bronze.agg({"ingestion_time": "max"}).collect()[0][0]

checkpoint_data = {
    "last_silver_run_time": str(new_checkpoint_time)
}

s3.put_object(
    Bucket=checkpoint_bucket,
    Key=checkpoint_key,
    Body=json.dumps(checkpoint_data)
)