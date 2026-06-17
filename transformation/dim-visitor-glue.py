from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, max as spark_max
import boto3
import json

spark = SparkSession.builder.appName("dim_visitor_creation").getOrCreate()

s3 = boto3.client("s3")

visitor_src_path = "s3://wistia-video-analytics-yoh/silver-wistia-yoh/visitor_list/"
visitor_trg_path = "s3://wistia-video-analytics-yoh/gold-wistia-yoh/dim_visitor/"

bucket_visitor_checkpoint = "wistia-video-analytics-yoh"
key_visitor_checkpoint = "checkpoints/dim_visitor_last_run.json"

# Read checkpoint
try:
    obj = s3.get_object(
        Bucket=bucket_visitor_checkpoint,
        Key=key_visitor_checkpoint
    )
    checkpoint = json.loads(obj["Body"].read().decode("utf-8"))
    visitor_last_run_time = checkpoint["dim_visitor_last_run_time"]
except:
    visitor_last_run_time = "1900-01-01T00:00:00"

# Read silver visitor Delta table
visitor_silver_src = spark.read.format("delta").load(visitor_src_path)

print("Columns:")
print(visitor_silver_src.columns)

visitor_silver_src.printSchema()

# Read only new records after last successful gold run
visitor_refined = visitor_silver_src.filter(
    col("ingestion_time") > lit(visitor_last_run_time)
)

if visitor_refined.first() is not None:

    df_dim_visitor = visitor_refined.select(
        col("visitor_key"),
        col("visitor_name"),
        col("visitor_email"),
        col("visitor_title"),
        col("organization_name"),
        col("created_at"),
        col("last_active_at"),
        col("browser"),
        col("browser_version"),
        col("platform"),
        col("is_mobile"),
        col("ingestion_time")
    ).dropDuplicates(["visitor_key"])

    if DeltaTable.isDeltaTable(spark, visitor_trg_path):

        visitor_trg = DeltaTable.forPath(spark, visitor_trg_path)

        (
            visitor_trg.alias("vt")
            .merge(
                df_dim_visitor.alias("vr"),
                "vt.visitor_key = vr.visitor_key"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        df_dim_visitor.write.format("delta").mode("overwrite").save(visitor_trg_path)

    # Update checkpoint after successful execution
    max_ingestion_time = visitor_refined.agg(
        spark_max("ingestion_time")
    ).collect()[0][0]

    checkpoint = {
        "dim_visitor_last_run_time": max_ingestion_time.isoformat()
    }

    s3.put_object(
        Bucket=bucket_visitor_checkpoint,
        Key=key_visitor_checkpoint,
        Body=json.dumps(checkpoint)
    )

else:
    print("No new visitor data to process.")