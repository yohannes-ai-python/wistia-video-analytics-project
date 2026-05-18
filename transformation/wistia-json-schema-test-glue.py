from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wistia_schema_test").getOrCreate()

folders = [
    {
        "name": "visitor_list",
        "input_path": "s3://wistia-video-analytics-yoh/raw-wistia-yoh/visitor_list/",
        "output_path": "s3://wistia-video-analytics-yoh/test-output/visitor_list/"
    },
    {
        "name": "media_engagement",
        "input_path": "s3://wistia-video-analytics-yoh/raw-wistia-yoh/media_engagement/",
        "output_path": "s3://wistia-video-analytics-yoh/test-output/media_engagement/"
    },
    {
        "name": "media_summary",
        "input_path": "s3://wistia-video-analytics-yoh/raw-wistia-yoh/media_summary/",
        "output_path": "s3://wistia-video-analytics-yoh/test-output/media_summary/"
    },
    {
        "name": "visitor_detail",
        "input_path": "s3://wistia-video-analytics-yoh/raw-wistia-yoh/visitor_detail/",
        "output_path": "s3://wistia-video-analytics-yoh/test-output/visitor_detail/"
    }
]

def wistia_json_schema_test(folder):
    print(f"========== Testing {folder['name']} ==========")

    df = spark.read.option("multiline", "true").json(folder["input_path"])

    df.printSchema()

    df.limit(5).write.mode("overwrite").json(folder["output_path"])

    print(f"Sample written to: {folder['output_path']}")


for folder in folders:
    wistia_json_schema_test(folder)