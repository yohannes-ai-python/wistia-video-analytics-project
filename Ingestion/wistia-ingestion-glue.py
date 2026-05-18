import requests
import boto3
from datetime import datetime
import sys
import json
from awsglue.utils import getResolvedOptions
import time

s3 = boto3.client("s3")

args = getResolvedOptions(sys.argv, ["API_TOKEN", "BUCKET"])

BUCKET = args["BUCKET"]
API_TOKEN = args["API_TOKEN"]

KEY = "raw-wistia-yoh/"
CHECKPOINT_KEY = "checkpoints/last_run_timestamp.json"

media_ids = ["v08dlrgr7v", "gskhw4w4lm"]

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "X-Wistia-API-Version": "2026-03"
}


def api_call(url):
    response = requests.get(url, headers=headers, timeout=300)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"API failed. Status: {response.status_code}, Response: {response.text}"
        )


def save_wistia_data_s3(data, s3_key):
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )

    print(f"File saved to s3://{BUCKET}/{s3_key}")


def get_last_run_timestamp():
    try:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=CHECKPOINT_KEY
        )

        data = json.loads(obj["Body"].read().decode("utf-8"))

        return data["last_run_timestamp"]

    except Exception:
        return "2000-01-01T00:00:00Z"


def save_checkpoint_timestamp(timestamp):
    s3.put_object(
        Bucket=BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps({"last_run_timestamp": timestamp}, indent=2),
        ContentType="application/json"
    )

    print(f"Checkpoint saved: {timestamp}")


# Get checkpoint 
last_run = get_last_run_timestamp()
current_run_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
file_timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

print(f"Last run timestamp: {last_run}")
print(f"Current run timestamp: {current_run_time}")


# 1 and 2: MEDIA SUMMARY + MEDIA ENGAGEMENT
for media_id in media_ids:

    # 1. Media-level summary
    media_url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"

    media_data = api_call(media_url)

    media_s3_key = (
        f"{KEY}media_summary/"
        f"media_id={media_id}/"
        f"wistia_media_summary_{file_timestamp}.json"
    )

    save_wistia_data_s3(media_data, media_s3_key)

    # 2. Media engagement detail
    engagement_url = f"https://api.wistia.com/modern/stats/medias/{media_id}/engagement"

    engagement_data = api_call(engagement_url)

    engagement_s3_key = (
        f"{KEY}media_engagement/"
        f"media_id={media_id}/"
        f"wistia_media_engagement_{file_timestamp}.json"
    )

    save_wistia_data_s3(engagement_data, engagement_s3_key)
    
    #media metadata ingestion
    
    media_metadata_url =f"https://api.wistia.com/modern/medias/{media_id}"
    
    mediametadata_data=api_call(media_metadata_url)
    
    metadata_s3_key = (
        f"{KEY}media_metadata/"
        f"media_id={media_id}/"
        f"wistia_media_metadata_{file_timestamp}.json"
    )
    
    save_wistia_data_s3(mediametadata_data,metadata_s3_key)  
    


# 3. LIST VISITORS + VISITOR DETAIL WITH INCREMENTAL LOGIC
page = 1
max_pages = 50

while page <= max_pages:

    visitors_url = (
        f"https://api.wistia.com/modern/stats/visitors"
        f"?page={page}&per_page=25"
    )

    visitors_data = api_call(visitors_url)

    visitors_s3_key = (
        f"{KEY}visitor_list/"
        f"page={page}/"
        f"wistia_visitor_list_{file_timestamp}.json"
    )

    save_wistia_data_s3(visitors_data, visitors_s3_key)

    if not visitors_data:
        break

    for visitor in visitors_data:
        time.sleep(0.02)

        visitor_key = (
            visitor.get("visitor_key")
            or visitor.get("visitorKey")
            or visitor.get("key")
            or visitor.get("id")
        )

        record_time = (
            visitor.get("last_active_at")
            or visitor.get("updated_at")
            or visitor.get("created_at")
        )

        if not record_time:
            print(f"Skipping visitor {visitor_key}: no timestamp found")
            continue

        if record_time > last_run:
            print(f"New or updated visitor found: {visitor_key}")

            if visitor_key:
                visitor_url = f"https://api.wistia.com/modern/stats/visitors/{visitor_key}"

                visitor_detail = api_call(visitor_url)

                visitor_detail_s3_key = (
                    f"{KEY}visitor_detail/"
                    f"visitor_key={visitor_key}/"
                    f"wistia_visitor_detail_{file_timestamp}.json"
                )

                save_wistia_data_s3(visitor_detail, visitor_detail_s3_key)

        else:
            print(f"Skipping old visitor record: {visitor_key}")

    page += 1
    time.sleep(0.02)
# events
# EVENTS INGESTION

page = 1
per_page = 50

while True:

    events_url = (
        "https://api.wistia.com/modern/stats/events"
        f"?page={page}&per_page={per_page}"
    )

    print(f"Pulling events page {page}")

    events_data = api_call(events_url)

    if not events_data:
        print("No more events found")
        break

    events_s3_key = (
        f"{KEY}events/"
        f"page={page}/"
        f"wistia_events_{file_timestamp}.json"
    )

    save_wistia_data_s3(events_data, events_s3_key)

    print(f"Saved page {page}")

    if len(events_data) < per_page:
        print("Last page reached")
        break

    page += 1

    time.sleep(1)
    
# Save checkpoint 
save_checkpoint_timestamp(current_run_time)
    

 




