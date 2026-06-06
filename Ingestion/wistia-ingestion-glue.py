import requests
import boto3
from datetime import datetime,timezone
import sys
import json
from awsglue.utils import getResolvedOptions
import time

s3 = boto3.client("s3")

args = getResolvedOptions(sys.argv, ["API_TOKEN", "BUCKET"])

BUCKET = args["BUCKET"]
API_TOKEN = args["API_TOKEN"]
def parse_ts(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    
KEY = "raw-wistia-yoh/"

CHECKPOINT_KEYS = {
    "media_metadata": "checkpoints/media_metadata_last_run.json",
    "visitor_list": "checkpoints/visitor_list_last_run.json",
    "events": "checkpoints/events_last_run.json"
}


media_ids = ["v08dlrgr7v", "gskhw4w4lm"]

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "X-Wistia-API-Version": "2026-03"
}


def api_call(url, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=120)

            if response.status_code == 200:
                return response.json()

            if response.status_code in [429, 500, 502, 503, 504]:
                print(f"Retry {attempt}: API returned {response.status_code}")
                time.sleep(attempt * 10)
                continue

            raise Exception(
                f"API failed. Status: {response.status_code}, Response: {response.text}"
            )

        except requests.exceptions.RequestException as e:
            print(f"Retry {attempt}: request error {str(e)}")
            time.sleep(attempt * 10)

    raise Exception(f"API failed after {max_retries} retries: {url}")


def save_wistia_data_s3(data, s3_key):
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )

    print(f"File saved to s3://{BUCKET}/{s3_key}")


def get_checkpoint(endpoint_name):
    checkpoint_key = CHECKPOINT_KEYS[endpoint_name]

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=checkpoint_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data["last_run_timestamp"]

    except Exception:
        return "2000-01-01T00:00:00Z"


def save_checkpoint(endpoint_name, timestamp):
    checkpoint_key = CHECKPOINT_KEYS[endpoint_name]

    s3.put_object(
        Bucket=BUCKET,
        Key=checkpoint_key,
        Body=json.dumps({"last_run_timestamp": timestamp}, indent=2),
        ContentType="application/json"
    )

    print(f"{endpoint_name} checkpoint saved: {timestamp}")

# Get checkpoint 

media_last_run = get_checkpoint("media_metadata")
visitor_last_run = get_checkpoint("visitor_list")
events_last_run = get_checkpoint("events")

current_run_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
file_timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")

media_success=False
visitors_success=False
events_success=False


print(f"Current run timestamp: {current_run_time}")


# media metadata ingestion
max_media_timestamp=media_last_run
for media_id in media_ids:

    media_metadata_url =f"https://api.wistia.com/modern/medias/{media_id}"
    
    mediametadata_data=api_call(media_metadata_url)
    
# for media incremental logic using Updated filed in media_metadata

    updated_at=mediametadata_data.get("updated")
    
   
    if updated_at and parse_ts(updated_at) > parse_ts(media_last_run):
    
       metadata_s3_key = (
        f"{KEY}media_metadata/"
        f"media_id={media_id}/"
        f"wistia_media_metadata_{file_timestamp}.json"
      )


       save_wistia_data_s3(mediametadata_data,metadata_s3_key) 
       media_success=True
       if parse_ts(updated_at)>parse_ts(max_media_timestamp):
           max_media_timestamp=updated_at
       
       
       
    else:
     print(f"Skipping old media record: {media_id}")
  

# 3. LIST VISITORS + VISITOR DETAIL WITH INCREMENTAL LOGIC
page = 1
max_pages = 10
max_visitors_timestamp=visitor_last_run

while page <= max_pages:

    visitors_url = (
        f"https://api.wistia.com/modern/stats/visitors"
        f"?page={page}&per_page=25"
    )

    visitors_data = api_call(visitors_url)

   
#    save_wistia_data_s3(visitors_data, visitors_s3_key)

    if not visitors_data:
        break

    for visitor in visitors_data:
        time.sleep(0.02)

        visitor_key = visitor.get("visitor_key")
         
       
        record_time = visitor.get("last_active_at")
         
        
        if not record_time:
            print(f"Skipping visitor {visitor_key}: no timestamp found")
            continue

        if record_time and parse_ts(record_time) > parse_ts(visitor_last_run):
            print(f"New or updated visitor found: {visitor_key}")
            if parse_ts(record_time)>parse_ts(max_visitors_timestamp):
                    max_visitors_timestamp=record_time

            if visitor_key:
                visitor_url = f"https://api.wistia.com/modern/stats/visitors/{visitor_key}"

                visitor_detail = api_call(visitor_url)

                visitor_detail_s3_key = (
                    f"{KEY}visitor_detail/"
                    f"visitor_key={visitor_key}/"
                    f"wistia_visitor_detail_{file_timestamp}.json"
                )

                save_wistia_data_s3(visitor_detail, visitor_detail_s3_key)
                visitors_success=True
             

        else:
            print(f"Skipping old visitor record: {visitor_key}")

    page += 1


# EVENTS INGESTION

page = 1
per_page = 50
max_event_timestamp=events_last_run

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
    new_events=[]
    for event in events_data:
      received_at=event.get("received_at")
      if received_at and parse_ts(received_at) >parse_ts(events_last_run):
        new_events.append(event)
        if parse_ts(received_at)>parse_ts(max_event_timestamp):
              max_event_timestamp=received_at
    if new_events:
          events_s3_key = (
            f"{KEY}events/"
            f"page={page}/"
            f"wistia_events_{file_timestamp}.json"
          )
        
          save_wistia_data_s3(new_events, events_s3_key)
          events_success=True
       

    else:
        print(f"No new events on page {page}")
        
    if len(events_data) < per_page:
         print("Last page reached")
         break

    page += 1

    time.sleep(1)
    
#  save each checkpoints
if media_success:
    save_checkpoint("media_metadata", max_media_timestamp)
if visitors_success:
   save_checkpoint("visitor_list", max_visitors_timestamp)
if events_success:
    save_checkpoint("events", max_event_timestamp)   

    
