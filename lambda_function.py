import json
import requests
import boto3
from datetime import datetime

# Kinesis client
kinesis = boto3.client("kinesis")

def lambda_handler(event, context):
    """
    Main Lambda handler.
    1. Fetch API data
    2. Extract key attributes
    3. Infer congestion level
    4. Stream enriched records into Kinesis
    """

    API_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"

    try:
        # 1. Fetch API data
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        traffic_data = response.json()

        enriched_records = []

        # 2. Extract attributes & infer congestion
        for record in traffic_data:
            speed = float(record.get("speed", 0))
            borough = record.get("borough", "Unknown")
            timestamp = record.get("data_as_of", datetime.utcnow().isoformat())
            link_points = record.get("link_points", "")

            # 3. Infer congestion
            congestion_level = infer_congestion(speed)

            enriched = {
                "speed": speed,
                "borough": borough,
                "timestamp": timestamp,
                "link_points": link_points,
                "congestion_level": congestion_level
            }

            enriched_records.append(enriched)

            # 4. Stream to Kinesis
            send_to_kinesis(enriched)

        return {
            "statusCode": 200,
            "body": json.dumps({"records_sent": len(enriched_records)})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# ------------------------
# Helper Functions
# ------------------------

def infer_congestion(speed):
    """Determine congestion level based on speed."""
    if speed > 30:
        return "Low"
    elif 15 < speed <= 30:
        return "Moderate"
    elif 5 < speed <= 15:
        return "High"
    else:
        return "Severe"

def send_to_kinesis(record):
    """Send enriched record to Kinesis stream."""
    kinesis.put_record(
        StreamName="nyc_traffic_data",   # replace with your Kinesis stream name
        Data=json.dumps(record),
        PartitionKey=record["borough"]   # ensures load balancing across shards
    )
