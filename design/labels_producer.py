import os
import glob
import csv
import json
from google.cloud import pubsub_v1

def set_credentials():
    key_files = glob.glob("*.json")
    if not key_files:
        raise FileNotFoundError("No JSON key found")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(key_files[0])

def main():
    set_credentials()

    PROJECT_ID = "crested-ratio-477516-e0"
    TOPIC_ID = "labelsTopic"
    CSV_FILE = "Labels.csv"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing CSV records to {topic_path}\n")

    with open(CSV_FILE, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        count = 0
        for row in reader:
            data = json.dumps(row).encode("utf-8")
            message_id = publisher.publish(topic_path, data=data).result()
            count += 1
            print(f"Published record {count}: message_id={message_id}")

    print(f"\nDone. Published {count} records.")

if __name__ == "__main__":
    main()
