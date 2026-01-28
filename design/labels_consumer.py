import os
import glob
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
    SUBSCRIPTION_ID = "labelsTopic-sub"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Listening on {subscription_path}\n")

    def callback(message):
        record = json.loads(message.data.decode("utf-8"))
        print("Received record:")
        for k, v in record.items():
            print(f"  {k}: {v}")
        print("-" * 30)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)
    print("Consumer running... Press CTRL+C to stop.\n")

    while True:
        pass

if __name__ == "__main__":
    main()
