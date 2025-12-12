import requests
import sys
import time

API_KEY = "VnRacUFwc0I2VGNnZm9JeDVYWFQ6NUdrTldvTXctODlQREZjekp5SE1CQQ=="
ES_HOST = "https://8937661896594ff0ae1361359628abd0.eu-west-1.aws.found.io:443"

headers = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json"
}

task_id = sys.argv[1] if len(sys.argv) > 1 else "b7tjUiM9SiGLjyrlgpUfKw:73203178"

print(f"Monitoring task: {task_id}")
while True:
    response = requests.get(f"{ES_HOST}/_tasks/{task_id}", headers=headers)
    if response.status_code == 200:
        task_info = response.json()
        completed = task_info.get("completed", False)
        status = task_info.get("task", {}).get("status", {})
        total = status.get("total", 0)
        updated = status.get("updated", 0)

        print(f"Progress: {updated}/{total} documents updated", end="\r")

        if completed:
            print(f"\nCompleted! Updated: {updated}, Total: {total}")
            break
    else:
        print(f"Error: {response.status_code}")
        break
    time.sleep(2)
