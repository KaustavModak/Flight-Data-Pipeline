import requests
import json
from datetime import datetime, timezone
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"
# returns live aircraft state data worldwide
# Each row = one aircraft

def run_bronze_ingestion(**kwargs):
    response = requests.get(URL, timeout=30)
    response.raise_for_status() # HTTP status code

    data = response.json()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") # YYYYMMDDHHMMSS

    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")
    # flights_20260307224530.json
    # flights_20260307231500.json
    # flights_20260307234500.json

    with open(path, "w") as f:
        json.dump(data, f)
    
    kwargs["ti"].xcom_push(key="bronze_file", value=str(path))