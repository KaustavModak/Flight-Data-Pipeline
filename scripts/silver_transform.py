import json
import pandas as pd
from pathlib import Path

def run_silver_transform(**kwargs):
    execution_date=kwargs["ds_nodash"] # to name files
    bronze_file=kwargs["ti"].xcom_pull(key="bronze_file",task_ids="bronze_ingest")

    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom")

    silver_path=Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    with open(bronze_file) as f:
        raw = json.load(f)
        # {
            # "time": 17102000,
            # "states": [
            # ["a808c1","UAL123","United States",...],
            # ["3c6444","DLH7CN","Germany",...]]
        # }

    df_raw=pd.DataFrame(raw["states"]) # converting to pandas dataframe --> has no col name
    
    df_raw.columns=["icao24", "callsign", "origin_country", "time_position","last_contact", "longitude", "latitude", "baro_altitude","on_ground", "velocity", "true_track", "vertical_rate","sensors", "geo_altitude", "squawk","spi", "position_source"] # renaming cols

    df=df_raw[["icao24","origin_country","velocity","on_ground","baro_altitude","vertical_rate"]] # selecting on useful cols

    output_file=silver_path / f"flights_silver_{execution_date}.csv" 
    # data/silver/flights_silver_20260307.csv
    df.to_csv(output_file,index=False)

    kwargs["ti"].xcom_push(key="silver_file",value=str(output_file))