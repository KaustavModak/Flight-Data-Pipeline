import pandas as pd
from pathlib import Path

def run_gold_aggregate(**kwargs):
    silver_file=kwargs["ti"].xcom_pull(key="silver_file",task_ids="silver_transform")

    df=pd.read_csv(silver_file)

    df["climbing"]=df["vertical_rate"] > 0
    df["descending"]=df["vertical_rate"] < 0

    agg=(df.groupby("origin_country").agg(
        total_flights=("icao24","count"),
        avg_velocity=("velocity","mean"),
        max_velocity=("velocity", "max"),
        min_velocity=("velocity", "min"),
        avg_altitude=("baro_altitude","mean"), # shows typical flight levels by region
        max_altitude=("baro_altitude","max"),
        min_altitude=("baro_altitude","min"),
        grounded=("on_ground","sum"),
        # flight phase distribution
        climbing=("climbing","sum"),
        descending=("descending","sum")
    )
    .reset_index()) # Without this, origin_country would remain the index

    # rounding for better readability
    agg["avg_velocity"] = agg["avg_velocity"].round(3)
    agg["avg_altitude"] = agg["avg_altitude"].round(3)

    agg["airborne"]=agg["total_flights"]-agg["grounded"]
    agg["ground_ratio"] = (agg["grounded"] / agg["total_flights"]).round(3) # airport congestion levels

    output_file=Path(silver_file.replace("silver","gold"))
    # data/gold/flights_silver_20260307.csv
    agg.to_csv(output_file,index=False)
    kwargs["ti"].xcom_push(key="gold_file",value=str(output_file))