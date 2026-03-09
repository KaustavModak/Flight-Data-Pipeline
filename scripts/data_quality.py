import pandas as pd

def check_gold_data(**kwargs):
    ti=kwargs["ti"]
    gold_file=ti.xcom_pull(key="gold_file",task_ids="gold_aggregate")

    df = pd.read_csv(gold_file)

    # Check 1: dataset not empty
    if df.empty:
        raise ValueError("Gold dataset is empty")

    # Check 2: no null countries
    if df["origin_country"].isnull().any():
        raise ValueError("origin_country has NULL values")

    # Check 3: no negative flights
    if (df["total_flights"] < 0).any():
        raise ValueError("Negative flight counts detected")

    print("Data Quality Checks Passed")