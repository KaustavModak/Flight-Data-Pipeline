import pandas as pd
import psycopg2 # Python driver used to connect to PostgreSQL
from airflow.hooks.base import BaseHook # retrieves database credentials stored in Airflow Connections

def load_gold_to_postgres(**kwargs):
    ti=kwargs["ti"]
    gold_file=ti.xcom_pull(key="gold_file",task_ids="gold_aggregate")

    df=pd.read_csv(gold_file)

    conn=BaseHook.get_connection("postgres_default")

    pg_conn=psycopg2.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
        port=conn.port
    )

    cursor=pg_conn.cursor() # allows us to execute SQL commands in Python

    for _,row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO flights.flight_kpis
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                row["origin_country"],
                row["total_flights"],
                row["avg_velocity"],
                row["max_velocity"],
                row["min_velocity"],
                row["avg_altitude"],
                row["max_altitude"],
                row["min_altitude"],
                row["grounded"],
                row["climbing"],
                row["descending"],
                row["airborne"],
                row["ground_ratio"]
            )
        )
    pg_conn.commit() # stores data only after commit
    cursor.close() # closes the cursor, not the connection. 
    pg_conn.close() 