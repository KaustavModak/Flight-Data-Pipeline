# ✈️ Flight Data Engineering Pipeline (Medallion Architecture)

This project implements an **end-to-end data engineering pipeline** using **Apache Airflow, PostgreSQL, and Docker**.  
The pipeline collects live flight data, processes it through **Medallion Architecture (Bronze → Silver → Gold)**, performs **data quality checks**, and loads analytics-ready data into a **PostgreSQL data warehouse**.

---

# 📌 Project Overview

Modern data platforms use layered architectures to transform raw data into reliable analytical datasets.  
This project simulates a **production-style data pipeline** that:

1. Extracts flight data from the **OpenSky Network API**
2. Stores raw data in the **Bronze layer**
3. Cleans and structures data in the **Silver layer**
4. Generates analytical aggregates in the **Gold layer**
5. Runs **data quality validations**
6. Loads results into a **PostgreSQL warehouse**

---

# 🏗 Architecture

```
OpenSky API
     │
     ▼
Bronze Layer (Raw Data)
     │
     ▼
Silver Layer (Cleaned Data)
     │
     ▼
Gold Layer (Aggregated KPIs)
     │
     ▼
Data Quality Checks
     │
     ▼
PostgreSQL Data Warehouse
```

The pipeline is orchestrated using **Apache Airflow DAGs** running inside Docker containers.

---

# ⚙️ Technologies Used

- **Apache Airflow** – workflow orchestration  
- **Docker & Docker Compose** – containerized infrastructure  
- **Python** – ETL processing  
- **Pandas** – data transformation  
- **PostgreSQL** – analytical data warehouse  
- **OpenSky Network API** – flight data source  

---

# 📂 Project Structure

```
flight-data-pipeline/
│
├── dags/
│   └── flight_pipeline.py
│
├── scripts/
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_aggregate.py
│   ├── data_quality.py
│   └── load_to_postgres.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker-compose.yaml
├── requirements.txt
├── .env.example
├── README.md
└── .gitignore
```

---

# 🔄 Pipeline Workflow

## 1️⃣ Bronze Layer – Raw Ingestion

File: `bronze_ingest.py`

- Calls **OpenSky API**
- Stores raw flight data
- Saves timestamped files in:

```
data/bronze/
```

---

## 2️⃣ Silver Layer – Data Cleaning

File: `silver_transform.py`

Transforms raw data:

- Remove missing values
- Convert data types
- Select relevant columns
- Save cleaned dataset

```
data/silver/
```

---

## 3️⃣ Gold Layer – Aggregations

File: `gold_aggregate.py`

Creates analytical metrics per country:

- Total flights
- Average velocity
- Altitude statistics
- Grounded aircraft ratio
- Climb / descent counts

Output stored in:

```
data/gold/
```

Example output:

| origin_country | total_flights | avg_velocity |
|----------------|--------------|--------------|
| Algeria | 4 | 222.57 |
| Argentina | 23 | 186.46 |

---

## 4️⃣ Data Quality Checks

File: `data_quality.py`

Validates Gold data before loading.

Checks include:

- Dataset is not empty
- No NULL values in critical columns
- No negative flight counts
- Schema validation

If validation fails:

```
Pipeline stops
Data is not loaded into the warehouse
```

---

## 5️⃣ Load to PostgreSQL

File: `load_to_postgres.py`

Final step loads aggregated data into:

```
PostgreSQL → flights.flight_kpis
```

Data can then be used for:

- dashboards
- analytics
- reporting

---

# 🚀 Running the Project

## 1️⃣ Clone the repository

```bash
git clone https://github.com/KaustavModak/Flight-Data-Pipeline.git
cd Flight-Data-Pipeline
```

---

## 2️⃣ Create environment file

Copy example environment configuration:

```bash
cp .env.example .env
```

---

## 3️⃣ Start Airflow + PostgreSQL

```bash
docker compose up -d
```

This starts:

- PostgreSQL
- Airflow Webserver
- Airflow Scheduler

---

## 4️⃣ Access Airflow UI

```
http://localhost:8080
```

Login credentials:

```
username: admin
password: admin
```

---

## 5️⃣ Run the Pipeline

Enable DAG:

```
flights_ops_medallion_pipe
```

Run manually or let it execute on schedule.

---

# 📊 Example SQL Query

After the pipeline runs, query the warehouse:

```sql
SELECT * FROM flights.flight_kpis;
```

Example result:

| origin_country | total_flights | avg_velocity | ground_ratio |
|----------------|--------------|--------------|--------------|
| Algeria | 4 | 222.57 | 0.00 |
| Argentina | 23 | 186.46 | 0.04 |

---

# 📈 Future Improvements

Possible extensions:

- Bulk loading using **PostgreSQL COPY**
- Add **data lineage tracking**
- Integrate **Great Expectations for validation**
- Add **BI dashboards (Metabase / Superset)**
- Implement **data partitioning**

---

# 🎯 Learning Outcomes

This project demonstrates practical experience with:

- ETL pipeline development
- Airflow orchestration
- Medallion architecture
- Data quality validation
- Warehouse loading
- Dockerized data infrastructure

---

# 👨‍💻 Author

Kaustav Modak  
Data Engineering & Analytics Projects