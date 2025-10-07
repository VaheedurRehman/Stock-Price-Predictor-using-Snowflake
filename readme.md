# 📈 Stock Price Forecasting Pipeline (Airflow + Snowflake + YFinance)

This project implements a **fully automated data pipeline** for stock price forecasting using **Apache Airflow**, **Snowflake**, and the **YFinance API**.  
It retrieves historical stock data, stores it securely in Snowflake, and predicts future prices using **Snowflake’s ML forecasting** features.

---

## 🧠 Overview

The pipeline consists of two main **Airflow DAGs**:

1. **`fetch_stock_price_dag.py`** — Fetches and stores the latest 180 days of stock data.  
2. **`forecast_stock_price_dag.py`** — Runs forecasting on the stored data and writes the predictions back to Snowflake.

Both DAGs are orchestrated in Airflow to run on a schedule, enabling **automated, continuous updates** of both raw and forecasted stock data.

---

## 🏗️ System Architecture

The system integrates four core components:

- **YFinance API** – Data source for real-time and historical market data  
- **Apache Airflow** – Workflow orchestration for ETL and ML tasks  
- **Snowflake** – Cloud data warehouse for storage, transformation, and forecasting  
- **Docker** – Containerized environment for Airflow and PostgreSQL setup  

### **Workflow Summary**
1. Airflow triggers data ingestion from YFinance.  
2. Raw stock data is loaded into Snowflake.  
3. Snowflake performs ELT transformations and time-series forecasting.  
4. Forecasted prices are written to a final table for analytics and visualization.  

---

## ⚙️ Airflow DAGs

### 1. Data Ingestion — `fetch_stock_price_dag.py`
- Fetches 180 days of OHLCV (Open, High, Low, Close, Volume) data for multiple tickers.  
- Cleans and transforms the data.  
- Loads into the **`STOCK_PRICES`** table in Snowflake.  
- Scheduled to run daily.

### 2. Forecasting — `forecast_stock_price_dag.py`
- Reads the latest stock data from Snowflake.  
- Applies time-series forecasting (e.g., ARIMA or `ML.FORECAST()` in Snowflake).  
- Stores results in the **`STOCK_FORECAST`** table.  
- Produces 7-day ahead forecasts.  


---

## 🧰 Technologies Used

| Component | Purpose |
|------------|----------|
| **Apache Airflow** | DAG orchestration, task scheduling |
| **Snowflake** | Cloud data warehouse & forecasting |
| **YFinance API** | Data ingestion from Yahoo Finance |
| **Docker** | Containerization of Airflow & PostgreSQL |
| **Python 3.x** | Scripting and ETL logic |

---

## 🚀 Setup & Deployment

### 1. Clone the Repository
```bash
git clone https://github.com/VaheedurRehman/stock-forecasting-pipeline.git
cd stock-forecasting-pipeline
```

### 2. Configure Airflow
Place both DAG files inside your Airflow `dags/` directory:
```
fetch_stock_price_dag.py
forecast_stock_price_dag.py
```

Update **Airflow connections**:
- `snowflake_default` → Your Snowflake credentials  
- `yfinance_api` → Optional (if using a custom connection wrapper)

### 3. Configure Snowflake
Run the following SQL commands in Snowflake to create tables:
```sql
CREATE TABLE STOCK_PRICES (
  SYMBOL STRING,
  DATE DATE,
  OPEN FLOAT,
  HIGH FLOAT,
  LOW FLOAT,
  CLOSE FLOAT,
  VOLUME FLOAT
);

CREATE TABLE STOCK_FORECAST (
  SYMBOL STRING,
  DATE DATE,
  PREDICTED_CLOSE FLOAT
);
```

### 4. Start the Airflow Environment
If using Docker:
```bash
docker-compose up -d
```

### 5. Trigger DAGs
Access the Airflow UI (`http://localhost:8080`):
- Enable `fetch_stock_price_dag`
- Enable `forecast_stock_price_dag`

---

## 📊 Outputs

After successful execution:

- **STOCK_PRICES** → Historical data  
- **`STOCK_FORECAST`** → 7-day forecasted prices  
- Combined results available for visualization or dashboarding  


---

## 🧩 Example SQL Queries

```sql
-- View historical prices
SELECT * FROM STOCK_PRICES WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC;

-- View forecasted prices
SELECT * FROM STOCK_FORECAST WHERE SYMBOL = 'AAPL' ORDER BY DATE DESC;

-- Combine actual and predicted data
SELECT p.SYMBOL, p.DATE, p.CLOSE, f.PREDICTED_CLOSE
FROM STOCK_PRICES p
JOIN STOCK_FORECAST f
ON p.SYMBOL = f.SYMBOL AND p.DATE = f.DATE;
```

---

## 📚 References

- [YFinance Documentation](https://pypi.org/project/yfinance/)  
- [Apache Airflow Docs](https://airflow.apache.org/docs/)  
- [Snowflake AI/ML Features](https://www.snowflake.com/en/data-cloud/workloads/ai-ml/)

---

## 🧑‍💻 Authors

- **Vaheedur Rehman Mahmed** — San Jose State University
- **Tejas Nandkishor Sawant** — San Jose State University  
  

---
