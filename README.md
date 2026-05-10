# Stock Market Real-Time Data Analytics Pipeline on AWS

Disclaimer: This project was built as a hands-on AWS data analytics lab to demonstrate a near real-time stock market analytics pipeline using serverless and managed AWS services. The stock market data used in this project is fetched from a public finance API through `yfinance` for educational purposes only. This project is not intended for financial advice, production trading, or investment decision-making.

## Overview

This project demonstrates the design and deployment of a scalable, cost-efficient, and event-driven stock market data analytics pipeline on **AWS**. The solution continuously fetches stock market data from a Python script using **yfinance**, streams the data through **Amazon Kinesis Data Streams**, processes it with **AWS Lambda**, and stores the results in both **Amazon DynamoDB** and **Amazon S3**.

The pipeline supports real-time data processing, historical data storage, SQL-based analytics, and automated stock trend alerts. Raw stock data is archived in **Amazon S3** for long-term analysis, while processed stock records are stored in **Amazon DynamoDB** for low-latency lookups. **AWS Glue Data Catalog** structures the S3 data so it can be queried using **Amazon Athena**, and **Amazon SNS** sends Email/SMS alerts when trend changes are detected.

This project implements a **near real-time data analytics pipeline** rather than a fully real-time trading system. Stock data is streamed, processed, and stored with an intentional delay to keep AWS usage and costs low.

---

## Architecture Diagram

![image](https://github.com/user-attachments/assets/3d153707-5548-46c4-9a53-ca27786f4bd2)

*Figure 1: AWS Stock Market Real-Time Data Analytics Pipeline integrating Kinesis Data Streams, Lambda, DynamoDB, S3, Glue Data Catalog, Athena, and SNS.*

The architecture illustrates the full stock market data analytics workflow:
- **Data Streaming**: A local Python script continuously fetches stock data using `yfinance` and sends it to Amazon Kinesis Data Streams.
- **Real-Time Processing**: AWS Lambda is triggered by Kinesis to process, clean, enrich, and transform incoming stock records.
- **Storage Layer**: Raw data is stored in Amazon S3 for historical analysis, while processed data is stored in DynamoDB for fast lookups.
- **Historical Analytics**: AWS Glue Data Catalog structures the S3 data, allowing Amazon Athena to run SQL queries against historical stock records.
- **Trend Alerts**: DynamoDB Streams trigger a second Lambda function that calculates moving averages and sends stock trend alerts through Amazon SNS.

---

## Key Components

1. **Python Script**
   - **Role:** Acts as the stock data producer.
   - **Functionality:** Continuously fetches real-time stock data from `yfinance` and sends records to Amazon Kinesis Data Streams every 30 seconds.
   - **Security:** Uses AWS CLI credentials and IAM permissions to authenticate with AWS services.

2. **Amazon Kinesis Data Streams**
   - **Role:** Serves as the real-time streaming ingestion layer.
   - **Functionality:** Receives stock market data from the Python script and buffers it for downstream processing by AWS Lambda.
   - **Security:** Access is controlled through IAM policies that allow only authorized producers and consumers.

3. **AWS Lambda - Stock Data Processor**
   - **Role:** Processes incoming stock records from Kinesis.
   - **Functionality:** Decodes Kinesis records, stores raw JSON data in S3, calculates stock metrics such as price change, percentage change, moving average, and anomaly status, then writes processed records to DynamoDB.
   - **Security:** Uses a least-privilege IAM role with access to Kinesis, DynamoDB, S3, and CloudWatch Logs.

4. **Amazon S3 - Raw Stock Data Storage**
   - **Role:** Stores raw stock market data for long-term historical analysis.
   - **Functionality:** Saves original stock records in JSON format under organized S3 prefixes such as `raw-data/symbol/timestamp.json`.
   - **Security:** Bucket access is restricted, public access is blocked, and encryption at rest can be enabled.

5. **AWS Glue Data Catalog**
   - **Role:** Provides schema metadata for raw stock data stored in S3.
   - **Functionality:** Defines a structured table over JSON stock records so Amazon Athena can query the data using SQL.
   - **Security:** IAM permissions control access to Glue databases, tables, and crawled metadata.

6. **Amazon Athena**
   - **Role:** Enables serverless SQL analytics over historical stock data stored in S3.
   - **Functionality:** Runs analytical queries such as top price changes, average trading volume, and anomaly detection without requiring a traditional database server.
   - **Security:** Query access is controlled through IAM, and query results are stored in a dedicated S3 bucket.

7. **Amazon S3 - Athena Query Results**
   - **Role:** Stores Amazon Athena query output.
   - **Functionality:** Provides a dedicated S3 location where Athena saves SQL query results for review and reuse.
   - **Security:** Bucket policies and IAM permissions restrict access to query results.

8. **Amazon DynamoDB**
   - **Role:** Stores processed stock data for real-time and low-latency lookups.
   - **Functionality:** Uses `symbol` as the partition key and `timestamp` as the sort key to efficiently store and retrieve processed stock records.
   - **Security:** Access is controlled through IAM roles, and DynamoDB Streams are enabled for event-driven alerting.

9. **AWS Lambda - Trend Analysis**
   - **Role:** Analyzes processed stock records for trend changes.
   - **Functionality:** Triggered by DynamoDB Streams to calculate SMA-5 and SMA-20 moving averages and detect potential buy/sell signals.
   - **Security:** Uses an IAM role with access to DynamoDB, SNS, and CloudWatch Logs.

10. **Amazon SNS**
   - **Role:** Sends real-time stock trend alerts to users.
   - **Functionality:** Publishes Email/SMS notifications when the trend analysis Lambda detects an uptrend or downtrend based on moving average crossovers.
   - **Security:** Subscribers must confirm their subscription, and publish permissions are controlled through IAM.

---

## Data Analytics Methods

1. **Near Real-Time Stock Data Streaming**
   - A Python script fetches stock data from `yfinance`.
   - Records are formatted as JSON and sent to Amazon Kinesis Data Streams.
   - Kinesis acts as the ingestion layer for downstream serverless processing.

2. **Lambda-Based Data Processing**
   - AWS Lambda is triggered by incoming Kinesis records.
   - Raw stock records are written to Amazon S3.
   - Processed records are enriched with calculated metrics and stored in DynamoDB.
   - Anomalies are flagged when the price change percentage exceeds a defined threshold.

3. **Historical Querying with Athena**
   - AWS Glue Data Catalog defines a schema over raw S3 data.
   - Amazon Athena queries historical stock data directly from S3.
   - SQL queries can identify top price changes, average volume, and anomalous stock movements.

4. **Stock Trend Alerting**
   - DynamoDB Streams capture new processed stock records.
   - A second Lambda function calculates SMA-5 and SMA-20 moving averages.
   - Amazon SNS sends Email/SMS alerts when trend changes are detected.

---

## Traffic Flow

1. A Python script fetches stock data from `yfinance`.
2. The script sends JSON stock records to Amazon Kinesis Data Streams.
3. Kinesis triggers an AWS Lambda function for processing.
4. Lambda stores raw stock records in Amazon S3.
5. Lambda calculates metrics and stores processed records in Amazon DynamoDB.
6. AWS Glue Data Catalog structures the raw S3 data for querying.
7. Amazon Athena runs SQL queries against historical data stored in S3.
8. Athena stores query results in a dedicated S3 bucket.
9. DynamoDB Streams trigger a second Lambda function for trend analysis.
10. The trend analysis Lambda publishes Email/SMS alerts through Amazon SNS.

---
Here you go — clean `.md` block ready to paste directly into your README:

````md
## Example Athena SQL Queries

The following queries demonstrate how **Amazon Athena** can be used to analyze stock data stored in **Amazon S3**.

### 1. Preview Stock Data
```sql
SELECT *
FROM stock_data_table
LIMIT 10;
````

### 2. Top 5 Stocks by Price Change

```sql
SELECT symbol, price, previous_close,
       (price - previous_close) AS price_change
FROM stock_data_table
ORDER BY price_change DESC
LIMIT 5;
```

### 3. Average Trading Volume Per Stock

```sql
SELECT symbol, AVG(volume) AS avg_volume
FROM stock_data_table
GROUP BY symbol;
```

### 4. Detect Anomalous Stock Movements (>5% Change)

```sql
SELECT symbol, price, previous_close,
       ROUND(((price - previous_close) / previous_close) * 100, 2) AS change_percent
FROM stock_data_table
WHERE ABS(((price - previous_close) / previous_close) * 100) > 5;
```

### 5. Daily Average Closing Price

```sql
SELECT symbol,
       DATE(timestamp) AS trading_day,
       AVG(price) AS avg_price
FROM stock_data_table
GROUP BY symbol, DATE(timestamp)
ORDER BY trading_day DESC;
```

### 6. Highest Volume Trades

```sql
SELECT symbol, volume, timestamp
FROM stock_data_table
ORDER BY volume DESC
LIMIT 10;
```

### 7. Price Trend Over Time (Single Stock)

```sql
SELECT timestamp, price
FROM stock_data_table
WHERE symbol = 'AAPL'
ORDER BY timestamp ASC;
```
These queries highlight Athena's ability to perform **serverless SQL analytics** directly on data stored in S3, enabling fast and cost-effective insights without managing database infrastructure.


## Objectives

- **Real-Time Data Ingestion:** Stream stock market data into AWS using Amazon Kinesis Data Streams.
- **Serverless Processing:** Use AWS Lambda to process, clean, transform, and enrich incoming stock data.
- **Low-Latency Storage:** Store processed stock records in DynamoDB for fast lookups.
- **Historical Analytics:** Store raw JSON records in S3 and query them using Glue Data Catalog and Athena.
- **Automated Trend Detection:** Calculate SMA-5 and SMA-20 moving averages to identify trend changes.
- **Real-Time Alerts:** Send stock movement notifications through Amazon SNS using Email/SMS.
- **Cost Optimization:** Use serverless and managed AWS services to keep the project cost around $1 to $2 for lab usage.

---

## Security Considerations

- **IAM Access Control:** IAM roles and policies control access between Kinesis, Lambda, DynamoDB, S3, Athena, Glue, and SNS.
- **Least Privilege Permissions:** Lambda functions should be assigned only the permissions required for their specific tasks.
- **S3 Security:** S3 buckets should block public access and use encryption at rest for raw stock data and Athena query results.
- **Credential Management:** AWS CLI credentials are used locally for the Python script and should never be hardcoded in source code.
- **Monitoring:** CloudWatch Logs track Lambda execution, processing errors, and pipeline activity.
- **Cost Control:** The Python streaming script should be stopped with `CTRL+C` when testing is complete to avoid unnecessary Kinesis and Lambda usage.

---

## Conclusion

This project demonstrates the implementation of a **near real-time AWS stock market analytics pipeline** that streams, processes, stores, queries, and analyzes stock data using serverless and managed AWS services.

By integrating **Amazon Kinesis Data Streams**, **AWS Lambda**, **Amazon DynamoDB**, **Amazon S3**, **AWS Glue Data Catalog**, **Amazon Athena**, and **Amazon SNS**, the architecture provides a complete workflow for data ingestion, real-time processing, historical analytics, and automated alerting.

The solution is scalable, cost-efficient, and designed for hands-on learning. While this project is not intended for production trading or fully real-time financial decision-making, it reflects practical cloud data engineering principles and demonstrates how AWS services can be combined to build an end-to-end analytics pipeline.
