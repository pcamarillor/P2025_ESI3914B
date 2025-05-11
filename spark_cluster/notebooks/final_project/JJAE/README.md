# Real-Time Financial Data Stream with Apache Spark and Kafka

This project simulates a real-time financial data pipeline using historical stock data from Yahoo Finance and FinnHub. It streams processed data through Kafka, consumes it with Apache Spark Structured Streaming, and writes it to disk in Parquet format. This data is used to train ML model and create forecasts in stock prices.

## 🧱 Architecture

- **Data Source**: Parquet files of historical Yahoo Finance stock data
- **Producer**: Python script that reads the data and sends JSON messages to Kafka
- **Kafka Broker**: Acts as a buffer between the producer and the Spark consumer
- **Consumer**: Apache Spark Structured Streaming job that processes incoming messages and stores them as Parquet files