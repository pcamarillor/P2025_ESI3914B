# Real-Time Financial Data Stream with Apache Spark and Kafka

This project simulates a real-time financial data pipeline using historical stock data from Yahoo Finance. It streams processed data through Kafka, consumes it with Apache Spark Structured Streaming, and writes it to disk in Parquet format. This data is used to train ML model and create forecasts in stock prices.

## 🧱 Architecture

- **Data Source**: Parquet files of historical Yahoo Finance stock data
- **Producer**: Python script that reads the data and sends JSON messages to Kafka
- **Kafka Broker**: Acts as a buffer between the producer and the Spark consumer
- **Consumer**: Apache Spark Structured Streaming job that processes incoming messages and stores them as Parquet files

## ⚙️ How we include the 5 V's of BigData
- **Volume**: The project uses "historical stock data." Depending on the timeframe and number of stocks, this can represent a significant volume of data.
Storing data in Parquet format is efficient for large datasets.
Apache Spark is designed to handle large-scale data processing.
Our application manages 3kb every 5 seconds which equals to 2.1Mb per hour, 50.4Mb per day or 17.96Gb per year.
However if you request information every second for faster and more precise data this can go to 89.82Gb per year. 

- **Velocity**: The core of your project is a "real-time financial data stream."
Using Kafka as a message broker and Apache Spark Structured Streaming for consumption directly addresses high-velocity data.
The producer sends messages to Kafka, and the consumer processes them as they arrive, indicating a continuous flow of data.

- **Variety**: The data source is historical stock data from Yahoo Finance.
The producer sends JSON messages to Kafka. JSON is a flexible format that can handle structured and semi-structured data.
The data is ultimately stored in Parquet, a columnar storage format suitable for structured data.

![Financial Data Variety Visualization](./example-kafka.jpeg)

- **Veracity**: Financial data inherently requires a degree of accuracy.
The proyect uses train ML model to create forecasts. The reliability of the input data is crucial for the accuracy of these models and forecasts.
Implicitly, by choosing established sources like Yahoo Finance, there's an assumption of a certain level of data veracity.

- **Value**: The project aims to "train ML model and create forecasts in stock prices."
This directly translates to generating valuable insights – predicting future stock movements, which can inform investment decisions or risk management strategies.
The real-time aspect adds value by enabling timely analysis and potentially faster reactions to market changes.


## ⚒️ Conclusions

This project successfully demonstrated the construction of a real-time financial data pipeline leveraging Apache Kafka for message brokering and Apache Spark Structured Streaming for data consumption and processing. Key outcomes include the ability to ingest streaming stock data, process it in real-time, and store it efficiently in Parquet format, laying the groundwork for subsequent machine learning model training and stock price forecasting.

Key learnings from this project include:
- Practical application of setting up and managing a streaming data flow with Kafka and Spark.
- Understanding the benefits of using Parquet for efficient storage and Spark for scalable processing of large and fast-moving datasets.
- Gaining insights into how the 5 V's of Big Data (Volume, Velocity, Variety, Veracity, and Value) manifest in a financial data context and how the chosen technologies address these characteristics.
- Recognizing the importance of data integrity and the potential for real-time analytics to derive significant value from financial data streams.

The architecture established provides a robust foundation for developing more complex analytical models and real-time decision-making systems based on financial market data.
