# Prerequisites for Running the Project

Before running this project, make sure you meet the following prerequisites:

## 1. Install Faker in the Jupyter Notebook

The project requires the [`Faker`](https://faker.readthedocs.io/) library to generate simulated data. To install it directly from a Jupyter notebook, run the following cell:

```python
!pip install Faker
```
## 2. Install Kakfa-python in the Jupyther Notebook

To enable communication with Kafka, install the kafka-python library. You can do this directly from a Jupyter notebook by running:

```python
!pip install kafka-python
```


## 3. Create Kafka Topics

You need to have 4 topics created in your Kafka instance. You can create them using the following command from your terminal, replacing `<id>` with the ID or name of your Kafka container:

```bash
docker exec -it <id> /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic tweet-1
```

```bash
docker exec -it <id> /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic tweet-2
```

```bash
docker exec -it <id> /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic tweet-3
```

```bash
docker exec -it <id> /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic tweet-4
```