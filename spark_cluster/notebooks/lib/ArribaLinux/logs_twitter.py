from kafka import KafkaProducer
import random 
import time
import os 
from datetime import datetime as dt

output_dir = "/home/jovyan/notebooks/data/twitter_logs"
os.makedirs(output_dir, exist_ok=True)

producer = KafkaProducer(
    bootstrap_servers='78a305ddc318:9093',
    value_serializer=lambda v: v.encode('utf-8')
)

user_ids = [f"user_{i}" for i in range(1, 51)]
twits = [
    "Just had the best coffee ever!", 
    "Traffic is terrible today", 
    "Can't wait for the weekend!", 
    "Why is the internet so slow?", 
    "Loving this new playlist!", 
    "Worst service ever", 
    "Amazing sunset", 
    "My dog is the cutest", 
    "Feeling blessed", 
    "Missed the bus again"
]

def generate_tweet():
    timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    user = random.choice(user_ids)
    text = random.choice(twits)
    likes = random.randint(0,5000)
    return f"{timestamp} | twitter | {user} | {text} | {likes}"

while True:
    for _ in range(random.randint(30, 50)):
        tweet = generate_tweet()
        producer.send("twitter_topic", tweet)
        print(f"Sent to twitter_topic: {tweet}")
    time.sleep(5)
