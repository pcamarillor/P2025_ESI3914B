from kafka import KafkaProducer
import random 
import time
import os 
from datetime import datetime as dt

output_dir = "/home/jovyan/notebooks/data/instagram_logs"
os.makedirs(output_dir, exist_ok=True)

producer = KafkaProducer(
    bootstrap_servers='78a305ddc318:9093',
    value_serializer=lambda v: v.encode('utf-8')
)

user_ids = [f"user_{i}" for i in range(1, 51)]
insta_texts = [
    "Look at this outfit!", 
    "Beach days are the best", 
    "Throwback to last summer", 
    "This place is magical", 
    "So in love with this meal", 
    "My heart is full", 
    "Feeling cute", 
    "Morning vibes", 
    "Besties forever", 
    "Workout done"
]

def generate_post():
    timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    user = random.choice(user_ids)
    text = random.choice(insta_texts)
    likes = random.randint(0,5000)
    return f"{timestamp} | instagram | {user} | {text} | {likes}"

while True:
    for _ in range(random.randint(30, 50)):
        post = generate_post()
        producer.send("instagram_topic", post)
        print(f"Sent to instagram_topic: {post}")
    time.sleep(5)
