import random 
import time
import os 
from datetime import datetime as dt

output_dir = "/home/jovyan/notebooks/data/twitter_logs"
os.makedirs(output_dir, exist_ok=True)

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
    
    # Write to file
    filename = f"{output_dir}/tweet_{int(time.time())}.log"
    with open(filename, 'a') as f:
        n = random.randint(30,300)
        # Create a random number of lines
        _ = [f.write(generate_tweet() + "\n") for i in range(n)]

    # Wait 10 seconds
    time.sleep(5)
