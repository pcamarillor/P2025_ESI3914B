import random 
import time
import os 
from datetime import datetime as dt

output_dir = "/home/jovyan/notebooks/data/instagram_logs"
os.makedirs(output_dir, exist_ok=True)

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

def generate_tweet():
    timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    user = random.choice(user_ids)
    text = random.choice(insta_texts)
    likes = random.randint(0,5000)
    return f"{timestamp} | instagram | {user} | {text} | {likes}"

while True:
    
    # Write to file
    filename = f"{output_dir}/insta_{int(time.time())}.log"
    with open(filename, 'a') as f:
        n = random.randint(30,300)
        # Create a random number of lines
        _ = [f.write(generate_tweet() + "\n") for i in range(n)]

    # Wait 10 seconds
    time.sleep(5)
