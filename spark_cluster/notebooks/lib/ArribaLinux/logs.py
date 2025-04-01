import random
import time
import os
from datetime import datetime as dt

output_dir = "/home/jovyan/notebooks/data/logs"
os.makedirs(output_dir, exist_ok=True)

log_info = ["INFO", "WARN", "ERROR"]
messages = {
    "INFO": ["User login successful", "File uploaded", "Connection established"],
    "WARN": ["Disk usage 85%", "Memory usage high", "Response time slow"],
    "ERROR": ["500 Internal Server Error", "503 Service Unavailable", "Connection timeout"]
}

servers = ["server-node-1", "server-node-2", "server-node-3"]

def generate_log():
    timestamp = dt.now().strftime("%Y-%m-%d $H:%M:%S")
    info = random.choice(log_info)
    message = random.choice(messages[info])
    server = random.choice(servers)

    return f"{timestamp} | {info} | {message} | {server}"

while True:
    
    # Write to file
    filename = f"{output_dir}/data_{int(time.time())}.log"
    with open(filename, 'a') as f:
        n = random.randint(30,300)
        # Create a random number of lines
        _ = [f.write(generate_log() + "\n") for i in range(n)]

    # Wait 10 seconds
    time.sleep(5)