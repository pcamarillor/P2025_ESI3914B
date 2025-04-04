import time
import random
import os
import string

# Configuration
output_dir = "/home/jovyan/notebooks/data/log/input"
os.makedirs(output_dir, exist_ok=True)

def generate_random_traffic_line():
        # Generate random data
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        type = random.choice(['| Error | 500 Internal Server Error', '| Info | Login Success', '| Warn | Disk Usage' ])
        server = random.choice(['| server-node-1','| server-node-2' ])
        log_line = f"{timestamp} {type} {server}"
        return log_line

while True:
    # Write to file
    filename = f"{output_dir}/data_{int(time.time())}.log"
    with open(filename, 'a') as f:
        n = random.randint(30,300)
        # Create a random number of lines
        _ = [f.write(generate_random_traffic_line() + "\n") for i in range(n)]

    # Wait 5 seconds
    time.sleep(5)