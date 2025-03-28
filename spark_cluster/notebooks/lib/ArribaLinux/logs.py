import random
import time
from datetime import datetime as dt

log_file = "spark_cluster/data/logs.txt"

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

with open(log_file, "a") as file:
    while True:
        log_entry = generate_log()
        print(log_entry)
        file.write(log_entry + "\n")
        file.flush()
        time.sleep(random.uniform(1, 3))