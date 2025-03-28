from time import sleep
from datetime import datetime, timedelta
import random
import os

levels = ["INFO", "WARN", "ERROR"]
messages = [
    "User login successful",
    "Disk usage 85%",
    "500 Internal Server Error",
    "Database connection lost",
    "Memory usage high",
    "File uploaded successfully"
]
servers = ["server-node-1", "server-node-2", "server-node-3"]
source_dir = "/home/jovyan/notebooks/data/log_streaming/"

os.makedirs(source_dir, exist_ok=True)

timestamp = datetime.now()

while True:
    n = random.randint(30, 100)
    output = f"{source_dir}{datetime.now().strftime('%Y%m%d%H%M%S')}.log"

    with open(output, 'w') as file:
        for _ in range(n):
            timestamp += timedelta(microseconds=7000)
            level = random.choice(levels)
            message = random.choice(messages)
            server = random.choice(servers)
            log_entry = f"{timestamp} | {level} | {message} | {server}\n"
            file.write(log_entry)

    print(f"Archivo generado: {output}")
    sleep(2)