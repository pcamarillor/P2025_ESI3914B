import os
import time
import random
from datetime import datetime

# Log levels and messages
log_levels = ["INFO", "WARN", "ERROR"]
error_messages = [
    "500 Internal Server Error",
    "404 Not Found",
    "403 Forbidden",
    "502 Bad Gateway"
]

# Servers
servers = ["server-node-1", "server-node-2", "server-node-3"]

# Log directory
log_dir = "spark_cluster/data/strucutred_streaming_files/"

# Crear el directorio si no existe
os.makedirs(log_dir, exist_ok=True)

i = 0  # File counter
start_time = time.time()

# Generate logs
while True:
    log_file = f"{log_dir}server_logs{i}.txt"
    with open(log_file, "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = random.choice(log_levels)
        message = random.choice(error_messages) if level == "ERROR" else "User login successful"
        server = random.choice(servers)
        log_entry = f"{timestamp} | {level} | {message} | {server}\n"
        print(log_entry.strip())
        f.write(log_entry)
    
    time.sleep(random.randint(1, 5))
    
    # Create a new file every 20 seconds
    if time.time() - start_time >= 20:
        i += 1
        start_time = time.time()