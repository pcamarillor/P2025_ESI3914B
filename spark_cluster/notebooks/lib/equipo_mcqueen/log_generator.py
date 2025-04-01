import random
import time
from datetime import datetime

servers = ['server-node-1', 'server-node-2', 'server-node-3']
levels = ['INFO', 'WARN', 'ERROR']
messages = {
    'INFO': ['User login successful', 'Service started'],
    'WARN': ['Disk usage 85%', 'High memory usage'],
    'ERROR': ['500 Internal Server Error', '404 Not Found']
}

def generate_log_entry():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    server = random.choice(servers)
    level = random.choices(levels, weights=[0.6, 0.3, 0.1], k=1)[0]
    
    if random.random() < 0.2:
        level = 'ERROR'
        message = '500 Internal Server Error'
        server = 'server-node-2'
    else:
        message = random.choice(messages[level])
    
    return f"{timestamp} | {level} | {message} | {server}"

log_dir = ""
while True:
    log_entry = generate_log_entry()
    filename = f"{log_dir}/log_{time.time()}.log"
    with open(filename, "w") as f:
        f.write(log_entry + "\n")
    time.sleep(random.randint(1, 5))