import random
import time
from datetime import datetime

servers = ['server-node-1', 'server-node-2', 'server-node-3']
levels = ['INFO', 'WARN', 'ERROR']
messages = {
    'INFO': ['User-login-successful', 'Service-started', 'Connection-established'],
    'WARN': ['Disk-usage-85%', 'High-memory-usage', 'Slow-response-time'],
    'ERROR': ['500-Internal Server-Error', '404-Not-Found', 'Connection-timeout']
}

def generate_log_entries(num_entries=5):
    entries = []
    for _ in range(num_entries):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        server = random.choice(servers)
        
        if random.random() < 0.15:
            level = 'ERROR'
            message = '500-Internal-Server-Error'
            server = 'server-node-2'
        else:
            level = random.choices(levels, weights=[0.6, 0.3, 0.1], k=1)[0]
            message = random.choice(messages[level])
        
        entries.append(f"{timestamp} {level} {message} {server}")
    
    return "\n".join(entries)

log_dir = "../spark_cluster/data/log_data/input"
min_entries_per_file = 49
max_entries_per_file = 80

while True:
    num_entries = random.randint(min_entries_per_file, max_entries_per_file)
    log_content = generate_log_entries(num_entries)
    
    filename = f"{log_dir}/log_{int(time.time() * 1000)}.log"
    with open(filename, "w") as f:
        f.write(log_content + "\n")
    
    print(f"Generated {filename} with {num_entries} entries")
    time.sleep(random.uniform(1.0, 3.0))