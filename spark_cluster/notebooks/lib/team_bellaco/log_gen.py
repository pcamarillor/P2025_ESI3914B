import random
import datetime
from pathlib import Path

log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
messages = {
    "INFO": ["User login successful", "Scheduled job executed", "Configuration loaded"],
    "WARN": ["Disk usage 85%", "High memory usage detected", "Slow response time"],
    "ERROR": ["500 Internal Server Error", "Database connection failed", "Application crash detected"],
    "DEBUG": ["Debugging mode enabled", "Variable X initialized", "Performance metrics logged"]
}
servers = ["server-node-1", "server-node-2", "server-node-3"]

def generate_log_entry():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choice(log_levels)
    message = random.choice(messages[level])
    server = random.choice(servers)
    return f"{timestamp} | {level} | {message} | {server}"

def generate_logs(n, log_dir, filename):
    
    for file in range(n):
        currentFilename = filename + "-" + str(file) + ".log"
        log_path = Path(log_dir) / currentFilename
        log_path.parent.mkdir(parents=True, exist_ok=True) 
        
        with open(log_path, "a") as log_file:
            for line in range(n):
                log_entry = generate_log_entry()
                log_file.write(log_entry + "\n")        
    
    print(f"Logs guardados en: {log_path}")

