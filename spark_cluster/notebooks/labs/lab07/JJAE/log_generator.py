import random
import time
import datetime
import os

def generate_log_entry():
    # Current date and time
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Random log type
    tipos = ["INFO", "WARN", "ERROR"]
    tipo = random.choice(tipos)
    
    # Descriptions based on log type
    descriptions = {
        "INFO": [
            "Application started successfully",
            "User login successful",
            "Data processing completed",
            "Configuration loaded",
            "Database connection established",
            "Cache refreshed",
            "Scheduled task executed"
        ],
        "WARN": [
            "High memory usage detected",
            "Slow database query",
            "Connection attempt retry",
            "Timeout on external API call",
            "Resource utilization above threshold",
            "Configuration parameter missing, using default"
        ],
        "ERROR": [
            "Database connection failed",
            "Authentication error",
            "File not found",
            "Out of memory error",
            "Unhandled exception",
            "API request failed",
            "Data corruption detected"
        ]
    }
    description = random.choice(descriptions[tipo])
    
    # Random server node
    server_node = f"Server-node-{random.randint(1, 5)}"
    
    # Format: Fecha | Tipo | DESCRIPTION | Server-node-x
    return f"{fecha} | {tipo} | {description} | {server_node}"

def generate_batch_log_files(log_dir='logs', interval=5, entries_per_batch=None):
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    batch_count = 1
    
    try:
        while True:
            # Determine number of entries in this batch (5-10 if not specified)
            if entries_per_batch is None:
                num_entries = random.randint(5, 10)
            else:
                num_entries = entries_per_batch
                
            # Create a unique filename for this batch using timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"batch_{batch_count}_logs_{timestamp}.log")
                
            # Generate and write the entries to a new file
            with open(log_file, 'w') as f:
                for _ in range(num_entries):
                    log_entry = generate_log_entry()
                    f.write(log_entry + "\n")
            
            print(f"Created log file {log_file} with {num_entries} entries")
            batch_count += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nLog generation stopped by user")

if __name__ == "__main__":
    print(f"Starting batch log generation")
    generate_batch_log_files()