import random
import time
import datetime
import os

def generate_log_entry():
    # Fecha
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tipos = ["INFO", "WARN", "ERROR"]
    tipo = random.choice(tipos)
    
    # Opciones de descripcion
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
    
    # Formato
    return f"{fecha} | {tipo} | {description} | {server_node}"

def generate_log_file(filename, interval=5, entries_per_batch=None):
    try:
        while True:
            # Batches de entre 5-10
            if entries_per_batch is None:
                num_entries = random.randint(5, 10)
            else:
                num_entries = entries_per_batch
                
            # Generate and write the entries
            with open(filename, 'a') as f:
                for _ in range(num_entries):
                    log_entry = generate_log_entry()
                    f.write(log_entry + "\n")
            
            print(f"Added {num_entries} log entries to {filename}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nLog generation stopped by user")

if __name__ == "__main__":
    log_file = "application.log"
    print(f"Generando logs {log_file}")
    generate_log_file(log_file)