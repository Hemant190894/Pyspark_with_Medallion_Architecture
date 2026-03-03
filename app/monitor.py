import psutil
import os
import threading
import time

def log_resource_usage(logger, interval=10):
    """
    Ek alag thread mein chalta hai aur periodic interval par 
    Memory aur CPU usage log karta hai.
    """
    process = psutil.Process(os.getpid())
    
    def monitor():
        while True:
            # Memory in GB
            mem_info = process.memory_info().rss / (1024 ** 3) 
            # CPU percent (interval 1s for accuracy)
            cpu_usage = psutil.cpu_percent(interval=1)
            # Thread count
            threads = threading.active_count()
            
            logger.info(
                f" [RESOURCE MONITOR] "
                f"Memory: {mem_info:.2f} GB | "
                f"CPU: {cpu_usage}% | "
                f"Active Threads: {threads}"
            )
            time.sleep(interval)

    # Daemon thread ensures it stops when main script ends
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()