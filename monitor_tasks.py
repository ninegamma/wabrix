#!/usr/bin/env python3
import sys
import time
import subprocess
from datetime import datetime

def get_tasks_on_cpus(target_cpus):
    try:
        # -e: all processes
        # -L: show threads (LWP - Light Weight Process)
        # -o: output format (PID, Thread ID, Processor, Command)
        result = subprocess.run(
            ["ps", "-e", "-L", "-o", "pid,lwp,psr,comm"],
            capture_output=True,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error running ps: {e}", file=sys.stderr)
        return {cpu: {} for cpu in target_cpus}
    except FileNotFoundError:
        print("Error: 'ps' command not found.", file=sys.stderr)
        sys.exit(1)

    lines = result.stdout.strip().split('\n')
    
    # Initialize a dictionary to store tasks per CPU
    # Format: {cpu_id: {tid: (pid, comm)}}
    tasks_per_cpu = {cpu: {} for cpu in target_cpus}
    
    for line in lines[1:]: # Skip the header
        # Split into exactly 4 parts to keep command names with spaces together
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        
        pid, tid, psr, comm = parts
        
        if psr in target_cpus:
            tasks_per_cpu[psr][tid] = (pid, comm)
            
    return tasks_per_cpu

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <CPU_ID_1> [CPU_ID_2] ...")
        print(f"Example: python3 {sys.argv[0]} 0 4 7")
        sys.exit(1)

    target_cpus = sys.argv[1:]
    
    # 1.0 second polling is recommended for process monitoring 
    # to avoid the script itself causing high CPU load.
    interval = 1.0 

    print(f"Monitoring tasks on CPUs: {', '.join(target_cpus)} every {interval}s...")
    print("Waiting for task transitions...\n")
    
    prev_tasks = get_tasks_on_cpus(target_cpus)

    try:
        while True:
            time.sleep(interval)
            curr_tasks = get_tasks_on_cpus(target_cpus)
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            for cpu in target_cpus:
                curr_cpu_tasks = curr_tasks.get(cpu, {})
                prev_cpu_tasks = prev_tasks.get(cpu, {})
                
                # Detect tasks that moved onto the monitored CPU (or were newly created)
                for tid, (pid, comm) in curr_cpu_tasks.items():
                    if tid not in prev_cpu_tasks:
                        print(f"[{timestamp}] CPU{cpu} | [+] Task ADDED   : {comm} (PID: {pid}, TID: {tid})")
                        
                # Detect tasks that moved off the monitored CPU (or terminated)
                for tid, (pid, comm) in prev_cpu_tasks.items():
                    if tid not in curr_cpu_tasks:
                        print(f"[{timestamp}] CPU{cpu} | [-] Task REMOVED : {comm} (PID: {pid}, TID: {tid})")

            prev_tasks = curr_tasks

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()
