#!/usr/bin/env python3
import sys
import time
from datetime import datetime

def parse_proc_interrupts(target_cpus):
    try:
        with open("/proc/interrupts", "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        print("Error: /proc/interrupts not found.", file=sys.stderr)
        sys.exit(1)

    if not lines:
        return {}

    # Extract header to map the exact column indices for the target CPUs
    header = lines[0].split()

    cpu_col_indices = {}
    for target_cpu in target_cpus:
        cpu_str = f"CPU{target_cpu}"
        if cpu_str not in header:
            print(f"Error: {cpu_str} not found in /proc/interrupts header.", file=sys.stderr)
            print(f"Available CPUs: {', '.join(header)}", file=sys.stderr)
            sys.exit(1)
        cpu_col_indices[target_cpu] = header.index(cpu_str)

    # Initialize a dictionary to store counts per CPU
    interrupt_counts = {cpu: {} for cpu in target_cpus}

    for line in lines[1:]:
        parts = line.split()
        if not parts:
            continue

        irq_label = parts[0].rstrip(":")

        # Determine the width of the CPU columns based on the header
        num_cpus = len(header)

        try:
            # Slice the list to target only the CPU interrupt counts
            counts = parts[1 : num_cpus + 1]

            # Reconstruct the trailing strings to form the IRQ description
            desc_parts = parts[num_cpus + 1 :]
            if desc_parts:
                irq_type = f"{irq_label} ({' '.join(desc_parts)})"
            else:
                irq_type = irq_label

            # Extract the count for each requested CPU
            for cpu, col_index in cpu_col_indices.items():
                if len(counts) > col_index:
                    count_val = int(counts[col_index])
                    interrupt_counts[cpu][irq_type] = count_val

        except ValueError:
            # Ignore lines containing non-numeric data in the CPU columns
            continue

    return interrupt_counts

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <CPU_ID_1> [CPU_ID_2] ...")
        print(f"Example: python3 {sys.argv[0]} 0 4 7")
        sys.exit(1)

    target_cpus = sys.argv[1:]
    interval = 0.2

    print(f"Monitoring /proc/interrupts for CPUs: {', '.join(target_cpus)} every 200ms...")

    prev_counts = parse_proc_interrupts(target_cpus)

    try:
        while True:
            time.sleep(interval)
            curr_counts = parse_proc_interrupts(target_cpus)

            # Grab the current time for this polling cycle
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            for cpu in target_cpus:
                # Ensure the CPU exists in the current snapshot
                if cpu not in curr_counts:
                    continue

                for irq_type, curr_count in curr_counts[cpu].items():
                    prev_count = prev_counts[cpu].get(irq_type, curr_count)
                    delta = curr_count - prev_count

                    if delta > 0:
                        print(f"[{timestamp}] CPU{cpu} | {irq_type}: {delta}")

            prev_counts = curr_counts

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()