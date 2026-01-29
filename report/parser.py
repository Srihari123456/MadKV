import sys
import re
from collections import defaultdict

def parse_log_file(input_filename, output_filename):
    operation_times = defaultdict(list)

    # Read log file and extract execution times
    with open(input_filename, 'r') as file:
        for line in file:
            match = re.match(r"(\w+) execution time: (\d+) microseconds", line)
            if match:
                operation, time_taken = match.groups()
                operation_times[operation].append(int(time_taken))

    # Compute average times
    avg_times = {op: sum(times) / len(times) for op, times in operation_times.items()}

    # Write results to a new file
    with open(output_filename, 'w') as file:
        file.write("--- Average Execution Times ---\n")
        for op, avg_time in avg_times.items():
            file.write(f"{op} average execution time: {avg_time:.2f} microseconds\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python parse_log.py <input_log_filename> <output_filename>")
        sys.exit(1)

    input_log_filename = sys.argv[1]
    output_filename = sys.argv[2]
    parse_log_file(input_log_filename, output_filename)
    print(f"Processed log file: {input_log_filename}")
    print(f"Results written to: {output_filename}")
