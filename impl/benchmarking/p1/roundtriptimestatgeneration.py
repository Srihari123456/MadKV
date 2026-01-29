import sys
import os
import re
import matplotlib.pyplot as plt

def extract_avg_times(file_path):
    """Extract average execution times from a log file."""
    avg_times = {}
    with open(file_path, 'r') as file:
        for line in file:
            match = re.match(r"(\w+) average execution time: ([\d.]+) microseconds", line)
            if match:
                operation, avg_time = match.groups()
                avg_times[operation] = float(avg_time)
    return avg_times

def plot_execution_times(directory):
    """Reads all files in the directory, extracts execution times, and plots them."""
    files = sorted([f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))])
    operations = {"PutKey": [], "SwapKey": [], "GetKey": [], "DeleteKey": [], "ScanKey": []}
    file_labels = []

    for file in files:
        file_path = os.path.join(directory, file)
        avg_times = extract_avg_times(file_path)

        for op in operations:
            operations[op].append(avg_times.get(op, 0))  # Default to 0 if missing
        file_labels.append(file)

    # Plot the data
    plt.figure(figsize=(10, 6))
    for op, times in operations.items():
        plt.plot(file_labels, times, marker='o', label=op)

    plt.xlabel("Files")
    plt.ylabel("Execution Time (microseconds)")
    plt.title("Average Execution Times per File")
    plt.xticks(rotation=45, ha='right')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    # Save the plot
    output_file = os.path.join(directory, "execution_times_plot.png")
    plt.savefig(output_file)
    print(f"Graph saved as: {output_file}")
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python plot_exec_times.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    plot_execution_times(directory_path)

