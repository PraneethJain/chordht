import subprocess
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import io

def run_benchmarks():
    print("Running benchmarks... this may take a minute.")
    result = subprocess.run(
        ["cargo", "test", "--test", "benchmarks", "--", "--nocapture", "--test-threads=1"],
        cwd="../",
        capture_output=True,
        text=True
    )
    return result.stdout

def parse_scalability(output):
    print("Parsing Scalability Benchmark...")
    match = re.search(r"=== Benchmark 1: Scalability .*?===\n(Nodes,Avg_Hops\n[\s\S]*?)(?=\n===|$)", output)
    if match:
        data_str = match.group(1)
        lines = [line for line in data_str.split('\n') if line.strip() and (line.startswith('Nodes') or line[0].isdigit())]
        df = pd.read_csv(io.StringIO('\n'.join(lines)))
        return df
    return None

def parse_load_balancing(output):
    print("Parsing Load Balancing Benchmark...")
    match = re.search(r"=== Benchmark 2: Load Balancing .*?===\n[\s\S]*?(Node_ID,Key_Count\n[\s\S]*?)(?=\n===|$)", output)
    if match:
        data_str = match.group(1)
        lines = [line for line in data_str.split('\n') if line.strip() and (line.startswith('Node_ID') or line[0].isdigit())]
        df = pd.read_csv(io.StringIO('\n'.join(lines)))
        df['Node_ID'] = pd.to_numeric(df['Node_ID'], errors='coerce')
        df['Key_Count'] = pd.to_numeric(df['Key_Count'], errors='coerce')
        df = df.dropna()
        df = df.sort_values('Node_ID')
        df['Node_ID'] = df['Node_ID'].astype(str).apply(lambda x: x[:8] + '...')
        return df
    return None

def parse_replication_delay(output):
    print("Parsing Replication Delay Benchmark...")
    match = re.search(r"=== Benchmark 4: Replication Delay .*?===\n[\s\S]*?(Trial,Delay_ms\n[\s\S]*?)(?=\n===|$)", output)
    if match:
        data_str = match.group(1)
        lines = [line for line in data_str.split('\n') if line.strip() and (line.startswith('Trial') or line[0].isdigit())]
        df = pd.read_csv(io.StringIO('\n'.join(lines)))
        return df
    return None

def parse_latency_cdf(output):
    print("Parsing Latency CDF Benchmark...")
    match = re.search(r"=== Benchmark 5: Latency CDF .*?===\n[\s\S]*?(Latency_us\n[\s\S]*?)(?=\n===|$)", output)
    if match:
        data_str = match.group(1)
        lines = [line for line in data_str.split('\n') if line.strip() and (line.startswith('Latency_us') or line[0].isdigit())]
        df = pd.read_csv(io.StringIO('\n'.join(lines)))
        df['Latency_us'] = pd.to_numeric(df['Latency_us'], errors='coerce')
        df = df.dropna()
        return df
    return None

def plot_scalability(df, output_dir):
    if df is None or df.empty:
        print("No data for Scalability")
        return
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='Nodes', y='Avg_Hops', marker='o')
    plt.title('Scalability: Average Hops vs Network Size')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Average Hops')
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, 'scalability.png'))
    plt.close()

def plot_load_balancing(df, output_dir):
    if df is None or df.empty:
        print("No data for Load Balancing")
        return

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x='Node_ID', y='Key_Count', color='skyblue')
    plt.title('Load Balancing: Key Distribution Across Nodes')
    plt.xlabel('Node ID')
    plt.ylabel('Number of Keys')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'load_balancing.png'))
    plt.close()

def plot_replication_delay(df, output_dir):
    if df is None or df.empty:
        print("No data for Replication Delay")
        return

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='Trial', y='Delay_ms', marker='o')
    plt.title('Replication Delay per Trial')
    plt.xlabel('Trial Number')
    plt.ylabel('Delay (milliseconds)')
    plt.ylim(bottom=0)
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, 'replication_delay.png'))
    plt.close()

def plot_latency_cdf(df, output_dir):
    if df is None or df.empty:
        print("No data for Latency CDF")
        return

    plt.figure(figsize=(10, 6))
    sns.ecdfplot(data=df, x='Latency_us')
    plt.title('Latency Cumulative Distribution Function (CDF)')
    plt.xlabel('Latency (microseconds)')
    plt.ylabel('Cumulative Probability (Proportion of Requests <= x)')
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, 'latency_cdf.png'))
    plt.close()

def main():
    output_dir = "../benchmark_results"
    os.makedirs(output_dir, exist_ok=True)
    
    output = run_benchmarks()
    with open(os.path.join(output_dir, "raw_output.txt"), "w") as f:
        f.write(output)

    df_scalability = parse_scalability(output)
    plot_scalability(df_scalability, output_dir)

    df_load = parse_load_balancing(output)
    plot_load_balancing(df_load, output_dir)

    df_rep = parse_replication_delay(output)
    plot_replication_delay(df_rep, output_dir)

    df_lat = parse_latency_cdf(output)
    plot_latency_cdf(df_lat, output_dir)
    
    print(f"Plots saved to {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    main()
