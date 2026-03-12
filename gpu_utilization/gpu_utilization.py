import argparse
import subprocess
from datetime import datetime, timedelta
import json
import pandas as pd

today = datetime.today().strftime('%Y-%m-%d')
one_month_ago = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
default_partition = "mit_normal_gpu"

parser = argparse.ArgumentParser(description='Analyze SLURM job wait times.')
parser.add_argument('-s', '--start-time', default=one_month_ago, help='Start date (YYYY-MM-DD), defaults to 30 days ago')
parser.add_argument('-e', '--end-time', default=today, help='End date (YYYY-MM-DD), defaults to today')
parser.add_argument('-p', '--partition', default=default_partition, help=f'Partition to run metrics on (must be a GPU partition), defaults to {default_partition}')
args = parser.parse_args()

START = args.start_time if args.start_time is not None else one_month_ago
END = args.end_time if args.end_time is not None else today
PARTITION = args.partition
API_ROUTE = "http://vcore001:9090/api/v1"
STEP = "60s" # Granularity for GPU metrics

def get_job_metrics(api_route, query, job_id, start, end, step):
    output = subprocess.run(
        [
            "curl",
            "-G", f"{api_route}/query_range",
            "--data-urlencode", f'query={query}{{jobid="{job_id}"}}',
            "--data-urlencode", f"start={start}Z",
            "--data-urlencode", f"end={end}Z",
            "--data-urlencode", f"step={step}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(output.stdout)

# Get job info:
sacct_fields = "JobID,User,Partition,ReqTRES,State,ExitCode,Start,End,Elapsed"
result = subprocess.run(
    [
        "sacct", "-X", "-a", "--parsable",
        "-r", PARTITION,
        "-S", START,
        "-E", END,
        "-o", sacct_fields
    ],
    capture_output=True,
    text=True,
    check=True,
)
sacct_path = f"data/sacct_{PARTITION}_{START}_{END}.csv"
with open(sacct_path, "w") as f:
    f.write(result.stdout)

sacct_df = pd.read_csv(sacct_path, delimiter="|")

# Filter sacct df:
sacct_df.drop(columns=["Unnamed: 9"], inplace=True)
sacct_df = sacct_df[sacct_df['State'] == "COMPLETED"]

# Compute per-job GPU metrics using per-job Start/End for efficiency
records = []
for _, row in sacct_df.iterrows():
    job_id = row["JobID"]
    job_start = row["Start"]
    job_end = row["End"]

    avg_util = None
    max_mem_util = None
    num_gpus = None

    try:
        util_data = get_job_metrics(API_ROUTE, "nvidia_gpu_duty_cycle", job_id, job_start, job_end, STEP)
        gpu_results = util_data["data"]["result"]
        num_gpus = len(gpu_results)
        per_gpu_avgs = [
            sum(int(v) for _, v in gpu["values"]) / len(gpu["values"])
            for gpu in gpu_results
        ]
        avg_util = sum(per_gpu_avgs) / (num_gpus * 100)
    except (IndexError, KeyError, ZeroDivisionError):
        pass

    try:
        mem_data = get_job_metrics(API_ROUTE, "nvidia_gpu_memory_used_bytes", job_id, job_start, job_end, STEP)
        total_data = get_job_metrics(API_ROUTE, "nvidia_gpu_memory_total_bytes", job_id, job_start, job_end, STEP)
        max_mem_util = max(
            max(int(v) for _, v in mem_data["data"]["result"][i]["values"])
            / int(total_data["data"]["result"][i]["values"][0][1])
            for i in range(len(mem_data["data"]["result"]))
        )
    except (IndexError, KeyError, ZeroDivisionError, ValueError):
        pass

    records.append({
        "JobID": job_id,
        "avg_gpu_utilization": avg_util,
        "max_gpu_memory_utilization": max_mem_util,
        "num_gpus": num_gpus,
    })

def elapsed_to_seconds(elapsed):
    # sacct Elapsed format: [D-]HH:MM:SS
    if "-" in elapsed:
        days, hms = elapsed.split("-")
        days = int(days)
    else:
        days, hms = 0, elapsed
    h, m, s = hms.split(":")
    return days * 86400 + int(h) * 3600 + int(m) * 60 + int(s)

metrics_df = pd.DataFrame(records)
df = sacct_df.merge(metrics_df, on="JobID", how="left")

df["elapsed_seconds"] = df["Elapsed"].apply(elapsed_to_seconds)
df["gpu_seconds"] = df["elapsed_seconds"] * df["num_gpus"]

# Weighted averages per user, weighted by elapsed_seconds
def weighted_avg(group, col):
    mask = group[col].notna()
    if not mask.any():
        return None
    return (group.loc[mask, col] * group.loc[mask, "elapsed_seconds"]).sum() / group.loc[mask, "elapsed_seconds"].sum()

summary = df.groupby("User").apply(
    lambda g: pd.Series({
        "avg_gpu_utilization": weighted_avg(g, "avg_gpu_utilization"),
        "avg_max_gpu_memory_utilization": weighted_avg(g, "max_gpu_memory_utilization"),
        "total_gpu_hours": g["gpu_seconds"].sum() / 3600,
    })
, include_groups=False).reset_index()

print(summary.to_string(index=False))

overall_gpu_util = weighted_avg(df, "avg_gpu_utilization")
overall_mem_util = weighted_avg(df, "max_gpu_memory_utilization")
print(f"\nOverall weighted avg GPU utilization: {overall_gpu_util:.2%}")
print(f"Overall weighted avg max GPU memory utilization: {overall_mem_util:.2%}")
