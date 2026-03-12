# GPU Utilization Analysis

Fetches SLURM job accounting data and Prometheus GPU metrics for a GPU partition and summarizes average GPU duty cycle and peak memory utilization per user, weighted by job duration.

## Requirements

- Python 3 with `pandas`
- Access to `sacct` (must be run on a SLURM cluster)
- Access to the Prometheus API at `http://vcore001:9090` (requires `curl`)
- `data/` directory must exist

## Usage

```bash
python gpu_utilization.py [-s YYYY-MM-DD] [-e YYYY-MM-DD] [-p PARTITION]
```

All arguments are optional:

| Flag | Description | Default |
|------|-------------|---------|
| `-s`, `--start-time` | Start date for job query (inclusive) | 30 days before today |
| `-e`, `--end-time` | End date for job query (inclusive) | Today |
| `-p`, `--partition` | SLURM partition to analyze (must be a GPU partition) | `mit_normal_gpu` |

**Examples:**

```bash
# Last 30 days on the default partition
python gpu_utilization.py

# Custom date range
python gpu_utilization.py -s 2026-03-01 -e 2026-03-10
```

## What it does

1. Runs `sacct` to fetch all completed jobs on the given partition in the date range and saves the raw output to `data/sacct_<partition>_<start>_<end>.csv`.
2. Filters to jobs with `State == COMPLETED`.
3. For each job, queries Prometheus for three metrics over the job's exact start/end window:
   - `nvidia_gpu_duty_cycle` — GPU compute utilization (0–100%)
   - `nvidia_gpu_memory_used_bytes` — GPU memory used
   - `nvidia_gpu_memory_total_bytes` — GPU memory capacity
4. Computes per-job metrics:
   - **`avg_gpu_utilization`**: mean duty cycle averaged across all GPUs allocated to the job.
   - **`max_gpu_memory_utilization`**: peak memory utilization (used/total) across all GPUs allocated to the job.
   - **`num_gpus`**: number of GPUs allocated, derived from the number of Prometheus result series.
5. Computes total GPU-hours per job as `elapsed_seconds × num_gpus`.
6. Groups by user and computes elapsed-time-weighted averages of both utilization metrics, plus total GPU-hours consumed.
7. Prints the per-user summary table, followed by overall weighted averages across all jobs.

## Output

- `data/sacct_<partition>_<start>_<end>.csv` — raw pipe-delimited sacct output
- Per-user summary table and overall averages printed to stdout:

```
         User  avg_gpu_utilization  avg_max_gpu_memory_utilization  total_gpu_hours
     userA               0.612                           0.841              142.3
     userB               0.234                           0.455               18.7
     ...

Overall weighted avg GPU utilization: 54.13%
Overall weighted avg max GPU memory utilization: 72.06%
```
