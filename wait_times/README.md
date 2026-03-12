# Wait Times Analysis

Fetches SLURM job accounting data for the `mit_normal_gpu` partition and plots median/mean wait times over time, broken down by account and H200 GPU job size.

## Requirements

- Python 3 with `pandas` and `matplotlib`
- Access to `sacct` (must be run on a SLURM cluster)
- `data/` and `plots/` directories must exist

## Usage

```bash
python wait_times.py [-s YYYY-MM-DD] [-e YYYY-MM-DD]
```

Both arguments are optional:

| Flag | Description | Default |
|------|-------------|---------|
| `-s`, `--start-time` | Start date for job query (inclusive) | 30 days before today |
| `-e`, `--end-time` | End date for job query (inclusive) | Today |

**Examples:**

```bash
# Last 30 days (default — also prints summary statistics table)
python wait_times.py

# Custom range
python wait_times.py -s 2026-03-01 -e 2026-03-10

# From a specific start date through today
python wait_times.py -s 2026-02-01
```

## What it does

1. Runs `sacct` to fetch all jobs on the `mit_normal_gpu` partition in the given date range and saves the raw output to `data/sacct_<start>_<end>.csv`.
2. Filters to jobs from three accounts: `mit_general`, `mit_amf_standard_gpu`, `mit_amf_advanced_gpu`.
3. Excludes jobs that never started (missing/unknown `Start` time) and jobs still pending for a reason other than normal scheduling.
4. Calculates wait time as `Start - Submit` in hours.
5. Groups jobs by H200 GPU count:
   - **small_h200**: 1 H200 GPU
   - **medium_h200**: 2 H200 GPUs
   - **large_h200**: 4 H200 GPUs
6. For each group, generates a line plot of daily median and mean wait time per account, saved to `plots/wait_times_<group>.png`.
7. **When run with no arguments**, prints a summary statistics table instead of per-group stats. The table shows total jobs, mean, median, and max wait time for each (group, account) combination across three time windows: past day, past week, and past month.

## Output

- `data/sacct_<start>_<end>.csv` — raw pipe-delimited sacct output
- `plots/wait_times_small_h200.png`
- `plots/wait_times_medium_h200.png`
- `plots/wait_times_large_h200.png`
- Summary statistics printed to stdout (format depends on arguments — see above)
