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
- Summary statistics printed to stdout (format depends on arguments — see above):

Example output:
```
Summary Statistics

                                   Past Day                              Past Week                             Past Month                            
                                 Total Jobs Mean (h) Median (h) Max (h) Total Jobs Mean (h) Median (h) Max (h) Total Jobs Mean (h) Median (h) Max (h)
Group       Account                                                                                                                                  
small_h200  mit_general                 127     0.07       0.05    0.81        520     0.43       0.05   22.53       3801     0.59       0.01   60.01
            mit_amf_standard_gpu          8     0.02       0.01    0.04         20     0.40       0.03    5.43         72     0.36       0.02    8.33
            mit_amf_advanced_gpu         48     0.03       0.03    0.06        238     0.22       0.05    2.64        995     0.12       0.01   11.61
medium_h200 mit_general                   9     0.31       0.14    1.60         36     1.79       0.15   23.89        696     0.55       0.01   81.98
            mit_amf_standard_gpu          0      NaN        NaN     NaN          3     0.51       0.47    1.01         43     0.20       0.01    4.17
            mit_amf_advanced_gpu         10     0.09       0.02    0.69         94     0.34       0.10    2.68        282     0.25       0.03   12.71
large_h200  mit_general                   0      NaN        NaN     NaN          0      NaN        NaN     NaN          0      NaN        NaN     NaN
            mit_amf_standard_gpu          0      NaN        NaN     NaN          0      NaN        NaN     NaN          0      NaN        NaN     NaN
            mit_amf_advanced_gpu          4     1.22       0.89    2.86         48     1.13       0.25    6.78        163     1.14       0.03   24.95
```
