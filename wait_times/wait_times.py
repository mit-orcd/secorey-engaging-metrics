import argparse
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

parser = argparse.ArgumentParser(description='Analyze SLURM job wait times.')
parser.add_argument('-s', '--start-time', required=True, help='Start date (YYYY-MM-DD)')
parser.add_argument('-e', '--end-time', required=True, help='End date (YYYY-MM-DD)')
args = parser.parse_args()

START = args.start_time
END = args.end_time
PARTITION = 'mit_normal_gpu'
OUTPUT_FILE = f'data/sacct_{START}_{END}.csv'

print(f"Fetching sacct data from {START} to {END}...")
subprocess.run(
    [
        'sacct', '-X', '-a', '--parsable',
        f'--partition={PARTITION}',
        f'--starttime={START}',
        f'--endtime={END}',
        '-o', 'JobID,User,Partition,Account,QOS,Reservation,NodeList,Submit,Eligible,Start,End,State,ExitCode,ReqTRES,Reason,Timelimit',
    ],
    stdout=open(OUTPUT_FILE, 'w'),
    check=True,
)
print(f"Data saved to {OUTPUT_FILE}")

# Read the pipe-delimited data file
df = pd.read_csv(OUTPUT_FILE, sep='|')

# Convert Submit column to datetime for filtering
df['Submit'] = pd.to_datetime(df['Submit'])

def parse_reqtres(reqtres_str):
    """Parse ReqTRES string into structured dictionary."""
    if pd.isna(reqtres_str):
        return {
            'req_billing': None,
            'req_cpu': None,
            'req_gres': None,
            'req_mem': None,
            'req_node': None
        }

    result = {
        'req_billing': None,
        'req_cpu': None,
        'req_gres': None,
        'req_mem': None,
        'req_node': None
    }

    # Split by comma
    parts = reqtres_str.split(',')

    for part in parts:
        if '=' not in part:
            continue
        key, value = part.split('=', 1)

        if key == 'billing':
            result['req_billing'] = int(value)
        elif key == 'cpu':
            result['req_cpu'] = int(value)
        elif key == 'mem':
            result['req_mem'] = value
        elif key == 'node':
            result['req_node'] = int(value)
        elif key.startswith('gres/gpu:'):
            # Extract GPU type from key (e.g., 'gres/gpu:l40s')
            gpu_type = key.split(':', 1)[1]
            result['req_gres'] = f"{gpu_type}:{value}"
        elif key == 'gres/gpu' and result['req_gres'] is None:
            # Fallback if no specific GPU type is mentioned
            result['req_gres'] = f"unknown:{value}"

    return result

def parse_mem_gb(mem_str):
    """Convert memory string (e.g. '64G', '512M', '1T') to float GB."""
    if pd.isna(mem_str) or mem_str is None:
        return None
    mem_str = str(mem_str).strip()
    if mem_str.endswith('T'):
        return float(mem_str[:-1]) * 1024
    elif mem_str.endswith('G'):
        return float(mem_str[:-1])
    elif mem_str.endswith('M'):
        return float(mem_str[:-1]) / 1024
    elif mem_str.endswith('K'):
        return float(mem_str[:-1]) / (1024 * 1024)
    else:
        try:
            return float(mem_str) / (1024 ** 3)  # assume bytes
        except ValueError:
            return None

def parse_timelimit_hours(tl_str):
    """Convert sacct Timelimit string (e.g. '6:00:00', '1-00:00:00') to float hours."""
    if pd.isna(tl_str) or tl_str in (None, 'Partition_Limit', 'UNLIMITED'):
        return None
    tl_str = str(tl_str).strip()
    days = 0
    if '-' in tl_str:
        day_part, tl_str = tl_str.split('-', 1)
        days = int(day_part)
    parts = tl_str.split(':')
    if len(parts) == 3:
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    elif len(parts) == 2:
        h, m, s = 0, int(parts[0]), int(parts[1])
    else:
        return None
    return days * 24 + h + m / 60 + s / 3600

# Apply the parsing functions
parsed_data = df['ReqTRES'].apply(parse_reqtres)
parsed_df = pd.DataFrame(parsed_data.tolist())
df = pd.concat([df.reset_index(drop=True), parsed_df], axis=1)

df['req_mem_gb'] = df['req_mem'].apply(parse_mem_gb)
df['req_timelimit_hours'] = df['Timelimit'].apply(parse_timelimit_hours)

start_date = pd.Timestamp(START)
end_date = pd.Timestamp(END)
df = df[(df['Submit'] >= start_date) & (df['Submit'] <= end_date)].copy()

# Filter for the three accounts of interest
accounts_of_interest = ['mit_general', 'mit_amf_standard_gpu', 'mit_amf_advanced_gpu']
df_filtered = df[df['Account'].isin(accounts_of_interest)].copy()

# Filter out jobs without valid Start times
df_filtered = df_filtered[
    (df_filtered['Start'].notna()) &
    (df_filtered['Start'] != 'None') &
    (df_filtered['Start'] != 'Unknown')
]

# Filter for only jobs with Reason == None
df_filtered = df_filtered[df_filtered["Reason"].isna()]

# Convert Start column to datetime
df_filtered['Start'] = pd.to_datetime(df_filtered['Start'])

# Calculate wait time in hours
df_filtered['WaitTime_hours'] = (df_filtered['Start'] - df_filtered['Submit']).dt.total_seconds() / 3600

# Extract the submit date (without time) for grouping
df_filtered['SubmitDate'] = df_filtered['Submit'].dt.date

# Define job size groupings
groupings = {
    'small_h200': (
        (df_filtered['req_gres'] == 'h200:1')
        # (df_filtered['req_timelimit_hours'] <= 6) &
        # (df_filtered['req_cpu'] <= 16) &
        # (df_filtered['req_mem_gb'] <= 64)
    ),
    'medium_h200': (
        (df_filtered['req_gres'] == 'h200:2')
        # (df_filtered['req_timelimit_hours'] <= 6) &
        # (df_filtered['req_cpu'] >= 17) & (df_filtered['req_cpu'] <= 32) &
        # (df_filtered['req_mem_gb'] >= 65) & (df_filtered['req_mem_gb'] <= 128)
    ),
    'large_h200': (
        (df_filtered['req_gres'] == 'h200:4')
        # (df_filtered['req_cpu'] >= 33) &
        # (df_filtered['req_mem_gb'] > 128)
    ),
}

colors = {'mit_general': '#1f77b4', 'mit_amf_standard_gpu': '#ff7f0e', 'mit_amf_advanced_gpu': '#2ca02c'}

# titles = {
#     'small_h200': 'Small H200 Jobs (1 GPU, ≤6h, ≤16 CPUs, ≤64GB)',
#     'medium_h200': 'Medium H200 Jobs (2 GPUs, ≤6h, 17-32 CPUs, 65-128GB)',
#     'large_h200': 'Large H200 Jobs (4 GPUs, 33+ CPUs, >128GB)',
# }
titles = {
    'small_h200': 'Small H200 Jobs (1 GPU)',
    'medium_h200': 'Medium H200 Jobs (2 GPUs)',
    'large_h200': 'Large H200 Jobs (4 GPUs)',
}

def make_plot(df_group, group_name, title):
    """Create and save a wait time plot for a job size group."""
    median_wt = df_group.groupby(['SubmitDate', 'Account'])['WaitTime_hours'].median().reset_index()
    median_wt.rename(columns={'WaitTime_hours': 'Median_WaitTime'}, inplace=True)

    mean_wt = df_group.groupby(['SubmitDate', 'Account'])['WaitTime_hours'].mean().reset_index()
    mean_wt.rename(columns={'WaitTime_hours': 'Mean_WaitTime'}, inplace=True)

    wait_times = pd.merge(median_wt, mean_wt, on=['SubmitDate', 'Account'])

    fig, ax = plt.subplots(figsize=(12, 7))

    for account in accounts_of_interest:
        account_data = wait_times[wait_times['Account'] == account]
        if not account_data.empty:
            ax.plot(account_data['SubmitDate'], account_data['Median_WaitTime'],
                    marker='o', label=f'{account} (median)', linewidth=2.5,
                    color=colors[account], linestyle='-')
            ax.plot(account_data['SubmitDate'], account_data['Mean_WaitTime'],
                    marker='s', label=f'{account} (mean)', linewidth=2,
                    color=colors[account], linestyle='--', alpha=0.7)

    ax.set_xlabel('Submit Date', fontsize=12)
    ax.set_ylabel('Wait Time (hours)', fontsize=12)
    ax.set_title(f'Job Wait Time by Account — {title}', fontsize=14, fontweight='bold')
    ax.legend(loc='best', fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_file = f'plots/wait_times_{group_name}.png'
    plt.savefig(out_file, dpi=300, bbox_inches='tight')
    print(f"Plot saved as '{out_file}'")

    print(f"\nSummary statistics ({group_name}):")
    for account in accounts_of_interest:
        account_data = df_group[df_group['Account'] == account]
        if not account_data.empty:
            print(f"\n  {account}:")
            print(f"    Total jobs: {len(account_data)}")
            print(f"    Average wait time: {account_data['WaitTime_hours'].mean():.2f} hours")
            print(f"    Median wait time: {account_data['WaitTime_hours'].median():.2f} hours")
            print(f"    Max wait time: {account_data['WaitTime_hours'].max():.2f} hours")

    plt.show()

for group_name, mask in groupings.items():
    df_group = df_filtered[mask].copy()
    make_plot(df_group, group_name, titles[group_name])
