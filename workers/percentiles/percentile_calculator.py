# Percentile Calculator - Market-level percentile calculations
import re
import os
import statistics
from datetime import datetime

def analyze_log_performance(log_date: str, task_name: str):
    """
    log_date: 'YYYY-MM-DD'
    task_name: e.g., 'tasks.calculate_i_score_task'
    """
    file_path = f"logs/celery_tasks.log.{log_date}"
    if log_date == datetime.now().strftime('%Y-%m-%d'):
        file_path = "logs/celery_tasks.log"

    if not os.path.exists(file_path):
        print(f"Error: Log file {file_path} not found.")
        return

    results = []

    # Regex to find: ✅ TASK COMPLETED: task_name | RESULT: 123.45
    pattern = rf"✅ TASK COMPLETED: {re.escape(task_name)} \| RESULT: ([\d.]+)"

    with open(file_path, 'r') as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                results.append(float(match.group(1)))

    if not results:
        print(f"No results found for {task_name} on {log_date}")
        return

    # Statistical calculations
    # P50 is the median
    # P95 and P99 represent the values below which 95% and 99% of data fall.
    p50 = statistics.quantiles(results, n=100)[49]
    p95 = statistics.quantiles(results, n=100)[94]
    p99 = statistics.quantiles(results, n=100)[98]

    print(f"--- Analysis for {task_name} ({log_date}) ---")
    print(f"Count:  {len(results)}")
    print(f"Mean:   {statistics.mean(results):.4f}")
    print(f"Median: {p50:.4f}")
    print(f"P95:    {p95:.4f}")
    print(f"P99:    {p99:.4f}")
