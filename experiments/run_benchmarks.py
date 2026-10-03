"""
Benchmark the project's five scheduling algorithms under identical synthetic workloads.

Run from the repository root:
    python -m experiments.run_benchmarks

Outputs:
    benchmark_results/raw_results.csv
    benchmark_results/summary_results.csv
    benchmark_results/carbon_reduction.csv
    benchmark_results/figures/*.png

This is a discrete-event simulation: task execution is simulated using each selected
worker's processing speed, power, and carbon intensity. Failures are injected
deterministically per task/attempt so that algorithms see the same failure outcomes
for the same seed. Failed attempts still consume simulated energy and carbon.
"""

from __future__ import annotations

import heapq
import itertools
import random
from pathlib import Path
from statistics import mean

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.schedulers.round_robin import RoundRobinScheduler
from src.schedulers.least_loaded import LeastLoadedScheduler
from src.schedulers.performance_only import PerformanceOnlyScheduler
from src.schedulers.cedar_inspired import CedarInspiredScheduler
from src.schedulers.failure_aware_carbon import FailureAwareCarbonScheduler
from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.workflow.task import Task
from src.workflow.workflow import Workflow


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "benchmark_results"
FIGURE_DIR = OUTPUT_DIR / "figures"

SCHEDULER_NAMES = [
    "Round Robin",
    "Least Loaded",
    "Performance Only",
    "CEDAR-inspired",
    "Proposed Failure-Aware Carbon",
]

FAILURE_LEVELS = [0.0, 0.1, 0.3, 0.5]
WORKER_FAILURE_LEVELS = [0.0, 0.1, 0.3, 0.5]
CAPACITY_LEVELS = [1, 2, 3]
CARBON_SCALES = {
    "Low": 0.5,
    "Medium": 1.0,
    "High": 1.5,
}
REPETITIONS = 3
MAX_RETRIES = 3
WORKER_RECOVERY_TIME = 5.0

# Heterogeneous workers: different speed, power, and carbon intensity.
WORKER_BASES = [
    {"speed": 1.00, "power": 120.0, "carbon": 120.0},
    {"speed": 1.40, "power": 180.0, "carbon": 300.0},
    {"speed": 0.80, "power": 240.0, "carbon": 550.0},
    {"speed": 1.20, "power": 160.0, "carbon": 220.0},
]

# Multipliers make worker risk heterogeneous so the proposed scheduler has a
# meaningful worker failure-risk signal to evaluate.
WORKER_FAILURE_MULTIPLIERS = [0.5, 1.0, 1.5, 0.75]

# Fixed DAG: task_id, runtime seconds, dependency IDs.
WORKLOAD = [
    ("T1", 5.0, []),
    ("T2", 8.0, []),
    ("T3", 6.0, []),
    ("T4", 7.0, ["T1"]),
    ("T5", 4.0, ["T1"]),
    ("T6", 9.0, ["T2"]),
    ("T7", 5.0, ["T2", "T3"]),
    ("T8", 8.0, ["T4"]),
    ("T9", 6.0, ["T5", "T6"]),
    ("T10", 7.0, ["T7"]),
    ("T11", 4.0, ["T8", "T9"]),
    ("T12", 5.0, ["T10", "T11"]),
]


def build_workflow(failure_probability: float) -> Workflow:
    workflow = Workflow(workflow_id="benchmark-workflow")
    for task_id, runtime, dependencies in WORKLOAD:
        workflow.add_task(
            Task(
                task_id=task_id,
                name=task_id,
                dependencies=list(dependencies),
                runtime=runtime,
                failure_probability=failure_probability,
                max_retries=MAX_RETRIES,
            )
        )
    return workflow


def build_worker_pool(
    capacity: int,
    carbon_scale: float,
    failure_probability: float,
) -> WorkerPool:
    pool = WorkerPool()
    for index, config in enumerate(WORKER_BASES):
        worker = Worker(
            worker_id=f"worker-{index + 1}",
            capacity=capacity,
            processing_speed=config["speed"],
            power_watts=config["power"],
            carbon_intensity_g_per_kwh=config["carbon"] * carbon_scale,
            failure_probability=min(
                0.99,
                failure_probability * WORKER_FAILURE_MULTIPLIERS[index],
            ),
        )
        pool.add_worker(worker)
    return pool


def build_scheduler(name: str, pool: WorkerPool):
    if name == "Round Robin":
        return RoundRobinScheduler(pool)
    if name == "Least Loaded":
        return LeastLoadedScheduler(pool)
    if name == "Performance Only":
        return PerformanceOnlyScheduler(pool)
    if name == "CEDAR-inspired":
        return CedarInspiredScheduler(pool)
    if name == "Proposed Failure-Aware Carbon":
        return FailureAwareCarbonScheduler(
            pool,
            time_weight=1.0,
            carbon_weight=1.0,
            load_weight=1.0,
            failure_weight=1.0,
        )
    raise ValueError(f"Unknown scheduler: {name}")


def stable_failure_draw(seed: int, task_id: str, attempt_number: int) -> float:
    """Same task/attempt gets the same random draw across algorithms."""
    stable_seed = f"{seed}:{task_id}:{attempt_number}"
    return random.Random(stable_seed).random()
def stable_worker_failure_event(
    seed: int,
    worker_id: str,
    failure_index: int,
) -> float:
    """Generate a scheduler-independent worker failure draw."""
    stable_seed = (
        f"worker-event:{seed}:{worker_id}:{failure_index}"
    )
    return random.Random(stable_seed).random()
def build_worker_failure_schedule(
    seed,
    worker_id,
    worker_failure_probability,
    simulation_horizon,
):
    """Generate deterministic worker failure times independent of task assignment."""

    failure_times = []

    failure_index = 0
    current_time = 0.0

    while current_time < simulation_horizon:
        failure_draw = stable_worker_failure_event(
            seed,
            worker_id,
            failure_index,
        )

        failure_index += 1

        if failure_draw < worker_failure_probability:
            failure_time_fraction = stable_worker_failure_time_fraction(
                seed,
                worker_id,
                failure_index,
            )

            failure_time = current_time + (
                failure_time_fraction * WORKER_RECOVERY_TIME
            )

            if failure_time < simulation_horizon:
                failure_times.append(failure_time)

            current_time = failure_time + WORKER_RECOVERY_TIME

        else:
            current_time += WORKER_RECOVERY_TIME

    return failure_times
def stable_worker_failure_time_fraction(
    seed: int,
    worker_id: str,
    failure_index: int,
) -> float:
    """Generate a deterministic point within a worker failure interval."""

    stable_seed = (
        f"worker-failure-time:{seed}:{worker_id}:{failure_index}"
    )

    return random.Random(stable_seed).uniform(0.2, 0.8)

def run_one_experiment(
    scheduler_name: str,
    failure_probability: float,
    worker_failure_probability: float,
    capacity: int,
    carbon_label: str,
    carbon_scale: float,
    seed: int,
) -> dict:
    workflow = build_workflow(failure_probability)
    pool = build_worker_pool(capacity, carbon_scale, failure_probability)
    scheduler = build_scheduler(scheduler_name, pool)

    # Events:
# (
#     event_time,
#     tie_breaker,
#     event_type,
#     task,
#     worker,
#     failed,
#     duration,
#     start_time,
#     attempt_number,
# )
    events = []
    event_counter = itertools.count()
    total_workload_runtime = sum(
        task.runtime
        for task in workflow.tasks.values()
    )

    max_task_attempts = MAX_RETRIES + 1

    simulation_horizon = (
        total_workload_runtime * max_task_attempts
        + WORKER_RECOVERY_TIME * len(WORKLOAD)
    )
    worker_failure_schedule = {}


    for worker in pool.workers.values():
        worker_failure_schedule[worker.worker_id] = build_worker_failure_schedule(
            seed,
            worker.worker_id,
            worker.failure_probability,
            simulation_horizon,
        )

    for worker in pool.workers.values():
        failure_times = worker_failure_schedule[worker.worker_id]

        for failure_time in failure_times:
            heapq.heappush(
                events,
                (
                    failure_time,
                    next(event_counter),
                    "WORKER_FAILURE",
                    None,
                    worker,
                    False,
                    0.0,
                    failure_time,
                    -1,
                ),
            )
    virtual_time = 0.0
    total_energy_kwh = 0.0
    total_carbon_g = 0.0
    total_attempts = 0
    failed_attempts = 0
    completed_tasks = 0
    permanently_failed_tasks = 0
    recovery_durations = []
    first_failure_time = {}
    assignment_count = 0
    worker_failures = 0
    worker_interrupted_tasks = 0
    worker_failure_retries = 0
    worker_recoveries = 0
    interrupted_attempts = set()
    active_attempts = {}


    workflow.refresh_ready_tasks()

    while True:
        # Dispatch ready tasks while capacity is available.
        dispatched_any = False
        ready_tasks = workflow.get_ready_tasks()

        for task in ready_tasks:
            available_workers = pool.get_available_workers()

            if not available_workers:
                break

            worker = scheduler.schedule(task)

            if worker is None:
                raise RuntimeError(
                    f"Scheduler returned None for task {task.task_id}. "
                    f"Available workers={[w.worker_id for w in available_workers]}"
                )

            task.transition_to("RUNNING")

            task.attempts += 1
            task.attempts_in_cycle += 1
            total_attempts += 1
            assignment_count += 1

            duration = task.runtime / worker.processing_speed
            active_attempts[
                    (task.task_id, task.attempts_in_cycle)
                ] = {
                    "worker_id": worker.worker_id,
                    "start_time": virtual_time,
                    "duration": duration,
                }

            failed = (
                stable_failure_draw(
                    seed,
                    task.task_id,
                    task.attempts_in_cycle,
                )
                < task.failure_probability
            )


            finish_time = virtual_time + duration
            finish_event_id = next(event_counter)

            heapq.heappush(
                events,
                (
                    finish_time,
                    finish_event_id,
                    "TASK_FINISH",
                    task,
                    worker,
                    failed,
                    duration,
                    virtual_time,
                    task.attempts_in_cycle,
                ),
            )

            dispatched_any = True

        if not events:
            ready_tasks = workflow.get_ready_tasks()
            available_workers = pool.get_available_workers()

            if not ready_tasks:
                break

            if not available_workers:
                failed_workers = []

                for current_worker in pool.workers.values():
                    if current_worker.is_failed:
                        failed_workers.append(current_worker.worker_id)

                raise RuntimeError(
                    f"Deadlock: ready tasks exist but no worker capacity is available. "
                    f"ready_tasks={[task.task_id for task in ready_tasks]}, "
                    f"failed_workers={failed_workers}, "
                    f"virtual_time={virtual_time}"
                )

            if not dispatched_any:
                raise RuntimeError(
                    f"Deadlock: ready tasks exist but scheduler did not dispatch a task. "
                    f"ready_tasks={[task.task_id for task in ready_tasks]}"
                )

        if events:
            (
                event_time,
                _,
                event_type,
                task,
                worker,
                failed,
                duration,
                start_time,
                event_attempt,
            ) = heapq.heappop(events)

            virtual_time = event_time
            if event_type == "WORKER_FAILURE":
                worker.fail()
                worker_failures += 1

                recovery_time = event_time + WORKER_RECOVERY_TIME

                heapq.heappush(
                    events,
                    (
                        recovery_time,
                        next(event_counter),
                        "WORKER_RECOVERY",
                        None,
                        worker,
                        False,
                        0.0,
                        event_time,
                        -1,
                    ),
                )

                running_task_ids = list(worker.running_tasks)

                if not running_task_ids:
                    continue

                for task_id in running_task_ids:
                    task = workflow.tasks[task_id]

                    active_key = None

                    for key, attempt_info in active_attempts.items():
                        if (
                            attempt_info["worker_id"] == worker.worker_id
                            and key[0] == task.task_id
                        ):
                            active_key = key
                            break

                    if active_key is None:
                        worker.running_tasks.remove(task.task_id)
                        continue

                    attempt_info = active_attempts[active_key]

                    elapsed_time = event_time - attempt_info["start_time"]

                    energy_kwh = (
                        worker.power_watts
                        * elapsed_time
                        / 3_600_000
                    )

                    carbon_g = (
                        energy_kwh
                        * worker.carbon_intensity_g_per_kwh
                    )

                    total_energy_kwh += energy_kwh
                    total_carbon_g += carbon_g

                    del active_attempts[active_key]

                    pool.release_task(
                        worker.worker_id,
                        task.task_id,
                    )

                    worker_interrupted_tasks += 1

                    interrupted_attempts.add(active_key)

                    task.transition_to("FAILED")

                    if task.attempts_in_cycle <= task.max_retries:
                        worker_failure_retries += 1
                        task.transition_to("READY")
                    else:
                        permanently_failed_tasks += 1
                        workflow.block_downstream_tasks(task.task_id)

                workflow.refresh_ready_tasks()

                continue
            if event_type == "WORKER_RECOVERY":
                worker.recover()
                worker_recoveries += 1
                continue
            if event_type == "TASK_FINISH":
                event_key = (
                    task.task_id,
                    event_attempt,
                )

                if event_key in interrupted_attempts:
                    interrupted_attempts.remove(event_key)
                    continue
                if event_key not in active_attempts:
                    continue
                if task.status!="RUNNING":
                    continue

            energy_kwh = worker.power_watts * duration / 3_600_000
            carbon_g = energy_kwh * worker.carbon_intensity_g_per_kwh
            total_energy_kwh += energy_kwh
            total_carbon_g += carbon_g
            active_attempts.pop(
                    (task.task_id, event_attempt),
                    None,
                )
            pool.release_task(worker.worker_id, task.task_id)

            if failed:
                failed_attempts += 1
                task.transition_to("FAILED")
                if task.task_id not in first_failure_time:
                    first_failure_time[task.task_id] = virtual_time

                if task.attempts_in_cycle <= task.max_retries:
                    task.transition_to("READY")
                else:
                    permanently_failed_tasks += 1
                    workflow.block_downstream_tasks(task.task_id)
            else:
                task.result = f"result-{task.task_id}"
                task.transition_to("COMPLETED")
                completed_tasks += 1
                if task.task_id in first_failure_time:
                    recovery_durations.append(
                        virtual_time - first_failure_time[task.task_id]
                    )

            workflow.refresh_ready_tasks()
    
    statuses = [task.status for task in workflow.tasks.values()]
    workflow_completed = all(status == "COMPLETED" for status in statuses)
    workflow_failed = any(status in ("FAILED", "BLOCKED") for status in statuses)

    failure_rate = 0.0
    if total_attempts > 0:
        failure_rate = failed_attempts / total_attempts * 100.0

    recovery_time = mean(recovery_durations) if recovery_durations else 0.0

    return {
        "scheduler": scheduler_name,
        "failure_probability": failure_probability,
        "capacity_per_worker": capacity,
        "carbon_condition": carbon_label,
        "carbon_scale": carbon_scale,
        "seed": seed,
        "execution_time_seconds": virtual_time,
        "energy_kwh": total_energy_kwh,
        "carbon_emissions_g": total_carbon_g,
        "carbon_reduction_percent": None,
        "total_attempts": total_attempts,
        "failed_attempts": failed_attempts,
        "failure_rate_percent": failure_rate,
        "completed_tasks": completed_tasks,
        "permanently_failed_tasks": permanently_failed_tasks,
        "recovery_time_seconds": recovery_time,
        "recovered_task_count": len(recovery_durations),
        "workflow_status": "COMPLETED" if workflow_completed else "FAILED_OR_BLOCKED",
        "assignments": assignment_count,
        "worker_failures": worker_failures,
        "worker_interrupted_tasks": worker_interrupted_tasks,
        "worker_failure_retries": worker_failure_retries,
        "worker_recoveries": worker_recoveries,
    }


def make_reduction_table(summary: pd.DataFrame) -> pd.DataFrame:
    key_columns = [
        "failure_probability",
        "capacity_per_worker",
        "carbon_condition",
    ]
    baseline_names = [name for name in SCHEDULER_NAMES if name != "Proposed Failure-Aware Carbon"]

    proposed = summary[
        summary["scheduler"] == "Proposed Failure-Aware Carbon"
    ][key_columns + ["carbon_emissions_g_mean"]].rename(
        columns={"carbon_emissions_g_mean": "proposed_carbon_g"}
    )

    rows = []
    for baseline_name in baseline_names:
        baseline = summary[summary["scheduler"] == baseline_name][
            key_columns + ["carbon_emissions_g_mean"]
        ].rename(columns={"carbon_emissions_g_mean": "baseline_carbon_g"})

        joined = baseline.merge(proposed, on=key_columns, how="inner")
        joined["baseline_scheduler"] = baseline_name
        joined["carbon_reduction_percent"] = (
            (joined["baseline_carbon_g"] - joined["proposed_carbon_g"])
            / joined["baseline_carbon_g"].replace(0, pd.NA)
            * 100.0
        )
        rows.append(joined)

    result = pd.concat(rows, ignore_index=True)
    return result[
        key_columns
        + [
            "baseline_scheduler",
            "baseline_carbon_g",
            "proposed_carbon_g",
            "carbon_reduction_percent",
        ]
    ]


def create_figures(raw: pd.DataFrame, summary: pd.DataFrame, reduction: pd.DataFrame) -> None:
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Overall emissions comparison.
    overall = raw.groupby("scheduler", as_index=False)["carbon_emissions_g"].mean()
    overall = overall.set_index("scheduler").reindex(SCHEDULER_NAMES).reset_index()
    plt.figure(figsize=(11, 6))
    plt.bar(overall["scheduler"], overall["carbon_emissions_g"])
    plt.title("Mean Carbon Emissions by Scheduler")
    plt.ylabel("Carbon emissions (g CO₂)")
    plt.xlabel("Scheduler")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "01_carbon_emissions_by_scheduler.png", dpi=180)
    plt.close()

    # 2. Mean carbon reduction vs each baseline.
    mean_reduction = reduction.groupby("baseline_scheduler", as_index=False)[
        "carbon_reduction_percent"
    ].mean()
    plt.figure(figsize=(10, 6))
    plt.bar(mean_reduction["baseline_scheduler"], mean_reduction["carbon_reduction_percent"])
    plt.axhline(0, linewidth=1)
    plt.title("Proposed Scheduler: Mean Carbon Reduction vs Baselines")
    plt.ylabel("Carbon reduction (%)")
    plt.xlabel("Baseline scheduler")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "02_carbon_reduction_vs_baselines.png", dpi=180)
    plt.close()

    # 3. Carbon emissions across failure probabilities.
    failure_summary = raw.groupby(
        ["scheduler", "failure_probability"], as_index=False
    )["carbon_emissions_g"].mean()
    plt.figure(figsize=(10, 6))
    for name in SCHEDULER_NAMES:
        subset = failure_summary[failure_summary["scheduler"] == name]
        plt.plot(
            subset["failure_probability"],
            subset["carbon_emissions_g"],
            marker="o",
            label=name,
        )
    plt.title("Carbon Emissions vs Task Failure Probability")
    plt.xlabel("Task failure probability")
    plt.ylabel("Mean carbon emissions (g CO₂)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "03_carbon_by_failure_probability.png", dpi=180)
    plt.close()

    # 4. Carbon emissions across capacity settings.
    capacity_summary = raw.groupby(
        ["scheduler", "capacity_per_worker"], as_index=False
    )["carbon_emissions_g"].mean()
    plt.figure(figsize=(10, 6))
    for name in SCHEDULER_NAMES:
        subset = capacity_summary[capacity_summary["scheduler"] == name]
        plt.plot(
            subset["capacity_per_worker"],
            subset["carbon_emissions_g"],
            marker="o",
            label=name,
        )
    plt.title("Carbon Emissions vs Worker Capacity")
    plt.xlabel("Capacity per worker")
    plt.ylabel("Mean carbon emissions (g CO₂)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "04_carbon_by_worker_capacity.png", dpi=180)
    plt.close()

    # 5. Carbon emissions across carbon-intensity conditions.
    carbon_order = ["Low", "Medium", "High"]
    intensity_summary = raw.groupby(
        ["scheduler", "carbon_condition"], as_index=False
    )["carbon_emissions_g"].mean()
    plt.figure(figsize=(10, 6))
    for name in SCHEDULER_NAMES:
        subset = intensity_summary[intensity_summary["scheduler"] == name].copy()
        subset["carbon_condition"] = pd.Categorical(
            subset["carbon_condition"], categories=carbon_order, ordered=True
        )
        subset = subset.sort_values("carbon_condition")
        plt.plot(
            subset["carbon_condition"].astype(str),
            subset["carbon_emissions_g"],
            marker="o",
            label=name,
        )
    plt.title("Carbon Emissions vs Carbon-Intensity Condition")
    plt.xlabel("Carbon-intensity condition")
    plt.ylabel("Mean carbon emissions (g CO₂)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "05_carbon_by_intensity_condition.png", dpi=180)
    plt.close()

    # 6–9. Additional metric comparisons.
    metric_specs = [
        ("execution_time_seconds", "Execution time (simulated seconds)", "06_execution_time.png"),
        ("energy_kwh", "Energy consumption (kWh)", "07_energy_consumption.png"),
        ("failure_rate_percent", "Failed attempts (%)", "08_failure_rate.png"),
        ("recovery_time_seconds", "Mean recovery time (simulated seconds)", "09_recovery_time.png"),
    ]
    for metric, ylabel, filename in metric_specs:
        metric_summary = raw.groupby("scheduler", as_index=False)[metric].mean()
        metric_summary = metric_summary.set_index("scheduler").reindex(SCHEDULER_NAMES).reset_index()
        plt.figure(figsize=(11, 6))
        plt.bar(metric_summary["scheduler"], metric_summary[metric])
        plt.title(f"Mean {ylabel} by Scheduler")
        plt.ylabel(ylabel)
        plt.xlabel("Scheduler")
        plt.xticks(rotation=20, ha="right")
        plt.tight_layout()
        plt.savefig(FIGURE_DIR / filename, dpi=180)
        plt.close()

    # 10. Trade-off scatter: mean time vs mean carbon.
    tradeoff = raw.groupby("scheduler", as_index=False).agg(
        execution_time_seconds=("execution_time_seconds", "mean"),
        carbon_emissions_g=("carbon_emissions_g", "mean"),
    )
    plt.figure(figsize=(9, 6))
    plt.scatter(tradeoff["execution_time_seconds"], tradeoff["carbon_emissions_g"])
    for _, row in tradeoff.iterrows():
        plt.annotate(
            row["scheduler"],
            (row["execution_time_seconds"], row["carbon_emissions_g"]),
            xytext=(5, 5),
            textcoords="offset points",
        )
    plt.title("Execution Time vs Carbon Emissions")
    plt.xlabel("Mean execution time (simulated seconds)")
    plt.ylabel("Mean carbon emissions (g CO₂)")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "10_time_vs_carbon.png", dpi=180)
    plt.close()


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    scenario_counter = 0

    for failure_probability in FAILURE_LEVELS:
        for worker_failure_probability in WORKER_FAILURE_LEVELS:
            for capacity in CAPACITY_LEVELS:
                for carbon_label, carbon_scale in CARBON_SCALES.items():
                    scenario_counter += 1
                    for repetition in range(REPETITIONS):
                        seed = 20260929 + scenario_counter * 100 + repetition
                        for scheduler_name in SCHEDULER_NAMES:
                            result = run_one_experiment(
                                scheduler_name=scheduler_name,
                                failure_probability=failure_probability,
                                worker_failure_probability=worker_failure_probability,
                                capacity=capacity,
                                carbon_label=carbon_label,
                                carbon_scale=carbon_scale,
                                seed=seed,
                            )
                            result["repetition"] = repetition + 1
                            rows.append(result)

    raw = pd.DataFrame(rows)
    raw_path = OUTPUT_DIR / "raw_results.csv"
    raw.to_csv(raw_path, index=False)

    metric_columns = [
        "execution_time_seconds",
        "energy_kwh",
        "carbon_emissions_g",
        "failure_rate_percent",
        "recovery_time_seconds",
        "total_attempts",
        "failed_attempts",
        "completed_tasks",
        "permanently_failed_tasks",
        "recovered_task_count",
        "worker_failures",
        "worker_interrupted_tasks",
        "worker_failure_retries",
        "worker_recoveries",
    ]
    group_columns = [
        "scheduler",
        "failure_probability",
        "capacity_per_worker",
        "carbon_condition",
    ]
    summary = raw.groupby(group_columns, as_index=False)[metric_columns].agg(["mean", "std"])
    summary.columns = [
        "_".join(str(part) for part in column if part).strip("_")
        if isinstance(column, tuple)
        else column
        for column in summary.columns
    ]
    summary_path = OUTPUT_DIR / "summary_results.csv"
    summary.to_csv(summary_path, index=False)

    reduction = make_reduction_table(summary)
    reduction_path = OUTPUT_DIR / "carbon_reduction.csv"
    reduction.to_csv(reduction_path, index=False)

    create_figures(raw, summary, reduction)

    print("\nBenchmark complete.")
    print(f"Raw run-level results: {raw_path}")
    print(f"Aggregated summary:    {summary_path}")
    print(f"Carbon reductions:      {reduction_path}")
    print(f"Figures folder:         {FIGURE_DIR}")
    print(f"Total runs: {len(raw)}")
    print("\nMean carbon emissions by scheduler (g CO2):")
    print(
        raw.groupby("scheduler")["carbon_emissions_g"]
        .mean()
        .reindex(SCHEDULER_NAMES)
        .to_string()
    )
    print("\nMean carbon reduction vs each baseline (%):")
    print(
        reduction.groupby("baseline_scheduler")["carbon_reduction_percent"]
        .mean()
        .to_string()
    )


if __name__ == "__main__":
    main()
