
import pytest

from src.schedulers.cedar_inspired import CedarInspiredScheduler
from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.workflow.task import Task


def create_worker_pool(workers):
    pool = WorkerPool()

    for worker in workers:
        pool.add_worker(worker)

    return pool


def test_scheduler_prefers_lower_carbon_worker():
    worker_high_carbon = Worker(
        worker_id="worker_high_carbon",
        processing_speed=1.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=800.0,
    )

    worker_low_carbon = Worker(
        worker_id="worker_low_carbon",
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
    )

    pool = create_worker_pool([
        worker_high_carbon,
        worker_low_carbon,
    ])

    scheduler = CedarInspiredScheduler(
        pool,
        carbon_weight=100.0,
    )

    task = Task(
        task_id="task_1",
        name="Carbon Test",
        runtime=10.0,
    )

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "worker_low_carbon"


def test_scheduler_prefers_faster_worker_when_carbon_weight_is_zero():
    fast_worker = Worker(
        worker_id="fast_worker",
        processing_speed=2.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=800.0,
    )

    slow_worker = Worker(
        worker_id="slow_worker",
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
    )

    pool = create_worker_pool([
        fast_worker,
        slow_worker,
    ])

    scheduler = CedarInspiredScheduler(
        pool,
        carbon_weight=0.0,
    )

    task = Task(
        task_id="task_2",
        name="Speed Test",
        runtime=10.0,
    )

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "fast_worker"


def test_scheduler_raises_error_when_no_workers_are_available():
    pool = WorkerPool()

    scheduler = CedarInspiredScheduler(pool)

    task = Task(
        task_id="task_3",
        name="No Worker Test",
        runtime=10.0,
    )

    with pytest.raises(ValueError):
        scheduler.schedule(task)


def test_negative_carbon_weight_is_rejected():
    pool = WorkerPool()

    with pytest.raises(ValueError):
        CedarInspiredScheduler(
            pool,
            carbon_weight=-1.0,
        )