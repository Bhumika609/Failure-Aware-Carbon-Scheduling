
import pytest

from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.workflow.task import Task
from src.schedulers.cedar_inspired import CedarInspiredScheduler


def test_selects_worker_with_lowest_score():
    pool = WorkerPool()

    pool.add_worker(
        Worker("worker_1", capacity=2, processing_speed=1.0)
    )
    pool.add_worker(
        Worker("worker_2", capacity=2, processing_speed=2.0)
    )

    scheduler = CedarInspiredScheduler(pool)

    task = Task("task_1", "Task 1", runtime=10.0)

    selected = scheduler.schedule(task)

    assert selected.worker_id == "worker_2"


def test_considers_worker_load():
    pool = WorkerPool()

    worker_1 = Worker(
        "worker_1", capacity=4, processing_speed=1.5
    )
    worker_2 = Worker(
        "worker_2", capacity=2, processing_speed=1.0
    )

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.assign_task("existing_1")
    worker_1.assign_task("existing_2")
    worker_1.assign_task("existing_3")

    scheduler = CedarInspiredScheduler(pool)

    task = Task("task_1", "Task 1", runtime=10.0)

    selected = scheduler.schedule(task)

    assert selected.worker_id == "worker_2"


def test_skips_full_workers():
    pool = WorkerPool()

    worker_1 = Worker(
        "worker_1", capacity=1, processing_speed=3.0
    )
    worker_2 = Worker(
        "worker_2", capacity=2, processing_speed=1.0
    )

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.assign_task("existing_task")

    scheduler = CedarInspiredScheduler(pool)

    task = Task("task_1", "Task 1", runtime=10.0)

    selected = scheduler.schedule(task)

    assert selected.worker_id == "worker_2"


def test_no_available_workers():
    pool = WorkerPool()

    worker = Worker(
        "worker_1", capacity=1, processing_speed=1.0
    )
    pool.add_worker(worker)

    worker.assign_task("existing_task")

    scheduler = CedarInspiredScheduler(pool)

    task = Task("task_1", "Task 1", runtime=10.0)

    with pytest.raises(ValueError):
        scheduler.schedule(task)