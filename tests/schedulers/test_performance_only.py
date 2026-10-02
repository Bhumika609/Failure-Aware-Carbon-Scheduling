
import pytest

from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.schedulers.performance_only import PerformanceOnlyScheduler


def test_selects_fastest_worker():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=2, processing_speed=1.0))
    pool.add_worker(Worker("worker_2", capacity=2, processing_speed=2.0))
    pool.add_worker(Worker("worker_3", capacity=2, processing_speed=1.5))

    scheduler = PerformanceOnlyScheduler(pool)

    selected = scheduler.schedule("task_1")

    assert selected.worker_id == "worker_2"


def test_skips_full_fastest_worker():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=1, processing_speed=3.0)
    worker_2 = Worker("worker_2", capacity=2, processing_speed=2.0)
    worker_3 = Worker("worker_3", capacity=2, processing_speed=1.0)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)
    pool.add_worker(worker_3)

    worker_1.assign_task("existing_task")

    scheduler = PerformanceOnlyScheduler(pool)

    selected = scheduler.schedule("new_task")

    assert selected.worker_id == "worker_2"


def test_equal_speed_selects_first_worker():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=2, processing_speed=2.0))
    pool.add_worker(Worker("worker_2", capacity=2, processing_speed=2.0))

    scheduler = PerformanceOnlyScheduler(pool)

    selected = scheduler.schedule("task_1")

    assert selected.worker_id == "worker_1"


def test_no_available_workers():
    pool = WorkerPool()

    worker = Worker("worker_1", capacity=1, processing_speed=2.0)
    pool.add_worker(worker)

    worker.assign_task("existing_task")

    scheduler = PerformanceOnlyScheduler(pool)

    with pytest.raises(ValueError):
        scheduler.schedule("new_task")