
import pytest

from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.schedulers.least_loaded import LeastLoadedScheduler


def test_selects_least_loaded_worker():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=3)
    worker_2 = Worker("worker_2", capacity=3)
    worker_3 = Worker("worker_3", capacity=3)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)
    pool.add_worker(worker_3)

    worker_1.assign_task("task_1")
    worker_1.assign_task("task_2")
    worker_2.assign_task("task_3")

    scheduler = LeastLoadedScheduler(pool)

    selected = scheduler.schedule("new_task")

    assert selected.worker_id == "worker_3"


def test_skips_full_workers():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=1)
    worker_2 = Worker("worker_2", capacity=2)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.assign_task("task_1")

    scheduler = LeastLoadedScheduler(pool)

    selected = scheduler.schedule("new_task")

    assert selected.worker_id == "worker_2"


def test_tie_selects_first_worker():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=2))
    pool.add_worker(Worker("worker_2", capacity=2))

    scheduler = LeastLoadedScheduler(pool)

    selected = scheduler.schedule("new_task")

    assert selected.worker_id == "worker_1"


def test_no_available_workers():
    pool = WorkerPool()

    worker = Worker("worker_1", capacity=1)
    pool.add_worker(worker)

    worker.assign_task("existing_task")

    scheduler = LeastLoadedScheduler(pool)

    with pytest.raises(ValueError):
        scheduler.schedule("new_task")