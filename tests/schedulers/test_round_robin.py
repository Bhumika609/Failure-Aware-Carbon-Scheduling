
import pytest

from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.schedulers.round_robin import RoundRobinScheduler


def test_round_robin_distribution():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=2))
    pool.add_worker(Worker("worker_2", capacity=2))
    pool.add_worker(Worker("worker_3", capacity=2))

    scheduler = RoundRobinScheduler(pool)

    assert scheduler.schedule("task_1").worker_id == "worker_1"
    assert scheduler.schedule("task_2").worker_id == "worker_2"
    assert scheduler.schedule("task_3").worker_id == "worker_3"
    assert scheduler.schedule("task_4").worker_id == "worker_1"


def test_round_robin_skips_full_worker():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=1)
    worker_2 = Worker("worker_2", capacity=2)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.assign_task("existing_task")

    scheduler = RoundRobinScheduler(pool)

    selected = scheduler.schedule("new_task")

    assert selected.worker_id == "worker_2"


def test_no_workers():
    pool = WorkerPool()
    scheduler = RoundRobinScheduler(pool)

    with pytest.raises(ValueError):
        scheduler.schedule("task_1")


def test_all_workers_full():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=1)
    worker_2 = Worker("worker_2", capacity=1)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.assign_task("task_1")
    worker_2.assign_task("task_2")

    scheduler = RoundRobinScheduler(pool)

    with pytest.raises(ValueError):
        scheduler.schedule("task_3")