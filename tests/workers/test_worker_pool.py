
import pytest

from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool


def test_add_workers():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=2))
    pool.add_worker(Worker("worker_2", capacity=3))

    assert len(pool.workers) == 2


def test_duplicate_worker():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1"))

    with pytest.raises(ValueError):
        pool.add_worker(Worker("worker_1"))


def test_get_available_workers():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=1)
    worker_2 = Worker("worker_2", capacity=2)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.assign_task("task_1")

    available = pool.get_available_workers()

    assert len(available) == 1
    assert available[0].worker_id == "worker_2"


def test_assign_and_release_task():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=1))

    pool.assign_task("worker_1", "task_1")

    assert pool.get_worker("worker_1").available_slots() == 0

    pool.release_task("worker_1", "task_1")

    assert pool.get_worker("worker_1").available_slots() == 1


def test_total_capacity():
    pool = WorkerPool()

    pool.add_worker(Worker("worker_1", capacity=2))
    pool.add_worker(Worker("worker_2", capacity=3))

    assert pool.get_total_capacity() == 5
def test_failed_worker_not_in_available_workers():
    pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=2)
    worker_2 = Worker("worker_2", capacity=2)

    pool.add_worker(worker_1)
    pool.add_worker(worker_2)

    worker_1.fail()

    available = pool.get_available_workers()

    assert len(available) == 1
    assert available[0].worker_id == "worker_2"


def test_failed_worker_cannot_be_assigned_task():
    pool = WorkerPool()

    worker = Worker("worker_1", capacity=2)

    pool.add_worker(worker)

    worker.fail()

    with pytest.raises(ValueError):
        pool.assign_task("worker_1", "task_1")


def test_recovered_worker_returns_to_available_workers():
    pool = WorkerPool()

    worker = Worker("worker_1", capacity=2)

    pool.add_worker(worker)

    worker.fail()

    available = pool.get_available_workers()

    assert len(available) == 0

    worker.recover()

    available = pool.get_available_workers()

    assert len(available) == 1
    assert available[0].worker_id == "worker_1"