
import pytest

from src.workers.worker import Worker


def test_worker_creation():
    worker = Worker("worker_1", capacity=2)

    assert worker.worker_id == "worker_1"
    assert worker.available_slots() == 2


def test_assign_task():
    worker = Worker("worker_1", capacity=2)

    worker.assign_task("task_1")

    assert worker.available_slots() == 1
    assert worker.is_available()


def test_worker_capacity_limit():
    worker = Worker("worker_1", capacity=1)

    worker.assign_task("task_1")

    with pytest.raises(ValueError):
        worker.assign_task("task_2")


def test_release_task():
    worker = Worker("worker_1", capacity=1)

    worker.assign_task("task_1")
    worker.release_task("task_1")

    assert worker.available_slots() == 1


def test_invalid_capacity():
    with pytest.raises(ValueError):
        Worker("worker_1", capacity=0)
def test_worker_never_exceeds_capacity():
    worker = Worker(
        worker_id="W1",
        capacity=2
    )

    worker.assign_task("T1")
    worker.assign_task("T2")

    assert worker.available_slots() == 0
    assert len(worker.running_tasks) == 2

    with pytest.raises(ValueError):
        worker.assign_task("T3")

    assert len(worker.running_tasks) == 2
def test_worker_starts_healthy():
    worker = Worker("worker_1", capacity=2)

    assert not worker.is_failed
    assert worker.is_available()


def test_worker_failure():
    worker = Worker("worker_1", capacity=2)

    worker.fail()

    assert worker.is_failed
    assert not worker.is_available()
    assert worker.available_slots() == 0


def test_failed_worker_cannot_accept_task():
    worker = Worker("worker_1", capacity=2)

    worker.fail()

    with pytest.raises(ValueError):
        worker.assign_task("task_1")


def test_running_tasks_remain_after_worker_failure():
    worker = Worker("worker_1", capacity=2)

    worker.assign_task("task_1")
    worker.assign_task("task_2")

    worker.fail()

    assert worker.is_failed
    assert worker.running_tasks == ["task_1", "task_2"]
    assert worker.available_slots() == 0


def test_worker_recovery():
    worker = Worker("worker_1", capacity=2)

    worker.fail()

    assert worker.is_failed
    assert not worker.is_available()

    worker.recover()

    assert not worker.is_failed
    assert worker.is_available()
    assert worker.available_slots() == 2


def test_recovered_worker_can_accept_task():
    worker = Worker("worker_1", capacity=2)

    worker.fail()
    worker.recover()

    worker.assign_task("task_1")

    assert worker.running_tasks == ["task_1"]
    assert worker.available_slots() == 1
    assert worker.is_available()