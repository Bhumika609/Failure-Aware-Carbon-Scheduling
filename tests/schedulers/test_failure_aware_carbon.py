import pytest

from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.workflow.task import Task
from src.schedulers.failure_aware_carbon import (
    FailureAwareCarbonScheduler,
)


def test_scheduler_prefers_worker_with_lower_failure_probability():
    worker_pool = WorkerPool()

    unreliable_worker = Worker(
        worker_id="worker_1",
        processing_speed=1.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.8,
    )

    reliable_worker = Worker(
        worker_id="worker_2",
        processing_speed=1.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    worker_pool.add_worker(unreliable_worker)
    worker_pool.add_worker(reliable_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=1.0,
        carbon_weight=1.0,
        load_weight=1.0,
        failure_weight=10.0,
    )

    task = Task(
        task_id="A",
        name="Test Task",
        runtime=10.0,
    )

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "worker_2"
def test_scheduler_prefers_worker_with_lower_carbon_emissions():
    worker_pool = WorkerPool()

    high_carbon_worker = Worker(
        worker_id="worker_1",
        processing_speed=1.0,
        power_watts=400.0,
        carbon_intensity_g_per_kwh=800.0,
        failure_probability=0.0,
    )

    low_carbon_worker = Worker(
        worker_id="worker_2",
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=200.0,
        failure_probability=0.0,
    )

    worker_pool.add_worker(high_carbon_worker)
    worker_pool.add_worker(low_carbon_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=1.0,
        carbon_weight=1000.0,
        load_weight=1.0,
        failure_weight=1.0,
    )

    task = Task(
        task_id="B",
        name="Carbon Test Task",
        runtime=10.0,
    )

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "worker_2"
def test_scheduler_prefers_worker_with_faster_execution_time():
    worker_pool = WorkerPool()

    slow_worker = Worker(
        worker_id="worker_1",
        processing_speed=1.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.0,
    )

    fast_worker = Worker(
        worker_id="worker_2",
        processing_speed=4.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.0,
    )

    worker_pool.add_worker(slow_worker)
    worker_pool.add_worker(fast_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=1.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task(
        task_id="C",
        name="Execution Time Test",
        runtime=10.0,
    )

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "worker_2"
def test_scheduler_prefers_worker_with_lower_load():
    worker_pool = WorkerPool()

    busy_worker = Worker(
        worker_id="worker_1",
        capacity=2,
        processing_speed=1.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.0,
        running_tasks=["existing_task"],
    )

    idle_worker = Worker(
        worker_id="worker_2",
        capacity=2,
        processing_speed=1.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.0,
    )

    worker_pool.add_worker(busy_worker)
    worker_pool.add_worker(idle_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=10.0,
        failure_weight=0.0,
    )

    task = Task(
        task_id="D",
        name="Load Test Task",
        runtime=10.0,
    )

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "worker_2"
def test_scheduler_raises_error_when_no_worker_is_available():
    worker_pool = WorkerPool()

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool
    )

    task = Task(
        task_id="E",
        name="No Worker Test",
        runtime=10.0,
    )

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        scheduler.schedule(task)
def test_scheduler_rejects_negative_weights():
    worker_pool = WorkerPool()

    with pytest.raises(
        ValueError,
        match="Time weight cannot be negative",
    ):
        FailureAwareCarbonScheduler(
            worker_pool=worker_pool,
            time_weight=-1.0,
        )
def test_worker_rejects_invalid_failure_probability():
    with pytest.raises(
        ValueError,
        match="Failure probability must be between 0 and 1",
    ):
        Worker(
            worker_id="worker_invalid",
            failure_probability=1.5,
        )
def test_zero_failure_probability_has_no_failure_penalty():
    worker_pool = WorkerPool()

    worker = Worker(
        worker_id="worker_1",
        processing_speed=1.0,
        failure_probability=0.0,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=1.0,
    )

    task = Task(
        task_id="F",
        name="Zero Failure Test",
        runtime=10.0,
    )

    score = scheduler.calculate_score(task, worker)

    assert score == 0.0
def test_zero_carbon_intensity_has_zero_carbon_emissions():
    worker = Worker(
        worker_id="worker_1",
        power_watts=200.0,
        carbon_intensity_g_per_kwh=0.0,
    )

    carbon = worker.estimate_carbon_g(10.0)

    assert carbon == 0.0
# Test 10: Failure probability of 1.0 is accepted

def test_worker_accepts_maximum_failure_probability():
    worker = Worker(
        worker_id="worker_1",
        failure_probability=1.0,
    )

    assert worker.failure_probability == 1.0


# Test 11: Negative failure probability is rejected

def test_worker_rejects_negative_failure_probability():
    with pytest.raises(
        ValueError,
        match="Failure probability must be between 0 and 1",
    ):
        Worker(
            worker_id="worker_1",
            failure_probability=-0.1,
        )


# Test 12: Carbon weight cannot be negative

def test_scheduler_rejects_negative_carbon_weight():
    with pytest.raises(
        ValueError,
        match="Carbon weight cannot be negative",
    ):
        FailureAwareCarbonScheduler(
            worker_pool=WorkerPool(),
            carbon_weight=-1.0,
        )


# Test 13: Load weight cannot be negative

def test_scheduler_rejects_negative_load_weight():
    with pytest.raises(
        ValueError,
        match="Load weight cannot be negative",
    ):
        FailureAwareCarbonScheduler(
            worker_pool=WorkerPool(),
            load_weight=-1.0,
        )


# Test 14: Failure weight cannot be negative

def test_scheduler_rejects_negative_failure_weight():
    with pytest.raises(
        ValueError,
        match="Failure weight cannot be negative",
    ):
        FailureAwareCarbonScheduler(
            worker_pool=WorkerPool(),
            failure_weight=-1.0,
        )
# Test 1: Scheduler selects the worker with the lowest score
def test_scheduler_selects_worker_with_lowest_score():
    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        processing_speed=1.0,
    )

    worker_2 = Worker(
        "W2",
        processing_speed=4.0,
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A", runtime=20.0)

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W2"


# Test 2: Scheduler calculates execution time correctly
def test_scheduler_calculates_execution_time():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=2.0,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A", runtime=20.0)

    score = scheduler.calculate_score(task, worker)

    expected_time = 10.0
    expected_carbon = worker.estimate_carbon_g(task.runtime)
    expected_load = 0.0
    expected_failure = worker.failure_probability * expected_time

    expected_score = (
        expected_time
        + expected_carbon
        + expected_load
        + expected_failure
    )

    assert score == pytest.approx(expected_score)


# Test 3: Scheduler calculates failure penalty correctly
def test_scheduler_calculates_failure_penalty():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=2.0,
        failure_probability=0.5,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=1.0,
    )

    task = Task("A", "Task A", runtime=20.0)

    score = scheduler.calculate_score(task, worker)

    assert score == pytest.approx(5.0)


# Test 4: Scheduler assigns the task to the selected worker
def test_scheduler_assigns_task_to_selected_worker():
    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A")

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W1"
    assert "A" in worker.running_tasks


# Test 5: Scheduler raises an error when no workers are available
def test_scheduler_raises_error_when_no_workers_are_available():
    worker_pool = WorkerPool()

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A")

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        scheduler.schedule(task)
# Additional unit tests: Batch 2


# Test 20: Verify the complete weighted score calculation
def test_scheduler_calculates_complete_weighted_score():
    worker_pool = WorkerPool()

    worker = Worker(
        worker_id="W1",
        capacity=4,
        processing_speed=2.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=300.0,
        failure_probability=0.2,
        running_tasks=["existing_task"],
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=2.0,
        carbon_weight=3.0,
        load_weight=4.0,
        failure_weight=5.0,
    )

    task = Task("A", "Task A", runtime=12.0)

    score = scheduler.calculate_score(task, worker)

    expected_score = 19.15

    assert score == pytest.approx(expected_score)


# Test 21: Equal scores should select the first worker
def test_scheduler_selects_first_worker_when_scores_are_equal():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1")
    worker_2 = Worker("W2")

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A", runtime=10.0)

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W1"


# Test 22: Scheduler skips a worker at full capacity
def test_scheduler_skips_worker_at_full_capacity():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_1.assign_task("existing_task")

    worker_2 = Worker("W2", capacity=1)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A")

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W2"
    assert worker_1.running_tasks == ["existing_task"]
    assert worker_2.running_tasks == ["A"]


# Test 23: Longer task runtime increases the time-based score
def test_scheduler_score_increases_with_task_runtime():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=1.0,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    short_task = Task("A", "Short Task", runtime=10.0)
    long_task = Task("B", "Long Task", runtime=20.0)

    short_score = scheduler.calculate_score(short_task, worker)
    long_score = scheduler.calculate_score(long_task, worker)

    assert short_score == pytest.approx(10.0)
    assert long_score == pytest.approx(20.0)
    assert long_score > short_score


# Test 24: Only the selected worker receives the task
def test_scheduler_assigns_task_only_to_selected_worker():
    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        processing_speed=1.0,
    )

    worker_2 = Worker(
        "W2",
        processing_speed=4.0,
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A", runtime=20.0)

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W2"
    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == ["A"]
# Batch 3: Advanced scheduler unit tests


# Test 25: Zero weights produce a zero score
def test_scheduler_returns_zero_score_when_all_weights_are_zero():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        failure_probability=0.5,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A", runtime=10.0)

    score = scheduler.calculate_score(task, worker)

    assert score == pytest.approx(0.0)


# Test 26: Increasing time weight increases the score
def test_scheduler_score_increases_with_time_weight():
    worker_pool = WorkerPool()

    worker = Worker("W1", processing_speed=2.0)
    worker_pool.add_worker(worker)

    task = Task("A", "Task A", runtime=20.0)

    scheduler_1 = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    scheduler_2 = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=3.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    score_1 = scheduler_1.calculate_score(task, worker)
    score_2 = scheduler_2.calculate_score(task, worker)

    assert score_1 == pytest.approx(10.0)
    assert score_2 == pytest.approx(30.0)
    assert score_2 > score_1


# Test 27: Increasing carbon weight increases the score
def test_scheduler_score_increases_with_carbon_weight():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
    )

    worker_pool.add_worker(worker)

    task = Task("A", "Task A", runtime=10.0)

    scheduler_1 = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=1.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    scheduler_2 = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=3.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    score_1 = scheduler_1.calculate_score(task, worker)
    score_2 = scheduler_2.calculate_score(task, worker)

    assert score_1 > 0.0
    assert score_2 == pytest.approx(score_1 * 3.0)


# Test 28: Failure penalty increases with task runtime
def test_failure_penalty_increases_with_task_runtime():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=2.0,
        failure_probability=0.5,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=1.0,
    )

    short_task = Task("A", "Short Task", runtime=10.0)
    long_task = Task("B", "Long Task", runtime=20.0)

    short_score = scheduler.calculate_score(short_task, worker)
    long_score = scheduler.calculate_score(long_task, worker)

    assert short_score == pytest.approx(2.5)
    assert long_score == pytest.approx(5.0)
    assert long_score > short_score


# Test 29: Load score uses the worker's capacity
def test_scheduler_load_score_uses_worker_capacity():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        capacity=4,
        running_tasks=["task_1", "task_2"],
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=1.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A")

    score = scheduler.calculate_score(task, worker)

    assert score == pytest.approx(0.5)
# Batch 4: Scheduler edge cases


# Test 30: Worker with zero carbon intensity has zero carbon score
def test_scheduler_handles_worker_with_zero_carbon_intensity():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        power_watts=200.0,
        carbon_intensity_g_per_kwh=0.0,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=1.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A", runtime=10.0)

    score = scheduler.calculate_score(task, worker)

    assert score == pytest.approx(0.0)


# Test 31: Worker with maximum failure probability
def test_scheduler_handles_maximum_failure_probability():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=2.0,
        failure_probability=1.0,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=1.0,
    )

    task = Task("A", "Task A", runtime=20.0)

    score = scheduler.calculate_score(task, worker)

    assert score == pytest.approx(10.0)


# Test 32: Scheduler works with a single available worker
def test_scheduler_selects_single_available_worker():
    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A")

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W1"
    assert worker.running_tasks == ["A"]


# Test 33: Scheduler assigns tasks within worker capacity
def test_scheduler_assigns_multiple_tasks_within_worker_capacity():
    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=3)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task_1 = Task("A", "Task A")
    task_2 = Task("B", "Task B")
    task_3 = Task("C", "Task C")

    selected_worker_1 = scheduler.schedule(task_1)
    selected_worker_2 = scheduler.schedule(task_2)
    selected_worker_3 = scheduler.schedule(task_3)

    assert selected_worker_1.worker_id == "W1"
    assert selected_worker_2.worker_id == "W1"
    assert selected_worker_3.worker_id == "W1"

    assert worker.running_tasks == ["A", "B", "C"]
    assert worker.is_available() is False


# Test 34: Scheduler balances execution time and carbon emissions
def test_scheduler_balances_execution_time_and_carbon_emissions():
    worker_pool = WorkerPool()

    fast_worker = Worker(
        "W1",
        processing_speed=4.0,
        power_watts=400.0,
        carbon_intensity_g_per_kwh=800.0,
    )

    efficient_worker = Worker(
        "W2",
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
    )

    worker_pool.add_worker(fast_worker)
    worker_pool.add_worker(efficient_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=1.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A", runtime=10.0)

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W2"
# Batch 5: Scheduler validation and reliability


# Test 35: Repeated score calculations should be consistent
def test_scheduler_returns_consistent_score_for_same_inputs():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=2.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.2,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A", runtime=10.0)

    score_1 = scheduler.calculate_score(task, worker)
    score_2 = scheduler.calculate_score(task, worker)
    score_3 = scheduler.calculate_score(task, worker)

    assert score_1 == pytest.approx(score_2)
    assert score_2 == pytest.approx(score_3)


# Test 36: Load score should account for worker capacity
def test_scheduler_load_score_accounts_for_worker_capacity():
    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        capacity=2,
        running_tasks=["task_1"],
    )

    worker_2 = Worker(
        "W2",
        capacity=4,
        running_tasks=["task_2"],
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=1.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A")

    score_1 = scheduler.calculate_score(task, worker_1)
    score_2 = scheduler.calculate_score(task, worker_2)

    assert score_1 == pytest.approx(0.5)
    assert score_2 == pytest.approx(0.25)
    assert score_2 < score_1


def test_scheduler_weight_changes_affect_worker_preference():
    worker_pool = WorkerPool()

    fast_worker = Worker(
        "W1",
        processing_speed=2.0,
        power_watts=400.0,
        carbon_intensity_g_per_kwh=800.0,
    )

    efficient_worker = Worker(
        "W2",
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
    )

    worker_pool.add_worker(fast_worker)
    worker_pool.add_worker(efficient_worker)

    task = Task("A", "Task A", runtime=10.0)

    time_focused_scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=0.5,
        load_weight=0.0,
        failure_weight=0.0,
    )

    carbon_focused_scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=2.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    selected_worker_1 = time_focused_scheduler.schedule(task)

    assert selected_worker_1.worker_id == "W1"

    fast_worker.release_task("A")

    selected_worker_2 = carbon_focused_scheduler.schedule(task)

    assert selected_worker_2.worker_id == "W2"

# Test 38: Increasing failure weight increases the failure contribution
def test_scheduler_failure_weight_changes_failure_penalty():
    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        processing_speed=2.0,
        failure_probability=0.5,
    )

    worker_pool.add_worker(worker)

    task = Task("A", "Task A", runtime=20.0)

    scheduler_1 = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=0.5,
        load_weight=0.0,
        failure_weight=1.0,
    )

    scheduler_2 = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=2.0,
        load_weight=0.0,
        failure_weight=3.0,
    )

    score_1 = scheduler_1.calculate_score(task, worker)
    score_2 = scheduler_2.calculate_score(task, worker)

    assert score_1 == pytest.approx(15.1111111111)
    assert score_2 == pytest.approx(25.4444444444)
    assert score_2 > score_1


# Test 39: A failed scheduling attempt should not assign the task
def test_scheduler_does_not_assign_task_when_no_worker_is_available():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=1)

    worker_1.assign_task("existing_task_1")
    worker_2.assign_task("existing_task_2")

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A")

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        scheduler.schedule(task)

    assert "A" not in worker_1.running_tasks
    assert "A" not in worker_2.running_tasks

# Test 40: Normalization produces correct values
def test_scheduler_normalize_produces_correct_values():
    scheduler = FailureAwareCarbonScheduler(
        WorkerPool()
    )

    assert scheduler.normalize(5, 5, 15) == pytest.approx(0.0)
    assert scheduler.normalize(10, 5, 15) == pytest.approx(0.5)
    assert scheduler.normalize(15, 5, 15) == pytest.approx(1.0)


# Test 41: Normalization handles equal values
def test_scheduler_normalize_handles_equal_values():
    scheduler = FailureAwareCarbonScheduler(
        WorkerPool()
    )

    assert scheduler.normalize(10, 10, 10) == pytest.approx(0.0)
# Test 42: Calculate score using raw weighted metrics
def test_calculate_score_matches_weighted_raw_metrics():
    worker_pool = WorkerPool()

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=2.0,
        carbon_weight=3.0,
        load_weight=4.0,
        failure_weight=5.0,
    )

    worker = Worker(
        worker_id="worker1",
        capacity=2,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    task = Task(
        task_id="task1",
        name="Test Task",
        runtime=10,
    )

    metrics = scheduler.calculate_metrics(task, worker)

    expected_score = (
        2.0 * metrics["time"]
        + 3.0 * metrics["carbon"]
        + 4.0 * metrics["load"]
        + 5.0 * metrics["failure"]
    )

    actual_score = scheduler.calculate_score(task, worker)

    assert actual_score == pytest.approx(expected_score)
# Test 43: Normalized scores stay within the expected range
def test_normalized_scores_stay_within_expected_range():
    worker_pool = WorkerPool()

    scheduler = FailureAwareCarbonScheduler(
        worker_pool=worker_pool,
        time_weight=2.0,
        carbon_weight=3.0,
        load_weight=4.0,
        failure_weight=5.0,
    )

    worker1 = Worker(
        worker_id="worker1",
        capacity=2,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    worker2 = Worker(
        worker_id="worker2",
        capacity=2,
        power_watts=200,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.2,
    )

    task = Task(
        task_id="task1",
        name="Test Task",
        runtime=10,
    )

    scores = scheduler.calculate_normalized_scores(
        task,
        [worker1, worker2],
    )

    assert len(scores) == 2

    for worker, score in scores:
        assert 0.0 <= score <= 14.0
# Test 44: Empty worker list returns an empty result
def test_normalized_scores_return_empty_for_no_workers():
    scheduler = FailureAwareCarbonScheduler(WorkerPool())

    task = Task(
        task_id="task1",
        name="Test Task",
        runtime=10,
    )

    scores = scheduler.calculate_normalized_scores(task, [])

    assert scores == []


# Test 45: One worker gets a normalized score of zero
def test_normalized_scores_single_worker_gets_zero():
    scheduler = FailureAwareCarbonScheduler(WorkerPool())

    worker = Worker(
        worker_id="worker1",
        capacity=2,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    task = Task(
        task_id="task1",
        name="Test Task",
        runtime=10,
    )

    scores = scheduler.calculate_normalized_scores(
        task,
        [worker],
    )

    assert len(scores) == 1
    assert scores[0][0] == worker
    assert scores[0][1] == pytest.approx(0.0)


# Test 46: Identical workers receive equal normalized scores
def test_normalized_scores_identical_workers_are_equal():
    scheduler = FailureAwareCarbonScheduler(WorkerPool())

    worker1 = Worker(
        worker_id="worker1",
        capacity=2,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    worker2 = Worker(
        worker_id="worker2",
        capacity=2,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    task = Task(
        task_id="task1",
        name="Test Task",
        runtime=10,
    )

    scores = scheduler.calculate_normalized_scores(
        task,
        [worker1, worker2],
    )

    assert len(scores) == 2
    assert scores[0][1] == pytest.approx(scores[1][1])


# Test 47: Worker with lower carbon and failure penalty gets a lower score
def test_normalized_scores_prefer_worker_with_lower_carbon_and_failure():
    scheduler = FailureAwareCarbonScheduler(WorkerPool())

    worker1 = Worker(
        worker_id="worker1",
        capacity=2,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    worker2 = Worker(
        worker_id="worker2",
        capacity=2,
        power_watts=200,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.2,
    )

    task = Task(
        task_id="task1",
        name="Test Task",
        runtime=10,
    )

    scores = scheduler.calculate_normalized_scores(
        task,
        [worker1, worker2],
    )

    assert scores[0][0] == worker1
    assert scores[1][0] == worker2
    assert scores[0][1] < scores[1][1]
# Test 48: Scheduler selects the worker with the lowest normalized score
def test_schedule_selects_worker_with_lowest_normalized_score():
    worker_pool = WorkerPool()

    worker1 = Worker(
        worker_id="worker1",
        capacity=2,
        processing_speed=1.0,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    worker2 = Worker(
        worker_id="worker2",
        capacity=2,
        processing_speed=2.0,
        power_watts=200,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.2,
    )

    worker_pool.add_worker(worker1)
    worker_pool.add_worker(worker2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task(
        task_id="task48",
        name="Test Task",
        runtime=10,
    )

    scores = scheduler.calculate_normalized_scores(
        task,
        [worker1, worker2],
    )

    expected_worker = min(scores, key=lambda item: item[1])[0]
    selected_worker = scheduler.schedule(task)

    assert selected_worker == expected_worker


# Test 49: Scheduler raises an error when no workers are available
def test_schedule_raises_error_when_no_workers_are_available():
    worker_pool = WorkerPool()

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task(
        task_id="task49",
        name="Test Task",
        runtime=10,
    )

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        scheduler.schedule(task)


# Test 50: Scheduling a task increases the worker's running task count
def test_schedule_assigns_task_and_updates_worker_load():
    worker_pool = WorkerPool()

    worker = Worker(
        worker_id="worker1",
        capacity=2,
        processing_speed=1.0,
        power_watts=100,
        carbon_intensity_g_per_kwh=400.0,
        failure_probability=0.1,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task(
        task_id="task50",
        name="Test Task",
        runtime=10,
    )

    initial_task_count = len(worker.running_tasks)

    selected_worker = scheduler.schedule(task)

    assert selected_worker == worker
    assert len(worker.running_tasks) == initial_task_count + 1
    assert task.task_id in worker.running_tasks