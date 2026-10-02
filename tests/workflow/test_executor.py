
from src.workflow.task import Task
from src.workflow.workflow import Workflow
from src.workflow.executor import WorkflowExecutor
from src.workers.worker import Worker
from src.workers.worker_pool import WorkerPool
from src.schedulers.cedar_inspired import CedarInspiredScheduler
from src.schedulers.failure_aware_carbon import (FailureAwareCarbonScheduler)
from src.schedulers.round_robin import RoundRobinScheduler
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import pytest

def test_sequential_workflow_execution():

    workflow = Workflow("workflow_1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["A"])
    task_d = Task("D", "Task D", dependencies=["B", "C"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)
    workflow.add_task(task_d)

    executor = WorkflowExecutor()

    execution_order = executor.execute_workflow(workflow)

    assert execution_order == ["A", "B", "C", "D"]

    assert task_a.status == "COMPLETED"
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"
    assert task_d.status == "COMPLETED"


def test_task_result_is_stored():

    workflow = Workflow("workflow_2")

    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    executor = WorkflowExecutor()

    executor.execute_workflow(workflow)

    assert task_a.result == "Result of A"

def test_execution_log():

    workflow = Workflow("workflow_3")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    executor = WorkflowExecutor()

    executor.execute_workflow(workflow)

    assert len(executor.execution_log) == 2

    assert executor.execution_log[0]["task_id"] == "A"
    assert executor.execution_log[0]["status"] == "COMPLETED"

    assert executor.execution_log[1]["task_id"] == "B"
    assert executor.execution_log[1]["status"] == "COMPLETED"


def test_workflow_statistics():

    workflow = Workflow("workflow_4")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    executor = WorkflowExecutor()

    executor.execute_workflow(workflow)

    statistics = executor.get_statistics(workflow)

    assert statistics["total_tasks"] == 2
    assert statistics["completed_tasks"] == 2
    assert statistics["failed_tasks"] == 0
    assert statistics["pending_tasks"] == 0

def test_workflow_status_completed():

    workflow = Workflow("workflow_5")

    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    executor = WorkflowExecutor()

    executor.execute_workflow(workflow)

    assert executor.get_workflow_status(workflow) == "COMPLETED"


def test_workflow_status_in_progress():

    workflow = Workflow("workflow_6")

    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    executor = WorkflowExecutor()

    workflow.refresh_ready_tasks()

    assert executor.get_workflow_status(workflow) == "IN_PROGRESS"


def test_workflow_status_blocked():

    workflow = Workflow("workflow_7")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    task_a.transition_to("READY")
    task_a.transition_to("RUNNING")
    task_a.transition_to("FAILED")

    executor = WorkflowExecutor()
    assert executor.get_workflow_status(workflow) == "BLOCKED"

def test_scheduler_assigns_worker_during_execution():

    workflow = Workflow("workflow_scheduler_1")
    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    worker_pool = WorkerPool()
    worker = Worker("worker_1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = CedarInspiredScheduler(worker_pool)

    def task_runner(task):
        assert task.task_id in worker.running_tasks
        return f"Executed on {worker.worker_id}"

    executor = WorkflowExecutor(
        task_runner=task_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task_a.status == "COMPLETED"
    assert task_a.result == "Executed on worker_1"
    assert worker.running_tasks == []


def test_worker_capacity_released_after_success():

    workflow = Workflow("workflow_scheduler_2")
    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    worker_pool = WorkerPool()
    worker = Worker("worker_1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = CedarInspiredScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task_a.status == "COMPLETED"
    assert worker.available_slots() == 1
    assert worker.running_tasks == []


def test_worker_capacity_released_after_failure():

    workflow = Workflow("workflow_scheduler_3")
    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    worker_pool = WorkerPool()
    worker = Worker("worker_1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = CedarInspiredScheduler(worker_pool)

    def failing_runner(task):
        raise RuntimeError("Simulated task failure")

    executor = WorkflowExecutor(
        task_runner=failing_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    import pytest

    with pytest.raises(RuntimeError, match="Simulated task failure"):
        executor.execute_workflow(workflow)

    assert task_a.status == "FAILED"
    assert worker.available_slots() == 1
    assert worker.running_tasks == []


def test_executor_raises_when_no_worker_is_available():

    workflow = Workflow("workflow_scheduler_4")
    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    worker_pool = WorkerPool()
    worker = Worker("worker_1", capacity=1)

    worker.assign_task("another_task")
    worker_pool.add_worker(worker)

    scheduler = CedarInspiredScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    import pytest

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        executor.execute_workflow(workflow)

    assert task_a.status == "READY"
    assert worker.running_tasks == ["another_task"]

def test_multiple_ready_tasks_are_scheduled_and_completed():

    workflow = Workflow("workflow_multiple_tasks")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B")
    task_c = Task("C", "Task C")

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    worker_pool = WorkerPool()

    worker_1 = Worker("worker_1", capacity=1)
    worker_2 = Worker("worker_2", capacity=1)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = RoundRobinScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    execution_order = executor.execute_workflow(workflow)

    assert execution_order == ["A", "B", "C"]

    assert task_a.status == "COMPLETED"
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"

    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == []

    assert worker_1.available_slots() == 1
    assert worker_2.available_slots() == 1
def test_independent_tasks_execute_concurrently():
    workflow = Workflow("workflow_concurrent")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B")

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    barrier = Barrier(2)

    def task_runner(task):
        barrier.wait(timeout=3)
        return f"Result of {task.task_id}"

    executor = WorkflowExecutor(task_runner=task_runner)

    execution_order = executor.execute_workflow(workflow)

    assert set(execution_order) == {"A", "B"}
    assert task_a.status == "COMPLETED"
    assert task_b.status == "COMPLETED"

def test_task_succeeds_after_one_retry():
    workflow = Workflow("workflow_retry_1")

    task_a = Task("A", "Task A", max_retries=3)
    workflow.add_task(task_a)

    call_count = 0

    def fail_once_runner(task):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("Temporary failure")

        return "Success after retry"

    executor = WorkflowExecutor(task_runner=fail_once_runner)

    execution_order = executor.execute_workflow(workflow)

    assert task_a.status == "COMPLETED"
    assert task_a.attempts == 2
    assert call_count == 2
    assert task_a.result == "Success after retry"
    assert execution_order == ["A"]


def test_task_fails_after_all_retries():
    workflow = Workflow("workflow_retry_2")

    task_a = Task("A", "Task A", max_retries=3)
    workflow.add_task(task_a)

    call_count = 0

    def always_fail_runner(task):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("Permanent failure")

    executor = WorkflowExecutor(task_runner=always_fail_runner)

    import pytest

    with pytest.raises(RuntimeError, match="Permanent failure"):
        executor.execute_workflow(workflow)

    assert task_a.status == "FAILED"
    assert task_a.attempts == 4
    assert call_count == 4


def test_retry_attempt_count_is_recorded_in_execution_log():
    workflow = Workflow("workflow_retry_3")

    task_a = Task("A", "Task A", max_retries=2)
    workflow.add_task(task_a)

    call_count = 0

    def fail_twice_runner(task):
        nonlocal call_count
        call_count += 1

        if call_count <= 2:
            raise RuntimeError("Temporary failure")

        return "Completed"

    executor = WorkflowExecutor(task_runner=fail_twice_runner)

    executor.execute_workflow(workflow)

    assert task_a.attempts == 3
    assert len(executor.execution_log) == 3

    assert executor.execution_log[0]["attempt"] == 1
    assert executor.execution_log[1]["attempt"] == 2
    assert executor.execution_log[2]["attempt"] == 3

    assert executor.execution_log[0]["status"] == "FAILED"
    assert executor.execution_log[1]["status"] == "FAILED"
    assert executor.execution_log[2]["status"] == "COMPLETED"


def test_worker_capacity_released_after_retry_success():
    workflow = Workflow("workflow_retry_4")

    task_a = Task("A", "Task A", max_retries=2)
    workflow.add_task(task_a)

    worker_pool = WorkerPool()
    worker = Worker("worker_1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = CedarInspiredScheduler(worker_pool)

    call_count = 0

    def fail_once_runner(task):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("Temporary failure")

        return "Success"

    executor = WorkflowExecutor(
        task_runner=fail_once_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task_a.status == "COMPLETED"
    assert task_a.attempts == 2
    assert worker.available_slots() == 1
    assert worker.running_tasks == []
def test_downstream_reexecutes_when_upstream_output_changes():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    output_a = ["A-old", "A-new"]
    call_counts = {"A": 0, "B": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            index = call_counts["A"] - 1
            return output_a[index]

        return "B-output"

    executor = WorkflowExecutor(task_runner=task_runner)

    executor.execute_workflow(workflow)

    assert call_counts["B"] == 1

    task_a.reset_for_reexecution()
    executor.execute_workflow(workflow)

    assert task_a.result == "A-new"
    assert task_b.status == "COMPLETED"
    assert call_counts["B"] == 2
def test_downstream_not_reexecuted_when_upstream_output_unchanged():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    call_counts = {"A": 0, "B": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            return "Same-output"

        return "B-output"

    executor = WorkflowExecutor(task_runner=task_runner)

    executor.execute_workflow(workflow)

    assert call_counts["B"] == 1

    task_a.reset_for_reexecution()
    executor.execute_workflow(workflow)

    assert task_a.result == "Same-output"
    assert task_b.status == "COMPLETED"
    assert call_counts["B"] == 1
def test_recovery_propagates_through_downstream_chain():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["B"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    output_a = ["A-old", "A-new"]
    call_counts = {"A": 0, "B": 0, "C": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            index = call_counts["A"] - 1
            return output_a[index]

        if task.task_id == "B":
            return f"B-output-{call_counts['B']}"

        return f"C-output-{call_counts['C']}"

    executor = WorkflowExecutor(task_runner=task_runner)

    executor.execute_workflow(workflow)

    assert call_counts["B"] == 1
    assert call_counts["C"] == 1

    task_a.reset_for_reexecution()
    executor.execute_workflow(workflow)

    assert call_counts["A"] == 2
    assert call_counts["B"] == 2
    assert call_counts["C"] == 2

    assert task_a.result == "A-new"
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"
def test_recovery_with_branching_dependencies():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["A"])
    task_d = Task("D", "Task D", dependencies=["B", "C"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)
    workflow.add_task(task_d)

    output_a = ["A-old", "A-new"]
    call_counts = {"A": 0, "B": 0, "C": 0, "D": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            return output_a[call_counts["A"] - 1]

        return f"{task.task_id}-output-{call_counts[task.task_id]}"

    executor = WorkflowExecutor(task_runner=task_runner)

    executor.execute_workflow(workflow)

    assert call_counts == {"A": 1, "B": 1, "C": 1, "D": 1}

    task_a.reset_for_reexecution()
    executor.execute_workflow(workflow)

    assert call_counts == {"A": 2, "B": 2, "C": 2, "D": 2}

    assert task_d.status == "COMPLETED"
def test_recovery_works_across_multiple_cycles():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    outputs_a = ["A-v1", "A-v2", "A-v3"]
    call_counts = {"A": 0, "B": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            index = call_counts["A"] - 1
            return outputs_a[index]

        return f"B-output-{call_counts['B']}"

    executor = WorkflowExecutor(task_runner=task_runner)

    # Initial execution
    executor.execute_workflow(workflow)

    assert task_a.result == "A-v1"
    assert call_counts["B"] == 1

    # First recovery cycle
    task_a.reset_for_reexecution()
    executor.execute_workflow(workflow)

    assert task_a.result == "A-v2"
    assert task_b.status == "COMPLETED"
    assert call_counts["B"] == 2

    # Second recovery cycle
    task_a.reset_for_reexecution()
    executor.execute_workflow(workflow)

    assert task_a.result == "A-v3"
    assert task_b.status == "COMPLETED"
    assert call_counts["B"] == 3

    # Final verification
    assert call_counts == {"A": 3, "B": 3}
def test_independent_tasks_continue_when_multiple_tasks_fail():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A", max_retries=0)
    task_b = Task("B", "Task B", max_retries=0)
    task_c = Task("C", "Task C", max_retries=0)

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    def task_runner(task):
        if task.task_id == "A":
            raise RuntimeError("Task A failed")

        if task.task_id == "B":
            raise RuntimeError("Task B failed")

        return "Task C completed successfully"

    executor = WorkflowExecutor(task_runner=task_runner)

    with pytest.raises(RuntimeError):
        executor.execute_workflow(workflow)

    assert task_a.status == "FAILED"
    assert task_b.status == "FAILED"
    assert task_c.status == "COMPLETED"

    assert task_a.attempts == 1
    assert task_b.attempts == 1
    assert task_c.result == "Task C completed successfully"
def test_workers_are_released_after_successful_execution():
    worker_pool = WorkerPool()

    worker_pool.add_worker(
        Worker(worker_id="W1", capacity=2)
    )
    worker_pool.add_worker(
        Worker(worker_id="W2", capacity=2)
    )

    scheduler = RoundRobinScheduler(worker_pool)

    workflow = Workflow("W1")

    workflow.add_task(Task("A", "Task A"))
    workflow.add_task(Task("B", "Task B"))
    workflow.add_task(Task("C", "Task C"))

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    for worker in worker_pool.workers.values():
        assert worker.running_tasks == []
def test_workers_are_released_after_permanent_failure():
    worker_pool = WorkerPool()

    worker_pool.add_worker(
        Worker(worker_id="W1", capacity=1)
    )

    scheduler = RoundRobinScheduler(worker_pool)

    workflow = Workflow("W1")

    task = Task(
        "A",
        "Task A",
        max_retries=0,
    )

    workflow.add_task(task)

    def task_runner(task):
        raise RuntimeError("Permanent failure")

    executor = WorkflowExecutor(
        task_runner=task_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    with pytest.raises(RuntimeError, match="Permanent failure"):
        executor.execute_workflow(workflow)

    assert task.status == "FAILED"
    assert task.attempts == 1

    for worker in worker_pool.workers.values():
        assert worker.running_tasks == []
def test_recovery_cycle_gets_fresh_retry_budget():
    workflow = Workflow("W1")

    task = Task(
        "A",
        "Task A",
        max_retries=1,
    )

    workflow.add_task(task)

    call_count = {"A": 0}

    def task_runner(task):
        call_count["A"] += 1

        if call_count["A"] == 2:
            raise RuntimeError("Temporary recovery failure")

        return f"output-{call_count['A']}"

    executor = WorkflowExecutor(task_runner=task_runner)

    # Initial execution
    executor.execute_workflow(workflow)

    assert task.result == "output-1"

    # Start a recovery cycle
    task.reset_for_reexecution()

    executor.execute_workflow(workflow)

    # The first recovery attempt fails, but its retry succeeds.
    assert task.status == "COMPLETED"
    assert task.result == "output-3"
    assert task.attempts == 3
    assert call_count["A"] == 3
def test_permanent_failure_blocks_downstream_but_independent_task_completes():
    import pytest

    from src.workflow.task import Task
    from src.workflow.workflow import Workflow
    from src.workflow.executor import WorkflowExecutor
    from src.workers.worker import Worker
    from src.workers.worker_pool import WorkerPool
    from src.schedulers.cedar_inspired import CedarInspiredScheduler

    # Create workflow
    workflow = Workflow("permanent_failure_workflow")

    # Create tasks
    task_a = Task(
        task_id="A",
        name="Task A",
        failure_probability=1.0,
        max_retries=0,
    )

    task_b = Task(
        task_id="B",
        name="Task B",
        dependencies=["A"],
    )

    task_c = Task(
        task_id="C",
        name="Task C",
        dependencies=["B"],
    )

    task_d = Task(
        task_id="D",
        name="Independent Task",
    )

    # Add tasks
    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)
    workflow.add_task(task_d)

    # Create worker pool
    worker_pool = WorkerPool()
    worker = Worker("worker_1", capacity=1)
    worker_pool.add_worker(worker)

    # Create scheduler and executor
    scheduler = CedarInspiredScheduler(worker_pool)

    def task_runner(task):
        return f"Result of {task.task_id}"

    executor = WorkflowExecutor(
        task_runner=task_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    # Execute workflow; Task A should fail permanently
    with pytest.raises(RuntimeError):
        executor.execute_workflow(workflow)

    # Verify task states
    assert task_a.status == "FAILED"
    assert task_b.status == "BLOCKED"
    assert task_c.status == "BLOCKED"
    assert task_d.status == "COMPLETED"

    # Verify worker resources are released
    assert worker.available_slots() == 1
    assert worker.running_tasks == []
def test_recovery_with_retry_triggers_downstream_reexecution():
    workflow = Workflow("recovery_retry_workflow")

    task_a = Task(
        "A",
        "Task A",
        max_retries=1,
    )
    task_b = Task(
        "B",
        "Task B",
        dependencies=["A"],
    )

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    call_counts = {"A": 0, "B": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            if call_counts["A"] == 1:
                return "Old output"

            if call_counts["A"] == 2:
                raise RuntimeError("Temporary recovery failure")

            return "New output"

        return f"B-output-{call_counts['B']}"

    executor = WorkflowExecutor(task_runner=task_runner)

    # Initial execution
    executor.execute_workflow(workflow)

    assert task_a.result == "Old output"
    assert call_counts["B"] == 1

    # Start a recovery cycle
    task_a.reset_for_reexecution()

    # Task A fails once, retries, and succeeds
    executor.execute_workflow(workflow)

    # Verify Task A succeeded after retry
    assert task_a.status == "COMPLETED"
    assert task_a.result == "New output"
    assert task_a.attempts == 3

    # Task B should execute again because A's output changed
    assert task_b.status == "COMPLETED"
    assert call_counts["B"] == 2

def test_worker_assignment_log_records_carbon_estimates():
    workflow = Workflow("workflow_carbon_log")

    task = Task(
        task_id="A",
        name="Carbon Tracking Task",
        runtime=10.0,
    )

    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker = Worker(
        worker_id="worker_1",
        capacity=1,
        processing_speed=2.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
    )

    worker_pool.add_worker(worker)

    scheduler = CedarInspiredScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert len(executor.worker_assignment_log) == 1

    assignment = executor.worker_assignment_log[0]

    assert assignment["task_id"] == "A"
    assert assignment["worker_id"] == "worker_1"
    assert assignment["attempt"] == 1

    assert assignment["estimated_execution_time"] == 5.0

    assert abs(
        assignment["estimated_energy_kwh"] - 0.0002777778
    ) < 1e-9

    assert abs(
        assignment["estimated_carbon_g"] - 0.1111111
    ) < 1e-6
def test_failure_aware_carbon_scheduler_with_executor():
    workflow = Workflow("failure_aware_workflow")

    task = Task(
        task_id="A",
        name="Task A",
        runtime=10.0
    )

    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker_1 = Worker(
        worker_id="worker_1",
        capacity=1,
        processing_speed=1.0,
        power_watts=300.0,
        carbon_intensity_g_per_kwh=500.0,
        failure_probability=0.8
    )

    worker_2 = Worker(
        worker_id="worker_2",
        capacity=1,
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
        failure_probability=0.1
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool
    )

    executor.execute_workflow(workflow)

    assert task.status == "COMPLETED"

    assert (
        executor.worker_assignment_log[0]["worker_id"]
        == "worker_2"
    )

    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == []
def test_multiple_tasks_complete_with_failure_aware_scheduler():
    workflow = Workflow("multi_task_workflow")

    task_a = Task("A", "Task A", runtime=5.0)
    task_b = Task("B", "Task B", runtime=8.0)
    task_c = Task("C", "Task C", runtime=3.0)

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=2)
    worker_2 = Worker("W2", capacity=2)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task_a.status == "COMPLETED"
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"


def test_scheduler_selects_worker_with_lowest_score_for_multiple_tasks():
    workflow = Workflow("score_selection_workflow")

    task_a = Task("A", "Task A", runtime=10.0)
    task_b = Task("B", "Task B", runtime=10.0)

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        capacity=2,
        processing_speed=1.0,
        power_watts=300.0,
        carbon_intensity_g_per_kwh=500.0,
        failure_probability=0.8,
    )

    worker_2 = Worker(
        "W2",
        capacity=2,
        processing_speed=1.0,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
        failure_probability=0.1,
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assigned_workers = [
        entry["worker_id"]
        for entry in executor.worker_assignment_log
    ]

    assert assigned_workers == ["W2", "W2"]


def test_scheduler_respects_worker_capacity():
    workflow = Workflow("capacity_workflow")

    for task_id in ["A", "B", "C", "D"]:
        workflow.add_task(
            Task(task_id, f"Task {task_id}", runtime=5.0)
        )

    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=2)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert all(
        task.status == "COMPLETED"
        for task in workflow.tasks.values()
    )

    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == []


def test_workers_are_released_after_multiple_task_execution():
    workflow = Workflow("release_workers_workflow")

    workflow.add_task(Task("A", "Task A"))
    workflow.add_task(Task("B", "Task B"))
    workflow.add_task(Task("C", "Task C"))

    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=2)
    worker_2 = Worker("W2", capacity=2)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == []

    assert worker_1.available_slots() == 2
    assert worker_2.available_slots() == 2


def test_dependent_tasks_execute_with_failure_aware_scheduler():
    workflow = Workflow("dependency_workflow")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["B"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=2)
    worker_2 = Worker("W2", capacity=2)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    execution_order = executor.execute_workflow(workflow)

    assert execution_order == ["A", "B", "C"]

    assert task_a.status == "COMPLETED"
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"
# Batch 2: Failure handling and recovery


def test_failure_aware_scheduler_task_succeeds_after_retry():
    workflow = Workflow("retry_success_workflow")

    task = Task(
        "A",
        "Task A",
        max_retries=2,
    )
    workflow.add_task(task)

    worker_pool = WorkerPool()
    worker = Worker("W1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    call_count = 0

    def fail_once_runner(task):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("Temporary failure")

        return "Success after retry"

    executor = WorkflowExecutor(
        task_runner=fail_once_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task.status == "COMPLETED"
    assert task.attempts == 2
    assert call_count == 2
    assert task.result == "Success after retry"


def test_worker_capacity_released_after_failed_attempt():
    workflow = Workflow("retry_capacity_workflow")

    task = Task(
        "A",
        "Task A",
        max_retries=1,
    )
    workflow.add_task(task)

    worker_pool = WorkerPool()
    worker = Worker("W1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    call_count = 0

    def fail_once_runner(task):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("Temporary failure")

        return "Success"

    executor = WorkflowExecutor(
        task_runner=fail_once_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task.status == "COMPLETED"
    assert task.attempts == 2
    assert worker.running_tasks == []
    assert worker.available_slots() == 1


def test_failure_aware_scheduler_task_fails_after_all_retries():
    workflow = Workflow("permanent_failure_workflow")

    task = Task(
        "A",
        "Task A",
        max_retries=2,
    )
    workflow.add_task(task)

    worker_pool = WorkerPool()
    worker = Worker("W1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    call_count = 0

    def always_fail_runner(task):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("Permanent failure")

    executor = WorkflowExecutor(
        task_runner=always_fail_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    with pytest.raises(RuntimeError, match="Permanent failure"):
        executor.execute_workflow(workflow)

    assert task.status == "FAILED"
    assert task.attempts == 3
    assert call_count == 3
    assert worker.running_tasks == []
    assert worker.available_slots() == 1


def test_independent_tasks_continue_when_one_task_fails():
    workflow = Workflow("independent_failure_workflow")

    task_a = Task(
        "A",
        "Task A",
        max_retries=0,
    )
    task_b = Task(
        "B",
        "Task B",
        max_retries=0,
    )
    task_c = Task(
        "C",
        "Task C",
        max_retries=0,
    )

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    worker_pool = WorkerPool()
    worker = Worker("W1", capacity=3)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    def task_runner(task):
        if task.task_id == "A":
            raise RuntimeError("Task A failed")

        return f"Result of {task.task_id}"

    executor = WorkflowExecutor(
        task_runner=task_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    with pytest.raises(RuntimeError, match="Task A failed"):
        executor.execute_workflow(workflow)

    assert task_a.status == "FAILED"
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"
    assert worker.running_tasks == []


def test_downstream_task_reexecutes_after_upstream_output_changes():
    workflow = Workflow("downstream_recovery_workflow")

    task_a = Task("A", "Task A")
    task_b = Task(
        "B",
        "Task B",
        dependencies=["A"],
    )

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    worker_pool = WorkerPool()
    worker = Worker("W1", capacity=2)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    output_a = ["Old output", "New output"]
    call_counts = {"A": 0, "B": 0}

    def task_runner(task):
        call_counts[task.task_id] += 1

        if task.task_id == "A":
            index = call_counts["A"] - 1
            return output_a[index]

        return f"B-output-{call_counts['B']}"

    executor = WorkflowExecutor(
        task_runner=task_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    # First execution
    executor.execute_workflow(workflow)

    assert task_a.result == "Old output"
    assert call_counts["B"] == 1

    # Start a recovery cycle
    task_a.reset_for_reexecution()

    # Execute the workflow again
    executor.execute_workflow(workflow)

    assert task_a.result == "New output"
    assert task_b.status == "COMPLETED"
    assert call_counts["A"] == 2
    assert call_counts["B"] == 2
    assert worker.running_tasks == []
# Batch 3: Scheduler behavior under different conditions


def test_scheduler_prefers_worker_with_lower_failure_probability():
    workflow = Workflow("failure_probability_workflow")

    task = Task("A", "Task A", runtime=10.0)
    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        capacity=1,
        failure_probability=0.9,
    )

    worker_2 = Worker(
        "W2",
        capacity=1,
        failure_probability=0.1,
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=1.0,
    )

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert executor.worker_assignment_log[0]["worker_id"] == "W2"
    assert task.status == "COMPLETED"


def test_scheduler_prefers_lower_carbon_worker():
    workflow = Workflow("carbon_preference_workflow")

    task = Task("A", "Task A", runtime=10.0)
    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        capacity=1,
        power_watts=300.0,
        carbon_intensity_g_per_kwh=500.0,
    )

    worker_2 = Worker(
        "W2",
        capacity=1,
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
    )

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=1.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert executor.worker_assignment_log[0]["worker_id"] == "W2"
    assert task.status == "COMPLETED"


def test_scheduler_raises_when_all_workers_are_busy():
    workflow = Workflow("all_workers_busy_workflow")

    task = Task("A", "Task A")
    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=1)
    worker.assign_task("another_task")
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        executor.execute_workflow(workflow)

    assert task.status == "READY"
    assert worker.running_tasks == ["another_task"]


def test_failure_aware_scheduler_records_carbon_estimates():
    workflow = Workflow("carbon_estimate_workflow")

    task = Task(
        "A",
        "Carbon Task",
        runtime=10.0,
    )
    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker = Worker(
        "W1",
        capacity=1,
        processing_speed=2.0,
        power_watts=200.0,
        carbon_intensity_g_per_kwh=400.0,
    )

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert len(executor.worker_assignment_log) == 1

    assignment = executor.worker_assignment_log[0]

    assert assignment["task_id"] == "A"
    assert assignment["worker_id"] == "W1"
    assert assignment["attempt"] == 1

    assert assignment["estimated_execution_time"] == 5.0

    assert abs(
        assignment["estimated_energy_kwh"] - 0.0002777778
    ) < 1e-9

    assert abs(
        assignment["estimated_carbon_g"] - 0.1111111
    ) < 1e-6


def test_retry_uses_failure_aware_scheduler_again():
    workflow = Workflow("retry_scheduler_workflow")

    task = Task(
        "A",
        "Task A",
        max_retries=1,
    )
    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=1)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    call_count = 0

    def fail_once_runner(task):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("Temporary failure")

        return "Success after retry"

    executor = WorkflowExecutor(
        task_runner=fail_once_runner,
        scheduler=scheduler,
        worker_pool=worker_pool,
    )

    executor.execute_workflow(workflow)

    assert task.status == "COMPLETED"
    assert task.attempts == 2
    assert call_count == 2

    assert len(executor.worker_assignment_log) == 2

    assert executor.worker_assignment_log[0]["attempt"] == 1
    assert executor.worker_assignment_log[1]["attempt"] == 2

    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == []
# Batch 4: Edge cases and scheduler robustness


def test_scheduler_raises_error_when_worker_pool_is_empty():
    worker_pool = WorkerPool()

    scheduler = FailureAwareCarbonScheduler(worker_pool)
    task = Task("A", "Task A")

    with pytest.raises(
        ValueError,
        match="No worker has available capacity",
    ):
        scheduler.schedule(task)


def test_scheduler_rejects_negative_time_weight():
    worker_pool = WorkerPool()

    with pytest.raises(ValueError, match="Time weight cannot be negative"):
        FailureAwareCarbonScheduler(
            worker_pool,
            time_weight=-1.0,
        )


def test_scheduler_rejects_negative_carbon_weight():
    worker_pool = WorkerPool()

    with pytest.raises(ValueError, match="Carbon weight cannot be negative"):
        FailureAwareCarbonScheduler(
            worker_pool,
            carbon_weight=-1.0,
        )


def test_scheduler_selects_first_worker_when_scores_are_equal():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=1)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=1.0,
        carbon_weight=1.0,
        load_weight=1.0,
        failure_weight=1.0,
    )

    task = Task("A", "Task A")

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W1"
    assert "A" in worker_1.running_tasks
    assert worker_2.running_tasks == []


def test_scheduler_skips_busy_worker_and_selects_available_worker():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=1)

    worker_1.assign_task("existing_task")

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task = Task("A", "Task A")

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W2"
    assert worker_1.running_tasks == ["existing_task"]
    assert worker_2.running_tasks == ["A"]
# Batch 5: Scheduler configuration and weight validation


def test_scheduler_rejects_negative_load_weight():
    worker_pool = WorkerPool()

    with pytest.raises(
        ValueError,
        match="Load weight cannot be negative",
    ):
        FailureAwareCarbonScheduler(
            worker_pool,
            load_weight=-1.0,
        )


def test_scheduler_rejects_negative_failure_weight():
    worker_pool = WorkerPool()

    with pytest.raises(
        ValueError,
        match="Failure weight cannot be negative",
    ):
        FailureAwareCarbonScheduler(
            worker_pool,
            failure_weight=-1.0,
        )


def test_scheduler_accepts_zero_weights():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=1)

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A")

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W1"


def test_scheduler_uses_time_weight_to_prefer_faster_worker():
    worker_pool = WorkerPool()

    worker_1 = Worker(
        "W1",
        capacity=1,
        processing_speed=1.0,
    )

    worker_2 = Worker(
        "W2",
        capacity=1,
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


def test_scheduler_uses_load_weight_to_prefer_less_loaded_worker():
    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=2)
    worker_2 = Worker("W2", capacity=2)

    worker_1.assign_task("existing_task")

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

    selected_worker = scheduler.schedule(task)

    assert selected_worker.worker_id == "W2"
    assert worker_1.running_tasks == ["existing_task"]
    assert worker_2.running_tasks == ["A"]
# Batch 6: Scheduler robustness and performance


def test_scheduler_score_increases_with_carbon_emissions():
    worker_pool = WorkerPool()

    low_carbon_worker = Worker(
        "W1",
        power_watts=100.0,
        carbon_intensity_g_per_kwh=100.0,
    )

    high_carbon_worker = Worker(
        "W2",
        power_watts=300.0,
        carbon_intensity_g_per_kwh=500.0,
    )

    worker_pool.add_worker(low_carbon_worker)
    worker_pool.add_worker(high_carbon_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=1.0,
        load_weight=0.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A", runtime=10.0)

    low_carbon_score = scheduler.calculate_score(
        task,
        low_carbon_worker,
    )

    high_carbon_score = scheduler.calculate_score(
        task,
        high_carbon_worker,
    )

    assert high_carbon_score > low_carbon_score


def test_scheduler_score_increases_with_worker_load():
    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=2)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=1.0,
        failure_weight=0.0,
    )

    task = Task("A", "Task A")

    initial_score = scheduler.calculate_score(task, worker)

    worker.assign_task("existing_task")

    loaded_score = scheduler.calculate_score(task, worker)

    assert loaded_score > initial_score


def test_scheduler_score_increases_with_failure_probability():
    worker_pool = WorkerPool()

    low_failure_worker = Worker(
        "W1",
        failure_probability=0.1,
    )

    high_failure_worker = Worker(
        "W2",
        failure_probability=0.8,
    )

    worker_pool.add_worker(low_failure_worker)
    worker_pool.add_worker(high_failure_worker)

    scheduler = FailureAwareCarbonScheduler(
        worker_pool,
        time_weight=0.0,
        carbon_weight=0.0,
        load_weight=0.0,
        failure_weight=1.0,
    )

    task = Task("A", "Task A", runtime=10.0)

    low_failure_score = scheduler.calculate_score(
        task,
        low_failure_worker,
    )

    high_failure_score = scheduler.calculate_score(
        task,
        high_failure_worker,
    )

    assert high_failure_score > low_failure_score


def test_scheduler_respects_worker_capacity_for_multiple_assignments():
    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=2)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task_1 = Task("A", "Task A")
    task_2 = Task("B", "Task B")

    selected_worker_1 = scheduler.schedule(task_1)
    selected_worker_2 = scheduler.schedule(task_2)

    assert selected_worker_1.worker_id == "W1"
    assert selected_worker_2.worker_id == "W1"

    assert len(worker.running_tasks) == 2
    assert "A" in worker.running_tasks
    assert "B" in worker.running_tasks

    assert worker.is_available() is False


def test_scheduler_can_reuse_worker_after_task_release():
    worker_pool = WorkerPool()

    worker = Worker("W1", capacity=1)
    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    task_1 = Task("A", "Task A")

    selected_worker_1 = scheduler.schedule(task_1)

    assert selected_worker_1.worker_id == "W1"
    assert worker.is_available() is False

    worker.release_task("A")

    assert worker.is_available() is True

    task_2 = Task("B", "Task B")

    selected_worker_2 = scheduler.schedule(task_2)

    assert selected_worker_2.worker_id == "W1"
    assert worker.running_tasks == ["B"]
def test_executor_detects_failed_worker():
    task = Task(
        task_id="A",
        name="Task A",
        status="READY",
        max_retries=0
    )

    worker = Worker(
        worker_id="W1",
        capacity=1
    )

    worker.fail()

    executor = WorkflowExecutor()

    with pytest.raises(
        RuntimeError,
        match="Worker W1 failed while executing task A"
    ):
        executor.execute_task(task, worker)

    assert task.status == "FAILED"
    assert task.attempts == 1
def test_scheduler_skips_failed_worker():
    workflow = Workflow("failed_worker_scheduler")

    task = Task(
        task_id="A",
        name="Task A"
    )

    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker_1 = Worker("W1", capacity=1)
    worker_2 = Worker("W2", capacity=1)

    worker_1.fail()

    worker_pool.add_worker(worker_1)
    worker_pool.add_worker(worker_2)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool
    )

    executor.execute_workflow(workflow)

    assert task.status == "COMPLETED"

    assert (
        executor.worker_assignment_log[0]["worker_id"]
        == "W2"
    )

    assert worker_1.running_tasks == []
    assert worker_2.running_tasks == []
def test_executor_does_not_use_failed_worker_with_available_capacity():
    workflow = Workflow("failed_worker_with_capacity")

    task = Task(
        task_id="A",
        name="Task A"
    )

    workflow.add_task(task)

    worker_pool = WorkerPool()

    worker = Worker(
        worker_id="W1",
        capacity=3
    )

    worker.fail()

    worker_pool.add_worker(worker)

    scheduler = FailureAwareCarbonScheduler(worker_pool)

    executor = WorkflowExecutor(
        scheduler=scheduler,
        worker_pool=worker_pool
    )

    with pytest.raises(
        ValueError,
        match="No worker has available capacity"
    ):
        executor.execute_workflow(workflow)

    assert task.status == "READY"
    assert worker.is_failed
    assert worker.available_slots() == 0
    assert worker.running_tasks == []