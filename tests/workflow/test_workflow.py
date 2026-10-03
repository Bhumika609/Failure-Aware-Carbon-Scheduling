import pytest
from src.workflow.task import Task
from src.workflow.workflow import Workflow


def test_add_tasks():
    workflow = Workflow("workflow_1")

    task_a = Task("A", "Collect information")
    task_b = Task(
        "B",
        "Analyze information",
        dependencies=["A"]
    )

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    assert len(workflow.tasks) == 2
    assert workflow.tasks["A"] == task_a
    assert workflow.tasks["B"] == task_b


def test_valid_workflow():
    workflow = Workflow("workflow_1")

    workflow.add_task(Task("A", "Collect information"))
    workflow.add_task(
        Task("B", "Analyze information", dependencies=["A"])
    )
    workflow.add_task(
        Task("C", "Generate report", dependencies=["B"])
    )

    assert workflow.validate() is True


def test_duplicate_task_id():
    workflow = Workflow("workflow_1")

    workflow.add_task(Task("A", "Task A"))

    with pytest.raises(ValueError, match="Duplicate task ID"):
        workflow.add_task(Task("A", "Another task A"))


def test_unknown_dependency():
    workflow = Workflow("workflow_1")

    workflow.add_task(
        Task("A", "Analyze information", dependencies=["X"])
    )

    with pytest.raises(ValueError, match="unknown task"):
        workflow.validate()


def test_cycle_detection():
    workflow = Workflow("workflow_1")

    workflow.add_task(
        Task("A", "Task A", dependencies=["B"])
    )
    workflow.add_task(
        Task("B", "Task B", dependencies=["A"])
    )

    with pytest.raises(ValueError, match="cycle"):
        workflow.validate()
def test_initial_ready_tasks():
    workflow = Workflow("workflow_1")

    task_a = Task("A", "Collect information")
    task_b = Task(
        "B",
        "Analyze information",
        dependencies=["A"]
    )
    task_c = Task(
        "C",
        "Generate report",
        dependencies=["B"]
    )

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    newly_ready = workflow.refresh_ready_tasks()

    assert [task.task_id for task in newly_ready] == ["A"]
    assert task_a.status == "READY"
    assert task_b.status == "PENDING"
    assert task_c.status == "PENDING"


def test_ready_tasks_after_dependency_completion():
    workflow = Workflow("workflow_1")

    task_a = Task("A", "Collect information")
    task_b = Task(
        "B",
        "Analyze information",
        dependencies=["A"]
    )
    task_c = Task(
        "C",
        "Generate report",
        dependencies=["B"]
    )

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    workflow.refresh_ready_tasks()
    task_a.transition_to("RUNNING")
    task_a.transition_to("COMPLETED")

    newly_ready = workflow.refresh_ready_tasks()

    assert [task.task_id for task in newly_ready] == ["B"]
    assert task_b.status == "READY"
    assert task_c.status == "PENDING"


def test_parallel_ready_tasks():
    workflow = Workflow("workflow_1")

    task_a = Task("A", "Collect information")
    task_b = Task("B", "Analyze financial data")
    task_c = Task("C", "Analyze technical data")

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    newly_ready = workflow.refresh_ready_tasks()

    assert [task.task_id for task in newly_ready] == [
        "A",
        "B",
        "C"
    ]

def test_get_downstream_tasks_returns_direct_and_indirect_tasks():
    workflow = Workflow("workflow_downstream")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["A"])
    task_d = Task("D", "Task D", dependencies=["B", "C"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)
    workflow.add_task(task_d)

    downstream = workflow.get_downstream_tasks("A")

    assert [task.task_id for task in downstream] == ["B", "C", "D"]


def test_get_downstream_tasks_returns_empty_for_leaf_task():
    workflow = Workflow("workflow_downstream_leaf")

    task_a = Task("A", "Task A")
    workflow.add_task(task_a)

    assert workflow.get_downstream_tasks("A") == []


def test_get_downstream_tasks_rejects_unknown_task():
    workflow = Workflow("workflow_downstream_unknown")

    import pytest

    with pytest.raises(ValueError, match="Unknown task ID"):
        workflow.get_downstream_tasks("UNKNOWN")
def test_reset_downstream_tasks():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["B"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    for task in [task_b, task_c]:
        task.status = "COMPLETED"
        task.result = f"Output {task.task_id}"

    reset_tasks = workflow.reset_downstream_tasks("A")

    assert [task.task_id for task in reset_tasks] == ["B", "C"]
    assert task_b.status == "PENDING"
    assert task_c.status == "PENDING"
    assert task_b.previous_result == "Output B"
    assert task_c.previous_result == "Output C"


def test_reset_downstream_tasks_does_not_reset_incomplete_tasks():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    reset_tasks = workflow.reset_downstream_tasks("A")

    assert reset_tasks == []
    assert task_b.status == "PENDING"


def test_reset_downstream_tasks_unknown_task():
    workflow = Workflow("W1")

    with pytest.raises(ValueError):
        workflow.reset_downstream_tasks("UNKNOWN")
def test_block_downstream_tasks():
    from src.workflow.workflow import Workflow
    from src.workflow.task import Task

    # Create a workflow
    workflow = Workflow(workflow_id="test_workflow")

    # Create tasks with dependencies
    task_a = Task(task_id="A", name="Task A")
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
    task_d = Task(task_id="D", name="Independent Task")

    # Add tasks to the workflow
    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)
    workflow.add_task(task_d)

    # Make Task B READY
    task_b.transition_to("READY")

    # Permanently fail Task A
    task_a.transition_to("READY")
    task_a.transition_to("RUNNING")
    task_a.transition_to("FAILED")

    # Block all downstream tasks of Task A
    blocked_tasks = workflow.block_downstream_tasks("A")

    # Verify that B and C are blocked
    assert task_b.status == "BLOCKED"
    assert task_c.status == "BLOCKED"

    # Verify that independent Task D is unaffected
    assert task_d.status == "PENDING"

    # Verify the returned list
    blocked_ids = [task.task_id for task in blocked_tasks]

    assert set(blocked_ids) == {"B", "C"}