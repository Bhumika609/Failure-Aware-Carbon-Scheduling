import pytest
from src.workflow.task import Task
from src.workflow.workflow import Workflow
from src.workflow.recovery import RecoveryPolicy
def test_unchanged_output_has_no_affected_tasks():
    workflow = Workflow("recovery_1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    policy = RecoveryPolicy()

    affected = policy.get_affected_tasks(
        workflow,
        "A",
        "same output",
        "same output",
    )

    assert affected == []


def test_changed_output_identifies_downstream_tasks():
    workflow = Workflow("recovery_2")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["B"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    policy = RecoveryPolicy()

    affected = policy.get_affected_tasks(
        workflow,
        "A",
        "old output",
        "new output",
    )

    assert [task.task_id for task in affected] == ["B", "C"]


def test_unknown_task_id_raises_error():
    workflow = Workflow("recovery_3")
    policy = RecoveryPolicy()

    import pytest

    with pytest.raises(ValueError, match="Unknown task ID"):
        policy.get_affected_tasks(
            workflow,
            "UNKNOWN",
            "old",
            "new",
        )


def test_uncertain_output_comparison_triggers_recovery():
    workflow = Workflow("recovery_4")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    class Uncomparable:
        def __eq__(self, other):
            raise RuntimeError("Cannot compare")

    policy = RecoveryPolicy()

    affected = policy.get_affected_tasks(
        workflow,
        "A",
        Uncomparable(),
        Uncomparable(),
    )

    assert [task.task_id for task in affected] == ["B"]
def test_recover_resets_downstream_tasks_when_output_changes():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    task_b.status = "COMPLETED"
    task_b.result = "Old output"

    policy = RecoveryPolicy()

    reset_tasks = policy.recover(
        workflow,
        "A",
        "Old source output",
        "New source output",
    )

    assert [task.task_id for task in reset_tasks] == ["B"]
    assert task_b.status == "PENDING"
    assert task_b.previous_result == "Old output"
    assert task_b.result is None


def test_recover_does_not_reset_when_output_is_unchanged():
    workflow = Workflow("W1")

    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)

    task_b.status = "COMPLETED"
    task_b.result = "Existing output"

    policy = RecoveryPolicy()

    reset_tasks = policy.recover(
        workflow,
        "A",
        "Same output",
        "Same output",
    )

    assert reset_tasks == []
    assert task_b.status == "COMPLETED"
    assert task_b.result == "Existing output"
def test_recover_resets_all_completed_downstream_tasks():
    workflow = Workflow("recovery_chain")

    # Create a chain: A -> B -> C
    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["B"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    # Simulate previously completed downstream tasks
    task_b.status = "COMPLETED"
    task_b.result = "Old B output"

    task_c.status = "COMPLETED"
    task_c.result = "Old C output"

    policy = RecoveryPolicy()

    # Task A produces a changed output
    reset_tasks = policy.recover(
        workflow,
        "A",
        "Old A output",
        "New A output",
    )

    # Both downstream tasks should be reset
    assert [task.task_id for task in reset_tasks] == ["B", "C"]

    assert task_b.status == "PENDING"
    assert task_b.previous_result == "Old B output"
    assert task_b.result is None

    assert task_c.status == "PENDING"
    assert task_c.previous_result == "Old C output"
    assert task_c.result is None
def test_unchanged_output_preserves_all_downstream_tasks():
    workflow = Workflow("recovery_unchanged_chain")

    # Create a chain: A -> B -> C
    task_a = Task("A", "Task A")
    task_b = Task("B", "Task B", dependencies=["A"])
    task_c = Task("C", "Task C", dependencies=["B"])

    workflow.add_task(task_a)
    workflow.add_task(task_b)
    workflow.add_task(task_c)

    # Simulate previously completed downstream tasks
    task_b.status = "COMPLETED"
    task_b.result = "Existing B output"

    task_c.status = "COMPLETED"
    task_c.result = "Existing C output"

    policy = RecoveryPolicy()

    # Task A produces the same output again
    reset_tasks = policy.recover(
        workflow,
        "A",
        "Same A output",
        "Same A output",
    )

    # No downstream tasks should be reset
    assert reset_tasks == []

    # Both tasks should remain completed
    assert task_b.status == "COMPLETED"
    assert task_c.status == "COMPLETED"

    # Their results should remain unchanged
    assert task_b.result == "Existing B output"
    assert task_c.result == "Existing C output"