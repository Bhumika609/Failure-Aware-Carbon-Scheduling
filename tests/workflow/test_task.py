from src.workflow.task import Task
import pytest


def test_task_creation():
    task = Task(
        task_id="A",
        name="Collect information"
    )

    assert task.task_id == "A"
    assert task.name == "Collect information"
    assert task.dependencies == []
    assert task.runtime == 1.0
    assert task.status == "PENDING"

def test_outputs_changed_detects_equal_outputs():
    task = Task("A", "Task A")

    assert task.outputs_changed("Result A", "Result A") is False


def test_outputs_changed_detects_different_outputs():
    task = Task("A", "Task A")

    assert task.outputs_changed("Result A", "Result B") is True


def test_outputs_changed_handles_comparison_failure():
    task = Task("A", "Task A")

    class Uncomparable:
        def __eq__(self, other):
            raise RuntimeError("Cannot compare")

    assert task.outputs_changed(
        Uncomparable(),
        Uncomparable(),
    ) is True
def test_reset_completed_task_for_reexecution():
    task = Task(task_id="A", name="Task A")
    task.status = "COMPLETED"
    task.result = "old output"
    task.attempts = 2

    task.reset_for_reexecution()

    assert task.status == "PENDING"
    assert task.previous_result == "old output"
    assert task.result is None
    assert task.attempts == 2


def test_reset_non_completed_task_raises_error():
    task = Task(task_id="A", name="Task A")

    with pytest.raises(ValueError):
        task.reset_for_reexecution()