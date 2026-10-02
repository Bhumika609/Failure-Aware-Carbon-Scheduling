import pytest

from src.workflow.task import Task


def test_valid_transitions():
    task = Task("A", "Collect information")

    task.transition_to("READY")
    assert task.status == "READY"

    task.transition_to("RUNNING")
    assert task.status == "RUNNING"

    task.transition_to("COMPLETED")
    assert task.status == "COMPLETED"


def test_failure_transition():
    task = Task("A", "Collect information")

    task.transition_to("READY")
    task.transition_to("RUNNING")
    task.transition_to("FAILED")

    assert task.status == "FAILED"


def test_invalid_transition():
    task = Task("A", "Collect information")

    with pytest.raises(ValueError, match="Invalid transition"):
        task.transition_to("COMPLETED")


def test_completed_task_cannot_restart():
    task = Task("A", "Collect information")

    task.transition_to("READY")
    task.transition_to("RUNNING")
    task.transition_to("COMPLETED")

    with pytest.raises(ValueError, match="Invalid transition"):
        task.transition_to("RUNNING")