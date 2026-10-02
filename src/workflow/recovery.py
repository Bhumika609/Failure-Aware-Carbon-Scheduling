
from src.workflow.task import Task
from src.workflow.workflow import Workflow


class RecoveryPolicy:
    """
    Decide which downstream tasks need re-execution
    when an upstream task's output changes.
    """

    def should_reexecute(
        self,
        task: Task,
        previous_output,
        new_output,
    ) -> bool:
        """
        Return True if the output changed or comparison
        was uncertain.
        """
        return task.outputs_changed(previous_output, new_output)

    def get_affected_tasks(
        self,
        workflow: Workflow,
        task_id: str,
        previous_output,
        new_output,
    ) -> list[Task]:
        """
        Return downstream tasks that need re-execution
        when the specified task's output changes.
        """
        source_task = workflow.tasks.get(task_id)

        if source_task is None:
            raise ValueError(f"Unknown task ID: {task_id}")

        output_changed = self.should_reexecute(
            source_task,
            previous_output,
            new_output,
        )

        if not output_changed:
            return []

        return workflow.get_downstream_tasks(task_id)
    def recover(
        self,
        workflow: Workflow,
        task_id: str,
        previous_output,
        new_output,
    ) -> list[Task]:
        """
        Reset downstream tasks if the source task's output
        has changed.

        Return the list of tasks that were reset.
        """
        affected_tasks = self.get_affected_tasks(
            workflow,
            task_id,
            previous_output,
            new_output,
        )

        if not affected_tasks:
            return []

        return workflow.reset_downstream_tasks(task_id)