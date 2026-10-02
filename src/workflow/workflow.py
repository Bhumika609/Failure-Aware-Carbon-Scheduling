from src.workflow.task import Task


class Workflow:
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self.tasks = {}

    def add_task(self, task: Task):
        if task.task_id in self.tasks:
            raise ValueError(
                f"Duplicate task ID: {task.task_id}"
            )

        self.tasks[task.task_id] = task

    def validate(self):
        # Check whether all dependencies exist
        for task in self.tasks.values():
            for dependency in task.dependencies:
                if dependency not in self.tasks:
                    raise ValueError(
                        f"Task {task.task_id} depends on "
                        f"unknown task {dependency}"
                    )

        # Check whether the graph contains a cycle
        visited = set()
        active = set()

        def dfs(task_id):
            if task_id in active:
                raise ValueError("Workflow contains a cycle")

            if task_id in visited:
                return

            active.add(task_id)

            task = self.tasks[task_id]

            for dependency in task.dependencies:
                dfs(dependency)

            active.remove(task_id)
            visited.add(task_id)

        for task_id in self.tasks:
            dfs(task_id)

        return True
    def get_ready_tasks(self):
        ready_tasks = []

        for task in self.tasks.values():
            if task.status == "READY":
                ready_tasks.append(task)

        return ready_tasks
    def refresh_ready_tasks(self):
        newly_ready = []

        for task in self.tasks.values():

            if task.status != "PENDING":
                continue

            dependencies_completed = True

            for dependency in task.dependencies:
                dependency_task = self.tasks[dependency]

                if dependency_task.status != "COMPLETED":
                    dependencies_completed = False
                    break

            if dependencies_completed:
                task.transition_to("READY")
                newly_ready.append(task)

        return newly_ready
    
    def get_downstream_tasks(self, task_id: str):
        """
        Return all direct and indirect downstream tasks
        in workflow insertion order.
        """
        if task_id not in self.tasks:
            raise ValueError(f"Unknown task ID: {task_id}")

        downstream_ids = set()
        changed = True

        while changed:
            changed = False

            for task in self.tasks.values():
                if task.task_id == task_id:
                    continue

                if task.task_id in downstream_ids:
                    continue

                for dependency in task.dependencies:
                    if (
                        dependency == task_id
                        or dependency in downstream_ids
                    ):
                        downstream_ids.add(task.task_id)
                        changed = True
                        break

        downstream_tasks = []

        for task in self.tasks.values():
            if task.task_id in downstream_ids:
                downstream_tasks.append(task)

        return downstream_tasks
    def reset_downstream_tasks(self, task_id: str):
        """
        Reset all completed downstream tasks so they can
        be re-executed after an upstream output changes.
        """
        downstream_tasks = self.get_downstream_tasks(task_id)

        reset_tasks = []

        for task in downstream_tasks:
            if task.status == "COMPLETED":
                task.reset_for_reexecution()
                reset_tasks.append(task)

        return reset_tasks
    def block_downstream_tasks(self, task_id: str):
        """
        Mark all downstream tasks as BLOCKED after
        an upstream task fails permanently.
        """
        downstream_tasks = self.get_downstream_tasks(task_id)

        blocked_tasks = []

        for task in downstream_tasks:
            if task.status in ("PENDING", "READY", "FAILED"):
                task.transition_to("BLOCKED")
                blocked_tasks.append(task)

        return blocked_tasks