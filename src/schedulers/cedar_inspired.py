
from src.workers.worker_pool import WorkerPool
from src.workflow.task import Task


class CedarInspiredScheduler:

    def __init__(
        self,
        worker_pool: WorkerPool,
        carbon_weight: float = 1.0,
    ):
        if carbon_weight < 0:
            raise ValueError("Carbon weight cannot be negative")

        self.worker_pool = worker_pool
        self.carbon_weight = carbon_weight

    def calculate_score(self, task: Task, worker):
        # Estimate execution time
        estimated_time = worker.estimate_execution_time(
            task.runtime
        )

        # Calculate worker load
        load_ratio = (
            len(worker.running_tasks) / worker.capacity
        )

        # Estimate carbon emissions
        estimated_carbon = worker.estimate_carbon_g(
            task.runtime
        )

        # Calculate combined score
        score = (
            estimated_time * (1 + load_ratio)
            + self.carbon_weight * estimated_carbon
        )

        return score

    def schedule(self, task: Task):
        available_workers = (
            self.worker_pool.get_available_workers()
        )

        if not available_workers:
            raise ValueError("No worker has available capacity")

        selected_worker = available_workers[0]

        selected_score = self.calculate_score(
            task, selected_worker
        )

        for worker in available_workers[1:]:
            score = self.calculate_score(task, worker)

            if score < selected_score:
                selected_worker = worker
                selected_score = score

        selected_worker.assign_task(task.task_id)

        return selected_worker