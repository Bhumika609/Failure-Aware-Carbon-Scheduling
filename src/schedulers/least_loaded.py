
from src.workers.worker_pool import WorkerPool


class LeastLoadedScheduler:

    def __init__(self, worker_pool: WorkerPool):
        self.worker_pool = worker_pool

    def schedule(self, task_id: str):
        if hasattr(task_id, "task_id"):
            task_id = task_id.task_id
        available_workers = (
            self.worker_pool.get_available_workers()
        )

        if not available_workers:
            raise ValueError("No worker has available capacity")

        selected_worker = available_workers[0]

        for worker in available_workers:
            if len(worker.running_tasks) < len(
                selected_worker.running_tasks
            ):
                selected_worker = worker

        selected_worker.assign_task(task_id)

        return selected_worker