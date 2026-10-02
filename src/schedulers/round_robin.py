
from src.workers.worker_pool import WorkerPool


class RoundRobinScheduler:

    def __init__(self, worker_pool: WorkerPool):
        self.worker_pool = worker_pool
        self.current_index = 0

    def schedule(self, task_id: str):
    # If a Task object is passed, extract its task_id
        if hasattr(task_id, "task_id"):
            task_id = task_id.task_id

        workers = list(self.worker_pool.workers.values())

        if not workers:
            raise ValueError("No workers available")

        total_workers = len(workers)

        for _ in range(total_workers):
            worker = workers[self.current_index]

            self.current_index = (
                self.current_index + 1
            ) % total_workers

            if worker.is_available():
                worker.assign_task(task_id)
                return worker

        raise ValueError("No worker has available capacity")