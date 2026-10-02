
from src.workers.worker import Worker


class WorkerPool:

    def __init__(self):
        self.workers = {}

    def add_worker(self, worker: Worker):
        if worker.worker_id in self.workers:
            raise ValueError(
                f"Worker already exists: {worker.worker_id}"
            )

        self.workers[worker.worker_id] = worker

    def get_worker(self, worker_id: str):
        if worker_id not in self.workers:
            raise ValueError(f"Worker not found: {worker_id}")

        return self.workers[worker_id]

    def get_available_workers(self):
        available_workers = []

        for worker in self.workers.values():
            if worker.is_available():
                available_workers.append(worker)

        return available_workers

    def assign_task(self, worker_id: str, task_id: str):
        worker = self.get_worker(worker_id)
        worker.assign_task(task_id)

    def release_task(self, worker_id: str, task_id: str):
        worker = self.get_worker(worker_id)
        worker.release_task(task_id)

    def get_total_capacity(self):
        total_capacity = 0

        for worker in self.workers.values():
            total_capacity += worker.capacity

        return total_capacity