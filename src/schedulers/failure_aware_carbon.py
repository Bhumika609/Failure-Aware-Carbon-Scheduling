
from src.workers.worker_pool import WorkerPool
from src.workflow.task import Task


class FailureAwareCarbonScheduler:

    def __init__(
        self,
        worker_pool: WorkerPool,
        time_weight: float = 1.0,
        carbon_weight: float = 1.0,
        load_weight: float = 1.0,
        failure_weight: float = 1.0,
    ):
        if time_weight < 0:
            raise ValueError("Time weight cannot be negative")

        if carbon_weight < 0:
            raise ValueError("Carbon weight cannot be negative")

        if load_weight < 0:
            raise ValueError("Load weight cannot be negative")

        if failure_weight < 0:
            raise ValueError("Failure weight cannot be negative")

        self.worker_pool = worker_pool
        self.time_weight = time_weight
        self.carbon_weight = carbon_weight
        self.load_weight = load_weight
        self.failure_weight = failure_weight

    def normalize(self, value, minimum, maximum):
        if maximum == minimum:
            return 0.0

        return (value - minimum) / (maximum - minimum)

    def calculate_metrics(self, task: Task, worker):
        estimated_time = worker.estimate_execution_time(
            task.runtime
        )

        estimated_carbon = worker.estimate_carbon_g(
            task.runtime
        )

        load_ratio = (
            len(worker.running_tasks) / worker.capacity
        )

        failure_penalty = (
            worker.failure_probability * estimated_time
        )

        return {
            "time": estimated_time,
            "carbon": estimated_carbon,
            "load": load_ratio,
            "failure": failure_penalty,
        }

    # Original raw weighted score
    def calculate_score(self, task: Task, worker):
        metrics = self.calculate_metrics(task, worker)

        score = (
            self.time_weight * metrics["time"]
            + self.carbon_weight * metrics["carbon"]
            + self.load_weight * metrics["load"]
            + self.failure_weight * metrics["failure"]
        )

        return score

    # Calculate normalized scores for all candidate workers
    def calculate_normalized_scores(self, task: Task, workers):
        if not workers:
            return []

        all_metrics = []

        for worker in workers:
            metrics = self.calculate_metrics(task, worker)
            all_metrics.append(metrics)

        minimums = {}
        maximums = {}

        for metric_name in ["time", "carbon", "load", "failure"]:
            values = [
                metrics[metric_name]
                for metrics in all_metrics
            ]

            minimums[metric_name] = min(values)
            maximums[metric_name] = max(values)

        normalized_scores = []

        for index, worker in enumerate(workers):
            metrics = all_metrics[index]

            normalized_time = self.normalize(
                metrics["time"],
                minimums["time"],
                maximums["time"],
            )

            normalized_carbon = self.normalize(
                metrics["carbon"],
                minimums["carbon"],
                maximums["carbon"],
            )

            normalized_load = self.normalize(
                metrics["load"],
                minimums["load"],
                maximums["load"],
            )

            normalized_failure = self.normalize(
                metrics["failure"],
                minimums["failure"],
                maximums["failure"],
            )

            score = (
                self.time_weight * normalized_time
                + self.carbon_weight * normalized_carbon
                + self.load_weight * normalized_load
                + self.failure_weight * normalized_failure
            )

            normalized_scores.append((worker, score))

        return normalized_scores

    # Select the worker with the lowest normalized score
    def schedule(self, task: Task):
        available_workers = (
            self.worker_pool.get_available_workers()
        )

        if not available_workers:
            raise ValueError(
                "No worker has available capacity"
            )

        normalized_scores = self.calculate_normalized_scores(
            task,
            available_workers,
        )

        selected_worker, selected_score = min(
            normalized_scores,
            key=lambda item: item[1],
        )

        selected_worker.assign_task(task.task_id)

        return selected_worker