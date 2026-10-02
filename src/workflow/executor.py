import time
from typing import Callable, Any
import random
from src.workflow.workflow import Workflow
from src.workflow.task import Task
from src.workers.worker_pool import WorkerPool
from concurrent.futures import ThreadPoolExecutor
from src.workflow.recovery import RecoveryPolicy
class WorkflowExecutor:

    def __init__(
        self,
        task_runner: Callable[[Task], Any] | None = None,
        scheduler=None,
        worker_pool: WorkerPool | None = None,
        recovery_policy=None,
    ):
        if (scheduler is None) != (worker_pool is None):
            raise ValueError(
                "Scheduler and worker_pool must be provided together"
            )

        self.task_runner = task_runner
        self.scheduler = scheduler
        self.worker_pool = worker_pool
        self.execution_log = []
        self.worker_assignment_log = []
        self.actual_metrics_log = []
        self.recovery_policy = (
            recovery_policy
            if recovery_policy is not None
            else RecoveryPolicy()
        )
    
    def log_worker_assignment(self, task: Task, worker):
        assignment = {
            "task_id": task.task_id,
            "worker_id": worker.worker_id,
            "attempt": task.attempts + 1,
            "estimated_execution_time": (
                worker.estimate_execution_time(task.runtime)
            ),
            "estimated_energy_kwh": (
                worker.estimate_energy_kwh(task.runtime)
            ),
            "estimated_carbon_g": (
                worker.estimate_carbon_g(task.runtime)
            ),
        }
        self.worker_assignment_log.append(assignment)
    def log_actual_metrics(
        self,
        task: Task,
        worker,
        start_time: float,
    ):
        end_time = time.perf_counter()
        actual_execution_time = end_time - start_time

        actual_energy_kwh = None
        actual_carbon_g = None
        worker_id = None

        if worker is not None:
            worker_id = worker.worker_id

            actual_energy_kwh = (
                worker.power_watts
                * actual_execution_time
                / 3_600_000
            )

            actual_carbon_g = (
                actual_energy_kwh
                * worker.carbon_intensity_g_per_kwh
            )

        metrics = {
            "task_id": task.task_id,
            "attempt": task.attempts,
            "worker_id": worker_id,
            "actual_execution_time_seconds": actual_execution_time,
            "actual_energy_kwh": actual_energy_kwh,
            "actual_carbon_g": actual_carbon_g,
        }

        self.actual_metrics_log.append(metrics)
    def execute_task(self, task: Task, worker=None):
        task.attempts += 1
        task.attempts_in_cycle += 1
        task.transition_to("RUNNING")

        start_time = time.perf_counter()

        try:
            if worker is not None and worker.is_failed:
                raise RuntimeError(
                    f"Worker {worker.worker_id} failed while executing "
                    f"task {task.task_id}"
                )

            # Inject a random failure based on the task's failure probability.
            if random.random() < task.failure_probability:
                raise RuntimeError(
                    f"Injected failure in task {task.task_id}"
                )

            if self.task_runner is not None:
                result = self.task_runner(task)
            else:
                result = f"Result of {task.task_id}"

            task.result = result
            task.transition_to("COMPLETED")

            self.execution_log.append({
                "task_id": task.task_id,
                "attempt": task.attempts,
                "status": task.status,
                "result": task.result,
            })

            self.log_actual_metrics(
                task,
                worker,
                start_time,
            )

            return result

        except Exception:
            task.transition_to("FAILED")

            self.execution_log.append({
                "task_id": task.task_id,
                "attempt": task.attempts,
                "status": task.status,
                "result": None,
            })

            self.log_actual_metrics(
                task,
                worker,
                start_time,
            )

            raise
    def execute_workflow(self, workflow: Workflow):
        workflow.validate()

        execution_order = []
        first_failure = None

        workflow.refresh_ready_tasks()
        workflow.validate()

        execution_order = []

        workflow.refresh_ready_tasks()

        while True:
            ready_tasks = workflow.get_ready_tasks()

            if not ready_tasks:
                break

            # Determine how many tasks can run concurrently
            if self.scheduler is not None:
                available_capacity = sum(
                    worker.available_slots()
                    for worker in self.worker_pool.workers.values()
                )

                if available_capacity == 0:
                    # Let the scheduler raise its normal error
                    self.scheduler.schedule(ready_tasks[0])

                tasks_to_run = ready_tasks[:available_capacity]
            else:
                tasks_to_run = ready_tasks

            scheduled_tasks = []

            # Assign workers before starting concurrent execution
            for task in tasks_to_run:
                worker = None

                if self.scheduler is not None:
                    worker = self.scheduler.schedule(task)

                    self.log_worker_assignment(task, worker)

                scheduled_tasks.append((task, worker))



            def run_task(task, worker):
                current_worker = worker

                while True:
                    try:
                        result = self.execute_task(task, current_worker)

                        if task.needs_recovery_check:
                            self.recovery_policy.recover(
                                workflow,
                                task.task_id,
                                task.previous_result,
                                result,
                            )

                            task.needs_recovery_check = False

                        return result

                    except Exception:
                        # Stop retrying when the retry limit is exhausted.
                        if task.attempts_in_cycle > task.max_retries:
                            raise

                        # Prepare the task for another attempt.
                        task.transition_to("READY")

                        # Release the worker used for the failed attempt.
                        if current_worker is not None:
                            self.worker_pool.release_task(
                                current_worker.worker_id,
                                task.task_id,
                            )
                            current_worker = None

                        # Ask the scheduler to select a worker for the retry.
                        if self.scheduler is not None:
                            current_worker = self.scheduler.schedule(task)

                            self.log_worker_assignment(
                                task,
                                current_worker,
                            )
                    finally:
                        # Always release the worker after the final attempt.
                        if (
                            current_worker is not None
                            and task.status in ("COMPLETED", "FAILED")
                        ):
                            self.worker_pool.release_task(
                                current_worker.worker_id,
                                task.task_id,
                            )
                            current_worker = None

            # Run the selected tasks concurrently
            with ThreadPoolExecutor(
                max_workers=len(scheduled_tasks)
            ) as pool:
                futures = []

                for task, worker in scheduled_tasks:
                    future = pool.submit(run_task, task, worker)
                    futures.append((task, future))

                # Collect results in submission order
# Collect results without stopping when one task fails
# Collect results without stopping when one task fails
                for task, future in futures:
                    try:
                        future.result()
                        execution_order.append(task.task_id)

                    except Exception as error:
                        # Save the first failure for reporting after execution
                        if first_failure is None:
                            first_failure = error

                        # Block downstream tasks after permanent failure
                        if task.status == "FAILED":
                            workflow.block_downstream_tasks(
                                task.task_id
                            )

                        # Continue collecting the remaining task results
                        continue

            # Refresh dependencies after this batch completes
            workflow.refresh_ready_tasks()

        if first_failure is not None:
            raise first_failure

        return execution_order

    def get_statistics(self, workflow: Workflow):
        total_tasks = len(workflow.tasks)

        completed_tasks = 0
        failed_tasks = 0
        pending_tasks = 0

        for task in workflow.tasks.values():
            if task.status == "COMPLETED":
                completed_tasks += 1
            elif task.status == "FAILED":
                failed_tasks += 1
            else:
                pending_tasks += 1

        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "pending_tasks": pending_tasks,
        }

    def get_workflow_status(self, workflow: Workflow):
        if all(
            task.status == "COMPLETED"
            for task in workflow.tasks.values()
        ):
            return "COMPLETED"

        for task in workflow.tasks.values():
            if task.status in ["READY", "RUNNING"]:
                return "IN_PROGRESS"

        for task in workflow.tasks.values():
            if task.status == "PENDING":
                return "BLOCKED"

        if any(
            task.status == "FAILED"
            for task in workflow.tasks.values()
        ):
            return "BLOCKED"

        return "IN_PROGRESS"