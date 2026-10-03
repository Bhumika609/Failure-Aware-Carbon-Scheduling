
from dataclasses import dataclass, field


@dataclass
class Worker:
    worker_id: str
    capacity: int = 1
    processing_speed: float = 1.0
    power_watts: float = 200.0
    carbon_intensity_g_per_kwh: float = 400.0
    failure_probability: float = 0.0
    running_tasks: list[str] = field(default_factory=list)
    is_failed: bool = False

    def __post_init__(self):
        if self.capacity <= 0:
            raise ValueError("Worker capacity must be positive")

        if self.processing_speed <= 0:
            raise ValueError("Processing speed must be positive")

        if self.power_watts <= 0:
            raise ValueError("Worker power consumption must be positive")

        if self.carbon_intensity_g_per_kwh < 0:
            raise ValueError("Carbon intensity cannot be negative")
        if not 0.0 <= self.failure_probability <= 1.0:
            raise ValueError(
                "Failure probability must be between 0 and 1"
            )

    def available_slots(self):
        if self.is_failed:
            return 0

        return self.capacity - len(self.running_tasks)

    def is_available(self):
        return self.available_slots() > 0


    def estimate_execution_time(self, task_runtime: float) -> float:
        return task_runtime / self.processing_speed

    def estimate_energy_kwh(self, task_runtime: float) -> float:
        execution_time = self.estimate_execution_time(task_runtime)

        energy_kwh = (
            self.power_watts * execution_time / 3_600_000
        )

        return energy_kwh

    def estimate_carbon_g(self, task_runtime: float) -> float:
        energy_kwh = self.estimate_energy_kwh(task_runtime)

        carbon_g = (
            energy_kwh * self.carbon_intensity_g_per_kwh
        )

        return carbon_g

    def assign_task(self, task_id: str):
        if self.is_failed:
            raise ValueError("Cannot assign task to failed worker")

        if not self.is_available():
            raise ValueError("Worker has no available capacity")

        self.running_tasks.append(task_id)
    def fail(self):
        self.is_failed = True
    def recover(self):
        self.is_failed = False
    def release_task(self, task_id: str):
        if task_id not in self.running_tasks:
            raise ValueError("Task is not assigned to this worker")

        self.running_tasks.remove(task_id)