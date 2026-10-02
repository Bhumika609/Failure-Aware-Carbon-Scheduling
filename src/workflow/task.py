
from dataclasses import dataclass, field


@dataclass
class Task:
    task_id: str
    name: str
    dependencies: list[str] = field(default_factory=list)
    runtime: float = 1.0
    status: str = "PENDING"
    result: object | None = None
    failure_probability: float = 0.0
    previous_result: object | None = None
    needs_recovery_check: bool = False

    # Retry configuration
    # Retry configuration
    max_retries: int = 3
    attempts: int = 0
    attempts_in_cycle: int = 0

    ALLOWED_TRANSITIONS = {
        "PENDING": ["READY", "BLOCKED"],
        "READY": ["RUNNING", "BLOCKED"],
        "RUNNING": ["COMPLETED", "FAILED"],
        "COMPLETED": [],
        "FAILED": ["READY", "BLOCKED"],
        "BLOCKED": [],
    }

    def __post_init__(self):
        if not 0.0 <= self.failure_probability <= 1.0:
            raise ValueError(
                "Failure probability must be between 0.0 and 1.0"
            )

        if self.max_retries < 0:
            raise ValueError(
                "max_retries cannot be negative"
            )

    def transition_to(self, new_status: str):
        if new_status not in self.ALLOWED_TRANSITIONS:
            raise ValueError(
                f"Unknown task status: {new_status}"
            )

        allowed = self.ALLOWED_TRANSITIONS[self.status]

        if new_status not in allowed:
            raise ValueError(
                f"Invalid transition: {self.status} -> {new_status}"
            )

        self.status = new_status
    
    def outputs_changed(self, previous_output, new_output):
        """
        Compare two task outputs.

        Returns True if outputs differ or cannot be compared reliably.
        Returns False if outputs are equal.
        """
        try:
            comparison = previous_output == new_output
            return not bool(comparison)
        except Exception:
            # If comparison is uncertain, assume output changed.
            return True
    def reset_for_reexecution(self):
        if self.status != "COMPLETED":
            raise ValueError(
                f"Task {self.task_id} must be COMPLETED "
                "before it can be reset for re-execution"
            )

        self.previous_result = self.result
        self.result = None
        self.status = "PENDING"
        self.needs_recovery_check = True
        self.attempts_in_cycle = 0