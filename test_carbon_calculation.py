
from src.workers.worker import Worker


worker = Worker(
    worker_id="worker_1",
    processing_speed=2.0,
    power_watts=200.0,
    carbon_intensity_g_per_kwh=400.0,
)

task_runtime = 10.0

execution_time = worker.estimate_execution_time(task_runtime)
energy = worker.estimate_energy_kwh(task_runtime)
carbon = worker.estimate_carbon_g(task_runtime)

print("Execution time:", execution_time, "seconds")
print("Energy:", energy, "kWh")
print("Carbon emissions:", carbon, "gCO2e")

assert execution_time == 5.0
assert abs(energy - 0.0002777778) < 1e-9
assert abs(carbon - 0.1111111) < 1e-6