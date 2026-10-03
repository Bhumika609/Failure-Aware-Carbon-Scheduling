from worker_client import check_all_workers


workers = [
    "http://localhost:8001",
    "http://localhost:8002",
]

results = check_all_workers(workers)

for result in results:
    print(result)