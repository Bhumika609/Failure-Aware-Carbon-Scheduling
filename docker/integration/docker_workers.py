from worker_client import check_all_workers


WORKERS = [
    "http://localhost:8001",
    "http://localhost:8002",
]


def get_worker_status():
    return check_all_workers(WORKERS)


if __name__ == "__main__":
    print(get_worker_status())