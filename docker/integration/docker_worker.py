from worker_client import check_worker


class DockerWorker:

    def __init__(self, worker_id, worker_url):
        self.worker_id = worker_id
        self.worker_url = worker_url

    def is_available(self):
        result = check_worker(self.worker_url)

        return result["status"] == "healthy"


if __name__ == "__main__":
    worker = DockerWorker(
        "worker-1",
        "http://localhost:8001",
    )

    print(worker.is_available())