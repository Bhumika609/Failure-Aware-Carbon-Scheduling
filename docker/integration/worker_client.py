import urllib.request
import json


def check_worker(worker_url):
    try:
        response = urllib.request.urlopen(
            f"{worker_url}/health",
            timeout=2,
        )

        data = json.loads(response.read().decode())

        return {
            "worker": worker_url,
            "status": data["status"],
        }

    except Exception:
        return {
            "worker": worker_url,
            "status": "unreachable",
        }


def check_all_workers(worker_urls):
    results = []

    for worker_url in worker_urls:
        results.append(check_worker(worker_url))

    return results