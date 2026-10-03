import urllib.request

from fastapi import FastAPI

app = FastAPI()

WORKERS = [
    "http://worker-1:8000",
    "http://worker-2:8000",
]


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/workers")
def workers():
    results = []

    for worker_url in WORKERS:
        try:
            response = urllib.request.urlopen(
                f"{worker_url}/health",
                timeout=2,
            )

            results.append({
                "worker": worker_url,
                "status": response.read().decode(),
            })

        except Exception:
            results.append({
                "worker": worker_url,
                "status": "unreachable",
            })

    return {"workers": results}