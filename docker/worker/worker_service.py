import os

from fastapi import FastAPI

app = FastAPI()

WORKER_ID = os.getenv("WORKER_ID", "worker-container")


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/worker")
def worker():
    return {
        "worker_id": WORKER_ID,
        "status": "available",
    }