FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn

COPY worker_service.py .

EXPOSE 8000

CMD ["uvicorn", "worker_service:app", "--host", "0.0.0.0", "--port", "8000"]