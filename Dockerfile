# Fargate worker for giant async exports (ADR 0030). Runs the SAME DuckDB
# materialization as the Lambda (query/), but on Fargate where memory goes well
# past Lambda's 10 GB join cap. The query Lambda launches one task per giant job,
# passing JOB_ID as an environment override (see _dispatch_async).
FROM python:3.12-slim

WORKDIR /app

# duckdb is pinned in query/requirements.txt; boto3 is NOT (the Lambda runtime
# bundles it, but a container must install it explicitly).
COPY query/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt boto3

# query.py + worker.py live at /app so `python -m worker` resolves `import query`.
COPY query/ /app/

# worker.py reads JOB_ID from the environment (set per-task by ecs:RunTask).
CMD ["python", "-m", "worker"]
