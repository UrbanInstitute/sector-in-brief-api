"""Fargate worker entrypoint (ADR 0030) — materialize one giant async export.

The query Lambda launches this as a one-shot task for jobs over
ASYNC_THRESHOLD_BYTES (too large for Lambda's 10 GB join-memory cap). It reuses
query.run_async_job, which shares _materialize with the synchronous path so there
is one materialization implementation.

Run:  python -m worker <job_id>      (job_id also accepted via the JOB_ID env var)
"""
import os, sys, boto3
from query import run_async_job


def main():
    job_id = sys.argv[1] if len(sys.argv) > 1 else os.environ["JOB_ID"]
    s3 = boto3.client("s3", region_name="us-east-1")
    run_async_job(job_id, s3)


if __name__ == "__main__":
    main()
