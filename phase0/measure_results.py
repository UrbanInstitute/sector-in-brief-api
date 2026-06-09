"""
Phase-0 measurement (ADR 0008): the result-size distribution that decides the
runtime host, taken from REAL production data rather than synthetic replay.

The legacy Athena API wrote every query result as a CSV to
s3://nccs-dataexplorer-stg/results/. That is 2.5k+ real user queries — the
actual size distribution, and (unlike latency) network-independent. We compute
order statistics + the threshold crossings that bound the host choice.

Run:  AWS_PROFILE=thiya python phase0/measure_results.py
"""
import math, boto3

BUCKET, PREFIX = "nccs-dataexplorer-stg", "results/"
THRESHOLDS = [
    ("6 MB  (API Gateway sync response cap)", 6 * 1024**2),
    ("100 MB (ADR 0008 sync-vs-async line)",  100 * 1024**2),
    ("512 MB (Lambda default /tmp)",          512 * 1024**2),
    ("10 GB  (Lambda max memory & /tmp)",      10 * 1024**3),
]


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    sizes = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".csv"):
                sizes.append(o["Size"])
    sizes.sort()
    n = len(sizes)

    def pct(p):
        k = max(0, min(n - 1, math.ceil(p / 100 * n) - 1))
        return sizes[k]

    mb = lambda b: f"{b/1024/1024:,.1f} MB"
    print(f"n = {n} real result CSVs  ({BUCKET}/{PREFIX})")
    print(f"empty (0 bytes): {sum(1 for s in sizes if s == 0)}")
    for label, p in [("min", 0), ("p50", 50), ("p75", 75), ("p90", 90),
                     ("p95", 95), ("p99", 99), ("max", 100)]:
        print(f"  {label:5s} {mb(sizes[0] if p==0 else (sizes[-1] if p==100 else pct(p)))}")
    print(f"  mean  {mb(sum(sizes)/n)}   <-- the aggregate that hides the bimodality")
    print("threshold crossings:")
    for label, thr in THRESHOLDS:
        over = sum(1 for s in sizes if s > thr)
        print(f"  > {label:42s}: {over:5d}  ({100*over/n:5.1f}%)")


if __name__ == "__main__":
    main()
