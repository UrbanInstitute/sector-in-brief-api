"""Unit test for the rollup's month-boundary logic (no AWS/DuckDB)."""
import os
from datetime import datetime, timezone
os.environ.setdefault("RESULTS_BUCKET", "test-bucket")  # rollup.py reads this at import

from jobs.rollup import _prev_month


def test_prev_month_mid_year():
    assert _prev_month(datetime(2026, 6, 9, tzinfo=timezone.utc)) == (2026, 5)


def test_prev_month_january_rolls_to_prior_december():
    assert _prev_month(datetime(2026, 1, 3, tzinfo=timezone.utc)) == (2025, 12)
