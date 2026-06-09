# The mean lies: sizing systems from fat-tailed distributions

*A worked example from sizing the modernized NCCS data API (2026-06-09), written
to generalize. One in a running set of fat-tail notes across repos.*

## The one-line lesson

When a quantity that drives an infrastructure decision is **fat-tailed or
bimodal, the mean describes no real case** — it is the arithmetic midpoint of two
populations that don't overlap. Size from the **order statistics (p50, p95, max)
and the threshold-crossings**, never from the average.

## The worked example

We had to choose a runtime host for an API that materializes query results to a
file. The cost driver is **result size**. We had a gift: 2,539 *real* result
files from the legacy production system — the actual distribution, not a model.

| statistic | result size |
|-----------|-------------|
| p50 | **0.1 MB** |
| p75 | 117 MB |
| p90 | 2.7 GB |
| p95 | 11.7 GB |
| p99 | 30.7 GB |
| max | **51.2 GB** |
| **mean** | **1.6 GB** |

The median query returns **0.1 MB**. The mean is **1.6 GB** — *sixteen thousand
times* the median. **No query in the dataset produces anything near 1.6 GB.** The
distribution is two separated humps: a dense cluster of near-zero results (someone
filtering to one state-year) and a sparse cluster of near-50 GB dumps (whole
dataset, all columns, all years). The mean is the empty valley between them.

Had we "planned for ~1.6 GB results," we would have been wrong in *both*
directions at once: over-built for the 50%+ of queries under a megabyte, and
under-built for the 5.6% that blow past any reasonable single-machine ceiling.

## Why the mean lies (the mechanism)

The mean is a **mass-weighted** summary: a handful of 50 GB files drag it up by
gigabytes apiece, while thousands of 0.1 MB files can't drag it back down. For a
sum/total that's the number you want. For "what is a *typical* case, and what is
the *worst* case I must survive" — the two questions infrastructure actually asks
— the mean answers neither. The median answers the first; the max / high
percentile answers the second. The mean answers a question nobody asked.

This is general: any time the tail carries a large share of the total mass
(file sizes, request latencies, fan-out, account balances, document lengths, blast
radius), the mean migrates toward the tail and stops describing the body.

## What to do instead

1. **Report order statistics, not the mean.** At minimum p50 / p95 / max. The
   spread between them *is* the finding — a p50≪p95 gap is the fat tail announcing
   itself.
2. **Decide from threshold-crossings, not central tendency.** Pick the lines that
   change the design and count what crosses them. Here:

   | threshold | meaning | % of queries over |
   |-----------|---------|-------------------|
   | 6 MB | inline API response cap | 38.5% |
   | 100 MB | sync-vs-async line | 25.6% |
   | 10 GB | single-host memory ceiling | 5.6% |

   Each crossing is a design forcing-function. *38.5% exceed the inline cap* is
   what killed "just stream the bytes back" — for more than a third of queries
   there is no inline option. *5.6% exceed 10 GB* is what killed the small,
   time-limited compute host. Neither fact is visible in p50, p95, or the mean.
3. **Design for the body and the tail separately.** Bimodal input often wants a
   bimodal system: a cheap fast path for the dense low cluster, a different
   (async, bigger, streamed) path for the sparse high cluster. Forcing one path to
   span both is how you get a host that's wasteful for the common case and falls
   over on the rare one.
4. **Prefer real data to a model of it.** The legacy system had already sampled
   the true distribution 2,539 times. Replaying a handful of synthetic queries
   would have *invented* a distribution; reading the production artifacts
   *measured* it — and at lower cost. Look for the data you already have before
   generating new data.

## Reusable checklist (port to other repos)

Before sizing anything from a measured quantity:

- [ ] Is there existing production data that already sampled this? Use it before synthesizing.
- [ ] Plot/print p50, p95, p99, max — not just the mean. Is p95/p50 ≫ 1? → fat tail; the mean is now suspect.
- [ ] Name the thresholds that change the design; count crossings at each.
- [ ] Is the body bimodal? If so, consider two code paths, not one averaged one.
- [ ] State the worst case you must survive (max, or a defensible p99), separately from the typical case (p50).
- [ ] Record the distribution and its uncertainty, not just the verdict — the next person should be able to re-decide if the tail moves.

## Related

- Project engineering principle #1: *"An aggregate is a hypothesis, not a
  diagnosis. A mean/total hides the distribution that selects the fix."*
- The concrete decision this note came from: [`phase0/FINDINGS.md`](../phase0/FINDINGS.md).
