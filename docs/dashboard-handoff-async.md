# Handoff prompt — async exports + issue #8 fixes (paste into a Claude Code session in `sector-in-brief`)

You are in the **`sector-in-brief`** dashboard. The data API (`sector-in-brief-api`,
sibling at `../sector-in-brief-api`) changed since you filed issue #8. This is the
**delta** for the Custom Panel Datasets download form — read the two prior handoffs
first; their auth / delivery / estimate / viz guidance is unchanged.

## Read first
- `../sector-in-brief-api/docs/dashboard-integration-handoff.md` — base (auth via
  `paws` IAM invoke, link-not-bytes, estimate→confirm, leave the viz alone).
- `../sector-in-brief-api/docs/dashboard-handoff-bmf-mode.md` — `source=bmf` mode.
- `../sector-in-brief-api/openapi.yaml`, ADR 0026 + **ADR 0030** in `../nccs-contracts/decisions/`.

## What changed since #8

1. **Parquet works again — re-enable the option.** #8 part 1 was a server bug
   (the CSV-only `HEADER` flag was sent for parquet too). Fixed; `format:"parquet"`
   now returns a parquet result.
2. **Large exports succeed and are fast.** The big-export `HTTPException` was a
   crosswalk-join fan-out bug + a redundant double-execution, both fixed. A 3-year ×
   3-form California export (193k rows) now materializes in ~5.5s synchronously.
3. **NEW — giant exports run asynchronously (ADR 0030).** A request whose *estimated*
   result exceeds ~8 GB (over what the Lambda can do synchronously) is handed to a
   background Fargate worker. **Your `/data` call can now return `202`.** This is the
   async/`/status` pattern you asked for in #8.

## The response contract now has THREE shapes — branch on `statusCode`

| `statusCode` | meaning | body |
|---|---|---|
| **200** | sync, ready (as before) | `result.url` (presigned), `download_url` (durable), `data_dictionary.url`, `row_count` |
| **202** | **async, pending (NEW)** | `job_id`, `status:"pending"`, `download_path`, `download_url`, `estimated_bytes` — **no `result` yet** |
| 400 | validation error | `error`, `detail` |

Today you treat the invoke as always-200; you must now check `statusCode` and handle
`202`.

## Handling a `202` (the async path)

The export is materializing on Fargate (typically a minute or more incl. ~30–60s
task cold start). Two delivery shapes — use either or both:

- **Email-and-wait (robust default):** if the request carried `email`, the worker
  sends the durable link when it finishes. Show *"This is a large export — we've
  started it and will email you a download link when it's ready,"* and let the user
  leave the page.
- **Poll:** hit the durable link until it resolves. `GET {download_url}` (or
  direct-invoke `{"download": job_id}`) returns:
  - `202 {status:"pending"}` — still running, keep polling (e.g. every 10–15s);
  - `302` with a `Location` header — ready; that's the presigned S3 URL to hand the
    browser;
  - `500 {status:"failed"}` — surface an error.

Append `?kind=dictionary` to the durable link for the data dictionary, same as the
sync path.

## Predicting async before you commit (nice-to-have)

You already call `estimate` before confirming. If `estimated_bytes` is large
(the server routes above ~8 GB), expect a `202` and pre-set the "we'll email you"
messaging. You don't *need* to predict it — handling the `202` reactively is enough
— but it makes the UX smoother.

## Still relevant from the base handoffs
- **Set the `paws` invoke read-timeout generously (≥120s).** Sync exports up to the
  ~8 GB line can take tens of seconds; too short a client timeout surfaces them as
  `HTTPException` even though the server is fine.
- **Keep estimate→confirm** as the warn-before-big-export guard.
- **Link-not-bytes**, durable `/download/{job_id}`, and **don't touch the viz panels**
  — unchanged.

## Suggested test matrix
- small state, CSV → `200`, result URL resolves.
- small state, **parquet** → `200`, `result.format == "parquet"` (the #8 fix).
- multi-year multi-form single state (e.g. CA) → `200`, ~5s.
- broad, no state filter, many years/forms/columns → may be `202`; then poll
  `download_path` to `302`, or rely on the emailed link.

## How to proceed
Build against `../sector-in-brief-api/openapi.yaml`. The one required change is
**branching on `statusCode` and adding the `202` poll/email path**; re-enabling
parquet is a one-line form change. Summarize your plan (response branching, the poll
loop / email messaging, parquet re-enable) before implementing.
