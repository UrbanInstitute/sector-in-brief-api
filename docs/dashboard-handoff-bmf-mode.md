# Handoff prompt — paste into a Claude Code session opened in the `sector-in-brief` repo

You are working in the **`sector-in-brief`** repo — the Shiny dashboard. The data
API, **`sector-in-brief-api`** (sibling at `../sector-in-brief-api`), has gained a
**second query mode** since the original Data-Download rework. Your job is to extend
the download form to expose it. This is a **delta** on the existing integration —
read the base handoff first and reuse its plumbing.

## Read first
- `docs/dashboard-integration-handoff.md` (this repo's sibling) — the **base
  handoff**: auth (paws invoke vs SigV4), the `/data` + durable `/download/{job_id}`
  surface, link-not-bytes delivery, the estimate/warn UX, and "don't touch the viz
  panels." **All of that is unchanged and still applies** — do not re-derive it.
- `../sector-in-brief-api/openapi.yaml` — the contract, now with `source` +
  `active_years` (conditional schema).
- `../nccs-contracts/decisions/0029-bmf-org-level-query-mode.md` — why this mode
  exists and its measured behavior.

## What's new in the API

A `source` field on `POST /data` selects the query mode (default `"core"` — the
existing filing-level behavior, **unchanged**). The new value is `"bmf"`:

| | `source: "core"` (default, existing) | `source: "bmf"` (new) |
|---|---|---|
| Grain | one row per **filing** (filed a 990 in the tax years) | one row per **EIN** — the whole org registry, **incl. non-filers** |
| Year filter | `tax_years` (partition selector) | `active_years` (lifespan overlap) |
| `forms` | applies | **rejected** (no forms concept) |
| Financial columns (`total_revenue`, …) | available | **absent** (registry has no financials/tax-year) |
| Geo + classification columns (`geo_*`, `census_region`, `cbsa_*`, `org_type`, `nteev2_subsector*`) | available | **available** (same crosswalk-derived columns) |

So BMF mode answers org-level / "every registered nonprofit incl. those that never
filed" questions that the CORE join structurally cannot (non-filers have no CORE
row). It's a **new download option**, not a replacement.

### `active_years` — read this carefully, it drives the UX

The registry has no per-year membership; each org carries only a lifespan
`[first_year_in_bmf, last_year_in_bmf]`. `active_years` is a list of years and the
API filters by **lifespan overlap** with that span:

    first_year_in_bmf <= max(active_years)  AND  last_year_in_bmf >= min(active_years)

i.e. **"active at any point during the requested span."** Only the **endpoints**
matter — gaps between listed years are not honored (the registry can't express a
gap). So `active_years: [2015, 2018]` and `active_years: [2015, 2016, 2017, 2018]`
mean the **same thing**: orgs alive at any point in 2015–2018.

**UX implication (your call, but important):** label this control as *"active
during"* a year range, **not** *"filed in"* / *"tax year."* It is a different
question from CORE's `tax_years`. A range slider (min/max) maps cleanly since only
the endpoints are used. Do **not** present it as a multi-select of discrete years
that implies per-year membership — that would misrepresent the semantic.

### Provenance columns come back automatically

When `active_years` is applied, the API **forces both** `first_year_in_bmf` and
`last_year_in_bmf` into the output (appended after your requested columns, deduped
like `ein`). This is intentional — the result self-audits the overlap filter, so a
user can see *why* each row matched. **Don't fight it**; surface them, or at least
expect two extra columns you didn't request.

### Request examples

BMF mode, orgs active anytime in 2015–2018 in CA, with the dashboard's usual
dimensions (note: no `tax_years`, no `forms`, no financial columns):
```json
{
  "source": "bmf",
  "active_years": [2015, 2018],
  "columns": ["ein", "geo_state_abbr", "org_type", "nteev2_subsector", "nteev2_subsector_definition"],
  "filters": { "geo_state_abbr": ["CA"], "org_type": ["501(c)(3) Public Charities"] },
  "format": "csv",
  "email": "user@example.org"
}
```
Result columns also include `first_year_in_bmf`, `last_year_in_bmf` (forced).

Size pre-check works exactly as in CORE mode — add `"estimate": true` to get
`{row_count, columns, estimated_bytes}` with no materialize/email. **Use it** — the
unfiltered registry is ~3.67M rows / up to ~3.5 GB, so the estimate/warn step (base
handoff §3) matters more here.

### Validation rules (the API enforces; your form should pre-respect)
- `source: "bmf"` **requires** `active_years` and **rejects** `tax_years` / `forms`
  → 400. `source: "core"` (or omitted) requires `tax_years` as today.
- Filters/columns are validated against the live schema for that mode. Financial
  columns in BMF mode → 400 (they don't exist in the registry). The shared
  geo/org_type/subsector filters work in **both** modes.

## What the form must do

This stays on **your** side of the API/dashboard boundary (API owns facts &
invariants; you own UX & product defaults):

1. **Mode selector** — let the user pick "filings (CORE)" vs "all registered orgs
   incl. non-filers (BMF)." Wording is yours; make the non-filer distinction legible.
2. **Swap the year control by mode** — `tax_years` (filing years) for CORE; an
   *"active during"* range → `active_years` for BMF. Don't show `forms` or financial
   columns in BMF mode.
3. **Default columns for BMF mode** — the API forces only `ein` (+ the two lifespan
   columns when filtering). The usual-wanted set is **yours** to pre-select (org
   name, `geo_state_abbr`, `org_type`, `nteev2_subsector` + `nteev2_subsector_definition`).
4. **Reuse everything else** — auth, estimate/warn, link-not-bytes delivery, durable
   `/download/{job_id}`, progress UX: all identical to the base handoff.

## Don't break the viz panels
Unchanged from the base handoff: the visualization panels read
`sector-in-brief-data`'s pre-built artifacts, **not** this API. Only the download
section changes.

## How to proceed
Build against `../sector-in-brief-api/openapi.yaml`. Treat BMF as an additive option
on the existing form, not a rewrite. Summarize your plan — mode toggle, the
`active_years` "active during" control, BMF default columns, and how you handle the
forced provenance columns — before implementing.
