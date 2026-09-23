# DevOps Engineer Take-Home Challenge: Config-Driven Mini ETL Pipeline

**Time box:** ~1.5 hours (please don't spend more than 2). We're evaluating judgment and
approach, not a production-grade system — it's fine, and expected, to leave notes about
what you'd do with more time instead of building it.

## Scenario

We've provided a seeded SQLite database of synthetic patient encounter records
(`input/encounters.db`, table `encounters`) and a starter YAML config (`config.yaml`).
Your job is to build a small pipeline that cleans up the data, plus a short piece of
infrastructure-as-code that could run it in the cloud on a schedule.

The team needs a cleaned encounters feed where:

- `patient_dob` is renamed to `date_of_birth` and cast to an actual date type (not a
  string).
- Cancelled encounters are dropped — we only want encounters that actually happened.
- Each row gets an `age_band` derived from the patient's age at the time of the run,
  bucketed into `0-18`, `18-40`, `40-65`, and `65+`.
- Every row has an `encounter_id`, `patient_id`, `date_of_birth`, and `department` —
  rows missing any of these, or with a `department` outside the known set (cardiology,
  orthopaedics, dermatology, radiology), aren't usable downstream and should be rejected
  rather than passed through or silently dropped.

No real patient data is involved — everything in the sample data is synthetic.

## Setup

Run `docker compose up seed-db` once to generate `input/encounters.db` — a SQLite
database seeded with the sample data.

## Task

Build a small pipeline, driven by `config.yaml`, that extracts data from the
`encounters` table, applies the required transforms, and writes the
cleaned result to an output file.

Then write a Terraform or Bicep snippet that provisions the Azure resources needed to
run this pipeline on a schedule.

## What we're looking for

- Clean separation between pipeline logic and configuration — the transform, filter,
  derive, and validation rules should live in `config.yaml`, not be hardcoded
- Competent, readable SQL doing the real transform/filter/derive/validation work against
  the seeded database — not just a `SELECT *` with the logic actually happening in
  application code
- Basic data-quality thinking — reject/flag bad rows rather than silently dropping or
  crashing
- IaC hygiene: parameterization, least-privilege access, scheduling, reasonable naming,
  no committed secrets or subscription IDs

## Submission

A zip or git repo containing your code, config, README notes, and the output your script
produced from the sample data.

---

### Notes

- **Use of AI tools:**  You may use AI tools for this test, but you will be expected to explain your submission and choices in depth.
- **Interview questions:** Consider the following during your task
    - How would you scale this for 50 similar-shaped feeds with different schemas needs to run on the same schedule?
    - How would you scale this if the encounters DB had millions of records?
    - What steps would you take to make this pipeline supportable for a team or organisation?

---

## Submission notes (Callum Jang)

### How to run

```bash
docker compose up seed-db          # generates input/encounters.db
pip install -r requirements.txt
python3 pipeline.py --config config.yaml
```
Output lands in `output/encounters_clean.csv`, `output/encounters_rejected.csv`,
`output/run_summary.json` (already included in this submission from a real run
against the seeded data).

To check the infrastructure code:
```bash
cd terraform
terraform init
terraform validate
```

### Design summary

`pipeline.py` compiles `config.yaml`'s `transforms`/`validation` sections into a
single chain of SQL CTEs (rename → filter → derive → band → validate) run
against SQLite, so the actual field logic lives in SQL/config, not in Python.
Bad rows are never dropped or silently passed through: each one is routed to
`encounters_rejected.csv` with a specific reason, and `run_summary.json` gives
per-reason counts for monitoring. `terraform/` provisions an Azure Container
Apps Job on a cron schedule, pulling the image via a user-assigned managed
identity (no admin credentials, no secrets in code), with `config.yaml`
uploaded into the mounted Azure Files share as part of `apply`.

### Use of AI tools

I used Claude throughout, for design discussion, writing the SQL
generation logic, the Terraform, and testing. Only the synthetic sample data was
ever used with any AI tool. I'm glad to walk through and defend any specific
choice in this submission in depth.

### Interview questions, short answers

**1. Scaling to 50 similar-shaped feeds with different schemas.**

- **Case 1, feeds differ only in field names/allowed values.** Already
  solved: one `config.yaml` per feed, same engine, and the Terraform
  module here takes a map of feeds via `for_each`, so a new feed means a
  new config file, not new code or infra.
- **Case 2, a feed needs genuinely different clinical logic** (e.g. a
  device-integration feed). That needs real code, added as a small
  `modules/` package a feed's config references by name, built only once
  a real feed demands it.
- **Case 3, a feed needs compute this platform can't provide** (e.g. GPU
  inference for batch analysis of already-captured images, not real-time
  signal acquisition, which is a different, out-of-scope problem
  entirely). A managed PaaS add-on for just that feed is cheaper than
  adopting Kubernetes outright for everything.

**2. Scaling to millions of records.** The current script loads everything
into memory at once, that's the first thing to fix, not throwing more compute
at it.

- **Stop loading everything at once.** `fetchall()` pulls every row into
  memory and builds full output lists before writing anything. Switch to
  streaming from the cursor in batches, writing incrementally as they're
  processed.
- **Page by keyset, not `OFFSET`.** `WHERE id > :last_id` keeps a constant
  cost per batch; `OFFSET` gets slower the further into the table it goes.
- **Make batch size a config value.** Tuned against real hardware, not
  guessed once and hardcoded.
- **Scale out with `parallelism`, not autoscaling.** If one process isn't
  enough throughput, Container Apps Jobs can run N replicas per schedule
  trigger, each handling a partitioned slice. Autoscaling is the wrong tool
  here, it's built for long-running services reacting to live load, not a
  batch job that starts, runs, and exits.

**3. Making this supportable for a team.** The goal is that a bad change
gets caught before it reaches production, and a knowledgeable person other
than me can operate this.

- **CI gate before merge.** Lint + unit tests on the engine, an integration
  test against a fixture DB, a Trivy scan on the built image, and a
  `terraform plan` check.
- **Monitoring tied to specific failures.** Surface `run_summary.json`'s
  reject counts wherever the team already watches its systems, with alerts
  tied to specific reject reasons so a spike is diagnosable, not just
  visible.
- **Config reviewed like code.** Config changes go through the same PR
  review as code, with schema validation in CI so a bad field name fails in
  review, not in production.
- **One version-controlled repo.** Pipeline code and every feed's config
  live together, so a bad change to either is one `git revert` away from
  being undone.

### What I'd do with more time

- Add an actual automated test suite (unit tests for the SQL-generation
  functions, an integration test against a fixture DB) rather than the manual
  adversarial testing I did during development.
- Case-insensitive matching for `allowed_values` (currently exact-match only,
  so e.g. `"Cardiology"` is rejected, which is arguably too strict for
  real-world upstream data).
- Validate `config.yaml` against a schema in CI, ahead of the fail-fast checks
  the pipeline already does at runtime.
- Wire up a CI pipeline to actually run the Docker build, `terraform plan`, and
  Trivy scan on every PR, this was all done manually for this submission.
