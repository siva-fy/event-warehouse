# Event Warehouse

**Turn messy payment events into a replay-safe daily revenue mart.**

[![CI](https://github.com/siva-fy/event-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/siva-fy/event-warehouse/actions/workflows/ci.yml)

A local data engineering reference implementation: JSONL ingestion, retained raw
payloads, validation, quarantine, deduplication and a SQL reporting layer. Built
with Python and SQLite so the entire data flow can be inspected without cloud
credentials.

## Run in a minute

Python 3.11+ with SQLite support; no pip packages or API keys.

~~~bash
git clone https://github.com/siva-fy/event-warehouse.git
cd event-warehouse
python3 demo.py
python3 -m unittest discover -s tests -v
~~~

The demo uses an in-memory database and is safe to repeat. It accepts **4 events**,
quarantines **1 invalid event**, and identifies **1 duplicate**. Replaying the same
batch adds no payments. The mart contains **18,000 USD minor units** and
**50,000 INR minor units**, on separate rows.

## Data flow

~~~mermaid
flowchart LR
  A[JSONL events] --> B[Canonical payload and SHA-256]
  B --> C[Raw event ledger]
  C --> D{Validate and deduplicate}
  D -->|Valid| E[Payments table]
  D -->|Invalid or conflicting| F[Quarantine]
  E --> G[Daily revenue SQL view]
~~~

## Ingest your own batch

~~~bash
python3 warehouse.py examples/events.jsonl --db warehouse.db
python3 warehouse.py examples/events.jsonl --db warehouse.db
~~~

Required fields: `event_id`, `customer_id`, timezone-aware `occurred_at`,
three-letter uppercase `currency`, `kind` (`payment` or `refund`) and
`amount_minor` (nonnegative integer, at most 10^12). Use currency minor units;
the code does not assume all currencies have two decimal places.

| Table/view | Purpose |
| --- | --- |
| `raw_events` | One retained canonical payload per digest, including rejected inputs |
| `rejected` | Reason for each quarantined payload |
| `payments` | Accepted events with unique event IDs and UTC timestamps |
| `daily_revenue` | Signed payment/refund totals grouped by UTC day and currency |

The exact same payload is a duplicate even if JSON keys are reordered. A different
payload with an accepted event ID is quarantined. A later correction to an event
that was rejected may be accepted because rejected IDs do not reserve a payment.
Unexpected source exceptions roll back the whole batch.

## Design tradeoffs

This is a single-process batch reference, not a streaming platform or accounting
ledger. It does not reconcile refunds with original payments, convert currencies,
validate ISO currency membership, enforce retention, or orchestrate jobs.
`raw_events` deduplicates deliveries rather than keeping every delivery attempt.
The SQL view recalculates totals; an incremental materialization is a future
extension. Large aggregate totals remain subject to SQLite integer limits.

## Verify and extend

Tests cover replay, ID collisions, UTC date boundaries, invalid input, currency
isolation, refund signs and transaction rollback. CI runs tests and the demo on
Python 3.11, 3.12 and 3.13. See [actual demo output](examples/demo-output.txt).

Useful next steps: add a source adapter, partition raw storage by ingestion date,
add a scheduler, and compare incremental aggregates with a full recomputation.

Original AI-assisted portfolio project using synthetic data.
[Provenance](PROVENANCE.md) · [MIT license](LICENSE) ·
[Python SQLite documentation](https://docs.python.org/3/library/sqlite3.html)
