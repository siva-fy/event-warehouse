"""Replay-safe event ingestion and currency-separated revenue marts."""
import argparse
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_events (
    digest TEXT PRIMARY KEY, payload TEXT NOT NULL, disposition TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rejected (
    digest TEXT PRIMARY KEY, reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS payments (
    event_id TEXT PRIMARY KEY, digest TEXT NOT NULL, customer_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL, currency TEXT NOT NULL,
    kind TEXT NOT NULL, amount_minor INTEGER NOT NULL);
CREATE VIEW IF NOT EXISTS daily_revenue AS
SELECT substr(occurred_at,1,10) AS day, currency, count(*) AS events,
       sum(CASE WHEN kind='refund' THEN -amount_minor ELSE amount_minor END)
       AS net_amount_minor
FROM payments GROUP BY day, currency;
"""


def connect(path=":memory:"):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def validate(event):
    if not isinstance(event, dict):
        raise ValueError("event must be an object")
    for field in ("event_id", "customer_id", "occurred_at", "currency", "kind"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise ValueError(f"{field} must be a nonempty string")
    if event["kind"] not in ("payment", "refund"):
        raise ValueError("kind must be payment or refund")
    if not re.fullmatch(r"[A-Z]{3}", event["currency"]):
        raise ValueError("currency must be three uppercase letters")
    amount = event.get("amount_minor")
    if type(amount) is not int or not 0 <= amount <= 10**12:
        raise ValueError("amount_minor must be an integer between 0 and 10^12")
    try:
        timestamp = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("occurred_at must be ISO 8601") from exc
    if timestamp.tzinfo is None:
        raise ValueError("occurred_at must include timezone")
    return timestamp.astimezone(timezone.utc).isoformat()


def ingest(conn, lines):
    """One transaction per input batch; all raw payloads are retained by digest."""
    counts = dict(accepted=0, duplicate=0, rejected=0)
    with conn:
        for line in lines:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                payload = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
            except (ValueError, TypeError):
                event, payload = None, line.strip()
            digest = hashlib.sha256(payload.encode()).hexdigest()
            if conn.execute("SELECT 1 FROM raw_events WHERE digest=?", (digest,)).fetchone():
                counts["duplicate"] += 1
                continue
            reason = None
            try:
                occurred = validate(event)
                existing = conn.execute("SELECT digest FROM payments WHERE event_id=?",
                                        (event["event_id"],)).fetchone()
                if existing:
                    raise ValueError("event_id conflicts with a previously accepted payload")
            except ValueError as exc:
                reason = str(exc)
            disposition = "rejected" if reason else "accepted"
            conn.execute("INSERT INTO raw_events VALUES (?,?,?)", (digest, payload, disposition))
            if reason:
                conn.execute("INSERT INTO rejected VALUES (?,?)", (digest, reason))
            else:
                conn.execute("INSERT INTO payments VALUES (?,?,?,?,?,?,?)",
                             (event["event_id"], digest, event["customer_id"], occurred,
                              event["currency"], event["kind"], event["amount_minor"]))
            counts[disposition] += 1
    return counts


def report(conn):
    return [dict(row) for row in conn.execute("SELECT * FROM daily_revenue ORDER BY day,currency")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--db", default="warehouse.db")
    args = parser.parse_args()
    conn = connect(args.db)
    try:
        with args.input.open() as stream:
            summary = ingest(conn, stream)
        print(json.dumps({"ingestion": summary, "daily_revenue": report(conn)}, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
