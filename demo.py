import json
from pathlib import Path
from warehouse import connect, ingest, report

with connect() as db:
    lines = (Path(__file__).parent / "examples/events.jsonl").read_text().splitlines()
    first = ingest(db, lines)
    second = ingest(db, lines)
    result = report(db)
    assert first == {"accepted": 4, "duplicate": 1, "rejected": 1}
    assert second == {"accepted": 0, "duplicate": 6, "rejected": 0}
    assert [r["net_amount_minor"] for r in result] == [50000, 18000]
    print(json.dumps({"first_run": first, "replay": second, "mart": result}, indent=2))
