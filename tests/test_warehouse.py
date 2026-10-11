import json
import unittest
from warehouse import connect, ingest, report


def event(**changes):
    row = dict(event_id="e1", customer_id="c1", occurred_at="2026-01-02T01:00:00+05:30",
               currency="USD", kind="payment", amount_minor=1000)
    return json.dumps(row | changes)


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.db = connect()
        self.addCleanup(self.db.close)

    def test_replay_and_key_order_are_idempotent(self):
        first = event()
        second = json.dumps(dict(reversed(list(json.loads(first).items()))))
        self.assertEqual(ingest(self.db, [first, second])["duplicate"], 1)
        self.assertEqual(report(self.db)[0]["net_amount_minor"], 1000)

    def test_conflicting_id_is_quarantined(self):
        result = ingest(self.db, [event(), event(amount_minor=2000)])
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(report(self.db)[0]["net_amount_minor"], 1000)

    def test_utc_day_refunds_and_currency_isolation(self):
        ingest(self.db, [event(), event(event_id="e2", kind="refund", amount_minor=200),
                         event(event_id="e3", currency="INR")])
        rows = report(self.db)
        self.assertEqual([(r["day"], r["currency"], r["net_amount_minor"]) for r in rows],
                         [("2026-01-01", "INR", 1000), ("2026-01-01", "USD", 800)])

    def test_invalid_payloads_do_not_break_batch(self):
        bad = ["{broken", "[]", event(amount_minor=True), event(amount_minor=-5),
               event(occurred_at="2026-01-01"), event(currency="usd")]
        result = ingest(self.db, bad + [event()])
        self.assertEqual(result, {"accepted": 1, "duplicate": 0, "rejected": 6})

    def test_failure_rolls_back_entire_batch(self):
        def broken_stream():
            yield event()
            raise RuntimeError("source failed")
        with self.assertRaises(RuntimeError):
            ingest(self.db, broken_stream())
        self.assertEqual(report(self.db), [])

    def test_empty_batch(self):
        self.assertEqual(ingest(self.db, ["", " "])["accepted"], 0)


if __name__ == "__main__":
    unittest.main()
