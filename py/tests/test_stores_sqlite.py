from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.jobs.manual import upsert_batch, upsert_incremental
from src.stores.bond_curves import BondCurveStore
from src.stores.policy_rates import PolicyRateStore


class StoresSqliteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.sqlite"

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_policy_rate_dry_run(self) -> None:
        store = PolicyRateStore(db_path=self.db_path)
        stats = store.upsert_many(
            [
                {
                    "date": "2026-03-24",
                    "type": "OMO",
                    "term": "7D",
                    "rate": "1.40",
                    "amount": "485",
                    "source": "pbc",
                }
            ],
            dry_run=True,
        )
        self.assertEqual(stats.inserted, 1)
        self.assertEqual(store.count_rows(), 0)

    def test_policy_rate_upsert_and_incremental(self) -> None:
        rows = [
            {"date": "2026-03-20", "type": "OMO", "term": "7D", "rate": 1.40, "amount": 100},
            {"date": "2026-03-21", "type": "OMO", "term": "7D", "rate": 1.50, "amount": 110},
        ]
        stats1 = upsert_batch("raw_policy_rate", rows, dry_run=False, db_path=self.db_path)
        self.assertEqual(stats1.inserted, 2)
        stats2 = upsert_incremental(
            "raw_policy_rate",
            [
                {"date": "2026-03-21", "type": "OMO", "term": "7D", "rate": 1.50, "amount": 110},
                {"date": "2026-03-22", "type": "OMO", "term": "7D", "rate": 1.60, "amount": 120},
            ],
            dry_run=False,
            db_path=self.db_path,
        )
        self.assertEqual(stats2.inserted, 1)
        self.assertEqual(stats2.skipped, 0)
        store = PolicyRateStore(db_path=self.db_path)
        self.assertEqual(store.count_rows(), 3)

    def test_bond_curve_spread_series(self) -> None:
        store = BondCurveStore(db_path=self.db_path)
        stats = store.upsert_many(
            [
                {"date": "2026-03-20", "curve": "国债", "y_1": 1.2, "y_10": 1.8},
                {"date": "2026-03-21", "curve": "国债", "y_1": 1.1, "y_10": 1.9},
            ]
        )
        self.assertEqual(stats.inserted, 2)
        series = store.fetch_term_spread_series("国债", "y_10", "y_1", limit=2)
        self.assertEqual(len(series), 2)
        self.assertAlmostEqual(series[0]["spread"], 0.8)

    def test_clean_normalizes_numbers(self) -> None:
        store = PolicyRateStore(db_path=self.db_path)
        store.upsert_many([
            {"date": "2026/03/24", "type": " OMO ", "term": " 7D ", "rate": "1.40%", "amount": "485"}
        ])
        stats = store.clean_data(dry_run=False)
        self.assertGreaterEqual(stats.updated + stats.skipped, 1)
        row = store.get_by_key(date="2026-03-24", type="OMO", term="7D")
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["date"], "2026-03-24")
        self.assertEqual(row["type"], "OMO")
        self.assertEqual(row["term"], "7D")
        self.assertAlmostEqual(row["rate"], 1.4)


if __name__ == "__main__":
    unittest.main()
