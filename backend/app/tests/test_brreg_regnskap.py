from __future__ import annotations

import unittest
import requests
from datetime import datetime

from app.brreg_regnskap import get_regnskap, extract_metrics, normalize_orgnr

class TestBrreg(unittest.TestCase):
    def test_brreg_live(self):
        try:
            data = get_regnskap("916823525")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code >= 500:
                self.skipTest(f"BRREG is returning {e.response.status_code} for this orgnr right now.")
            raise

class TestBrregRegnskapIntegration(unittest.TestCase):
    """
    Integration test: calls the live BRREG API.
    This will fail if you have no internet, BRREG is down, or your network blocks it.
    """

    ORGNR = "916 823 525"

    def test_get_regnskap_returns_dict(self):
        orgnr = normalize_orgnr(self.ORGNR)
        data = get_regnskap(orgnr)
        self.assertIsInstance(data, dict)
        # We only assert a couple of likely top-level keys (but keep it tolerant)
        self.assertTrue(
            ("regnskapsperiode" in data) or ("egenkapitalGjeld" in data) or ("resultatregnskapResultat" in data),
            f"Unexpected BRREG payload keys: {list(data.keys())[:30]}",
        )

    def test_extract_metrics_returns_expected_shape(self):
        orgnr = normalize_orgnr(self.ORGNR)
        data = get_regnskap(orgnr)
        metrics = extract_metrics(data)

        self.assertIsInstance(metrics, dict)

        expected_keys = {"revenue", "ebit", "assets", "equity", "total_liabilities", "period_to", "aarsresultat"}
        self.assertTrue(expected_keys.issubset(metrics.keys()), f"Missing keys: {expected_keys - set(metrics.keys())}")

        # period_to is useful as a year-marker; assert it's either None or ISO-like date
        period_to = metrics["period_to"]
        if period_to is not None:
            # usually "YYYY-MM-DD"
            self.assertGreaterEqual(len(str(period_to)), 10)
            # validate parse if it looks like a date string
            try:
                datetime.fromisoformat(str(period_to))
            except Exception as e:
                self.fail(f"period_to exists but is not ISO date-like: {period_to}. Error: {e}")

        # At least one of these core numbers should be present for a meaningful record
        core = [metrics["revenue"], metrics["ebit"], metrics["assets"], metrics["equity"]]
        self.assertTrue(any(v is not None for v in core), f"No core metrics present. Got: {metrics}")

    def test_get_regnskap_with_year_parameter_does_not_crash(self):
        orgnr = normalize_orgnr(self.ORGNR)
        # Even if the endpoint ignores year, this should still return a dict without raising
        data = get_regnskap(orgnr, year=2023)
        self.assertIsInstance(data, dict)


if __name__ == "__main__":
    unittest.main()
