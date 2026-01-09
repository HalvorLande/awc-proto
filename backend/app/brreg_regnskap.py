from __future__ import annotations

from typing import Any, Optional
import requests
import time

BASE = "https://data.brreg.no"


def normalize_orgnr(orgnr: str) -> str:
    return "".join(ch for ch in orgnr if ch.isdigit())

BASE = "https://data.brreg.no"

def get_regnskap(orgnr: str, year: Optional[int] = None) -> dict[str, Any]:
    url = f"{BASE}/regnskapsregisteret/regnskap/{orgnr}"
    param_variants = [
        {},  # no regnskapstype
        {"regnskapstype": "SELSKAP"},
        {"regnskapstype": "KONSERN"},
    ]

    if year is not None:
        for p in param_variants:
            p["år"] = int(year)

    last_exc = None
    for params in param_variants:
        # simple retry for 500
        for attempt in range(3):
            r = requests.get(url, params=params, timeout=30)
            if r.status_code >= 500:
                time.sleep(0.5 * (2 ** attempt))
                continue
            if r.status_code == 404:
                # no published accounts for this orgnr in this API
                raise requests.HTTPError("404 Not Found", response=r)
            r.raise_for_status()
            return r.json()

        last_exc = requests.HTTPError(f"{r.status_code} error after retries", response=r)

    raise last_exc or RuntimeError("BRREG regnskap request failed unexpectedly")



def extract_metrics(regnskap: dict[str, Any]) -> dict[str, Any]:
    """
    Extract a small set of metrics in a robust way (returns None if missing).
    This is deliberately forgiving because BRREG payloads may differ across company types.
    """
    rr = regnskap.get("resultatregnskapResultat") or {}
    drifts = rr.get("driftsresultat") or {}
    inntekter = drifts.get("driftsinntekter") or {}

    # Revenue candidates (BRREG structures can vary)
    revenue = (
        inntekter.get("sumDriftsinntekter")
        or inntekter.get("salgsinntekter")
        or inntekter.get("andreDriftsinntekter")
    )

    ebit = drifts.get("driftsresultat")

    eiendeler = regnskap.get("eiendeler") or {}
    assets = eiendeler.get("sumEiendeler")

    ekg = regnskap.get("egenkapitalGjeld") or {}
    egenkap = ekg.get("egenkapital") or {}
    equity = egenkap.get("sumEgenkapital")

    gjeld = ekg.get("gjeldOversikt") or {}
    total_liabilities = gjeld.get("sumGjeld")

    periode = regnskap.get("regnskapsperiode") or {}
    period_to = periode.get("tilDato")

    aarsresultat = rr.get("aarsresultat")

    return {
        "revenue": revenue,
        "ebit": ebit,
        "assets": assets,
        "equity": equity,
        "total_liabilities": total_liabilities,
        "period_to": period_to,
        "aarsresultat": aarsresultat,
    }
