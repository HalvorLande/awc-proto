from __future__ import annotations

import os
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Iterable

import requests
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

from dotenv import load_dotenv
from pathlib import Path

# Load .env from backend folder (or project root) deterministically
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env", override=False)

API_KEY = os.getenv("PROFF_API_KEY")
print("PROFF_API_KEY loaded:", "yes" if API_KEY else "no")
print("PROFF_API_KEY first6:", API_KEY[:6] if API_KEY else None)


# -----------------------------
# Config
# -----------------------------
DEFAULT_BASE_URL = os.getenv("PROFF_BASE_URL", "https://api.proff.no")
API_KEY = os.getenv("PROFF_API_KEY")  # REQUIRED
COUNTRY = "NO"
HISTORY_YEARS = 5

RUN_TYPE = "proff_backfill_details"
PHASE = "details"

# Tune these for your trial limits
MAX_RETRIES = 6
BACKOFF_BASE_SECONDS = 1.0
REQUEST_TIMEOUT = 30
CHECKPOINT_EVERY_N = 25


# -----------------------------
# DB Engine (your existing pattern)
# -----------------------------
def make_engine():
    server = os.getenv("SQL_SERVER", "AAD-GM12FD8W")
    database = os.getenv("SQL_DATABASE", "AwcProto")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    odbc_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str)
    return create_engine(url, future=True, pool_pre_ping=True)


# -----------------------------
# Proff client (Token auth + retries)
# -----------------------------
class ProffClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Token {api_key}",
            "Accept": "application/json",
            "api-version": os.getenv("PROFF_API_VERSION", "1.1"),
        })
        print("Proff headers:", dict(self.s.headers))


    def get_company_details(self, orgnr: str) -> tuple[int, dict[str, Any] | None, str]:
        """
        Returns: (http_status, json_payload_or_none, url)
        """
        url = f"{self.base_url}/api/companies/register/{COUNTRY}/{orgnr}"

        for attempt in range(MAX_RETRIES):
            try:
                r = self.s.get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException:
                self._sleep(attempt)
                continue
            if r.status_code == 401:
                raise RuntimeError(f"Proff returned 401 Invalid token. URL={url}")

            # Retry on throttling and transient server errors
            if r.status_code in (429, 500, 502, 503, 504):
                self._sleep(attempt, retry_after=r.headers.get("Retry-After"))
                continue

            if r.status_code == 404:
                return (404, None, url)

            if not r.ok:
                # Non-retryable error
                return (r.status_code, None, url)

            try:
                return (200, r.json(), url)
            except ValueError:
                return (200, None, url)

        # Exhausted retries
        return (599, None, url)

    @staticmethod
    def _sleep(attempt: int, retry_after: str | None = None):
        if retry_after:
            try:
                time.sleep(float(retry_after))
                return
            except ValueError:
                pass
        time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))


# -----------------------------
# Helpers: parse account payload into (year, view, code, value)
# -----------------------------
def iter_financial_items(payload: dict[str, Any], min_year: int) -> Iterable[dict[str, Any]]:
    """
    Yields dict rows suitable for dbo.proff_financial_item.

    This is intentionally defensive because Proff payload shapes can vary.
    We support two common patterns:
      A) account_view = [ { "year": 2024, "accounts": [ {"code": "...", "value": 123}, ... ] }, ... ]
      B) account_view = [ { "year": 2024, "accounts": { "CODE": 123, ... } }, ... ]
    """
    for view_key, view_name in (
        ("companyAccounts", "company"),
        ("corporateAccounts", "corporate"),
        ("annualAccounts", "annual"),
    ):
        items = payload.get(view_key) or []
        if not isinstance(items, list):
            continue

        for year_block in items:
            if not isinstance(year_block, dict):
                continue

            year = year_block.get("year") or year_block.get("fiscalYear") or year_block.get("accountYear")
            if year is None:
                # sometimes year is embedded in period end date
                continue

            try:
                year_int = int(year)
            except Exception:
                continue

            if year_int < min_year:
                continue

            accounts = year_block.get("accounts") or year_block.get("accountItems") or year_block.get("values")
            if accounts is None:
                continue

            # Pattern A: list of dicts
            if isinstance(accounts, list):
                for a in accounts:
                    if not isinstance(a, dict):
                        continue
                    code = a.get("code") or a.get("accountCode")
                    val = a.get("value") or a.get("amount")
                    if code is None:
                        continue
                    yield {
                        "orgnr": payload.get("id") or payload.get("orgnr"),  # we overwrite later anyway
                        "fiscal_year": year_int,
                        "account_view": view_name,
                        "code": str(code),
                        "value": _to_decimal(val),
                        "currency": a.get("currency"),
                        "unit": a.get("unit"),
                    }

            # Pattern B: dict of code->value
            elif isinstance(accounts, dict):
                for code, val in accounts.items():
                    yield {
                        "orgnr": payload.get("id") or payload.get("orgnr"),
                        "fiscal_year": year_int,
                        "account_view": view_name,
                        "code": str(code),
                        "value": _to_decimal(val),
                        "currency": year_block.get("currency"),
                        "unit": year_block.get("unit"),
                    }


def _to_decimal(val: Any):
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


# -----------------------------
# MERGE statements (SQL Server)
# -----------------------------
MERGE_RAW_COMPANY = """
MERGE dbo.proff_raw_company WITH (HOLDLOCK) AS tgt
USING (SELECT
    :orgnr           AS orgnr,
    :http_status     AS http_status,
    :source_url      AS source_url,
    :etag            AS etag,
    :payload_json    AS payload_json,
    SYSUTCDATETIME() AS fetched_at_utc
) AS src
ON tgt.orgnr = src.orgnr
WHEN MATCHED THEN UPDATE SET
    http_status    = src.http_status,
    source_url     = src.source_url,
    etag           = src.etag,
    payload_json   = src.payload_json,
    fetched_at_utc = src.fetched_at_utc
WHEN NOT MATCHED THEN INSERT (orgnr, http_status, source_url, etag, payload_json, fetched_at_utc)
VALUES (src.orgnr, src.http_status, src.source_url, src.etag, src.payload_json, src.fetched_at_utc);
"""

MERGE_COMPANY = """
MERGE dbo.company WITH (HOLDLOCK) AS tgt
USING (SELECT
    :orgnr               AS orgnr,
    :name                AS name,
    :nace                AS nace,
    :municipality        AS municipality,
    :website             AS website,
    :phone               AS phone,
    :email               AS email,
    :street              AS street,
    :postal_code         AS postal_code,
    :city                AS city,
    :country_code        AS country_code,
    :sector_code         AS sector_code,
    :is_public_sector    AS is_public_sector,
    :excluded_reason     AS excluded_reason,
    SYSUTCDATETIME()     AS last_proff_fetch_at_utc,
    SYSUTCDATETIME()     AS updated_at
) AS src
ON tgt.orgnr = src.orgnr
WHEN MATCHED THEN UPDATE SET
    name                  = COALESCE(src.name, tgt.name),
    nace                  = COALESCE(src.nace, tgt.nace),
    municipality          = COALESCE(src.municipality, tgt.municipality),
    website               = COALESCE(src.website, tgt.website),
    phone                 = COALESCE(src.phone, tgt.phone),
    email                 = COALESCE(src.email, tgt.email),
    street                = COALESCE(src.street, tgt.street),
    postal_code           = COALESCE(src.postal_code, tgt.postal_code),
    city                  = COALESCE(src.city, tgt.city),
    country_code          = COALESCE(src.country_code, tgt.country_code),
    sector_code           = COALESCE(src.sector_code, tgt.sector_code),
    is_public_sector      = COALESCE(src.is_public_sector, tgt.is_public_sector),
    excluded_reason       = COALESCE(src.excluded_reason, tgt.excluded_reason),
    last_proff_fetch_at_utc = src.last_proff_fetch_at_utc,
    updated_at            = src.updated_at
WHEN NOT MATCHED THEN INSERT (
    orgnr, name, nace, municipality, website, phone, email, street, postal_code, city,
    country_code, sector_code, is_public_sector, excluded_reason, created_at, updated_at, last_proff_fetch_at_utc
)
VALUES (
    src.orgnr, src.name, src.nace, src.municipality, src.website, src.phone, src.email, src.street, src.postal_code, src.city,
    src.country_code, src.sector_code, COALESCE(src.is_public_sector, 0), src.excluded_reason,
    SYSUTCDATETIME(), src.updated_at, src.last_proff_fetch_at_utc
);
"""

MERGE_FIN_ITEM = """
MERGE dbo.proff_financial_item WITH (HOLDLOCK) AS tgt
USING (SELECT
    :orgnr           AS orgnr,
    :fiscal_year     AS fiscal_year,
    :account_view    AS account_view,
    :code            AS code,
    :value           AS value,
    :currency        AS currency,
    :unit            AS unit,
    SYSUTCDATETIME() AS fetched_at_utc
) AS src
ON tgt.orgnr = src.orgnr
AND tgt.fiscal_year = src.fiscal_year
AND tgt.account_view = src.account_view
AND tgt.code = src.code
WHEN MATCHED THEN UPDATE SET
    value         = src.value,
    currency      = COALESCE(src.currency, tgt.currency),
    unit          = COALESCE(src.unit, tgt.unit),
    fetched_at_utc = src.fetched_at_utc
WHEN NOT MATCHED THEN INSERT (orgnr, fiscal_year, account_view, code, value, currency, unit, fetched_at_utc, source)
VALUES (src.orgnr, src.fiscal_year, src.account_view, src.code, src.value, src.currency, src.unit, src.fetched_at_utc, 'proff');
"""

MERGE_CHECKPOINT = """
MERGE dbo.ingestion_checkpoint WITH (HOLDLOCK) AS tgt
USING (SELECT
    :run_id          AS run_id,
    :phase           AS phase,
    :last_orgnr      AS last_orgnr,
    :last_offset     AS last_offset,
    :last_cursor     AS last_cursor,
    SYSUTCDATETIME() AS updated_at_utc
) AS src
ON tgt.run_id = src.run_id AND tgt.phase = src.phase
WHEN MATCHED THEN UPDATE SET
    last_orgnr = src.last_orgnr,
    last_offset = src.last_offset,
    last_cursor = src.last_cursor,
    updated_at_utc = src.updated_at_utc
WHEN NOT MATCHED THEN INSERT (run_id, phase, last_orgnr, last_offset, last_cursor, updated_at_utc)
VALUES (src.run_id, src.phase, src.last_orgnr, src.last_offset, src.last_cursor, src.updated_at_utc);
"""


# -----------------------------
# DB helpers
# -----------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_or_create_run(conn, batch_name: str | None) -> str:
    # Always start a new run for prototype simplicity
    run_id = conn.execute(
        text(
            """
            INSERT INTO dbo.ingestion_run(run_type, batch_name, status, started_at_utc)
            OUTPUT inserted.run_id
            VALUES (:run_type, :batch_name, 'running', SYSUTCDATETIME());
            """
        ),
        {"run_type": RUN_TYPE, "batch_name": batch_name},
    ).scalar_one()
    return str(run_id)


def get_checkpoint_last_orgnr(conn, run_id: str) -> str | None:
    row = conn.execute(
        text(
            """
            SELECT last_orgnr
            FROM dbo.ingestion_checkpoint
            WHERE run_id = :run_id AND phase = :phase;
            """
        ),
        {"run_id": run_id, "phase": PHASE},
    ).fetchone()
    return row[0] if row else None


def load_batch_orgnrs(conn, batch_name: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT i.orgnr
            FROM dbo.import_batch b
            JOIN dbo.import_batch_item i ON i.batch_id = b.batch_id
            WHERE b.batch_name = :batch_name
            ORDER BY i.orgnr ASC;
            """
        ),
        {"batch_name": batch_name},
    ).fetchall()
    return [r[0] for r in rows]


def finish_run(conn, run_id: str, status: str, notes: str | None = None):
    conn.execute(
        text(
            """
            UPDATE dbo.ingestion_run
            SET status = :status,
                finished_at_utc = SYSUTCDATETIME(),
                notes = COALESCE(:notes, notes)
            WHERE run_id = :run_id;
            """
        ),
        {"run_id": run_id, "status": status, "notes": notes},
    )


# -----------------------------
# Payload -> normalized company mapping (TODO: align to Proff schema)
# -----------------------------
def map_company_fields(orgnr: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Map Proff payload into dbo.company fields.
    This is a skeleton mapper; update field paths to match Proff payload.
    """
    # Common-ish patterns; adjust after you inspect one payload in proff_raw_company
    name = payload.get("name") or payload.get("companyName") or payload.get("company", {}).get("name")
    nace = payload.get("nace") or payload.get("naceCode") or payload.get("industryCode")
    municipality = payload.get("municipality") or payload.get("kommune")
    website = payload.get("website") or payload.get("homepage")
    phone = payload.get("phone") or payload.get("telephone")
    email = payload.get("email")

    addr = payload.get("address") or {}
    street = addr.get("street") or addr.get("streetAddress")
    postal_code = addr.get("postalCode") or addr.get("zip")
    city = addr.get("city") or addr.get("postOffice")

    sector_code = payload.get("sectorCode") or payload.get("sector") or None

    # Public sector exclusion: if Proff provides a flag, map it here; else leave 0 and fill later by rules.
    is_public_sector = bool(payload.get("isPublicSector", False))
    excluded_reason = "public_sector" if is_public_sector else None

    return {
        "orgnr": orgnr,
        "name": name,
        "nace": nace,
        "municipality": municipality,
        "website": website,
        "phone": phone,
        "email": email,
        "street": street,
        "postal_code": postal_code,
        "city": city,
        "country_code": COUNTRY,
        "sector_code": sector_code,
        "is_public_sector": 1 if is_public_sector else 0,
        "excluded_reason": excluded_reason,
    }


# -----------------------------
# Main
# -----------------------------
def main():
    if not API_KEY:
        raise SystemExit("PROFF_API_KEY is not set in environment.")

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True, help="import_batch.batch_name to process")
    parser.add_argument("--limit", type=int, default=None, help="Process only N companies (for testing)")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint in this run (default: start fresh run)")
    args = parser.parse_args()

    engine = make_engine()
    client = ProffClient(DEFAULT_BASE_URL, API_KEY)

    with engine.begin() as conn:
        run_id = get_or_create_run(conn, args.batch)
        orgnrs = load_batch_orgnrs(conn, args.batch)

    # Determine resume point (we resume within this run_id by checkpoint)
    start_after = None
    with engine.begin() as conn:
        start_after = get_checkpoint_last_orgnr(conn, run_id) if args.resume else None

    # Apply resume filter
    if start_after:
        orgnrs = [o for o in orgnrs if o > start_after]

    if args.limit:
        orgnrs = orgnrs[: args.limit]

    print(f"[{now_utc_iso()}] Run {run_id} starting. Companies to process: {len(orgnrs)}")

    processed = 0
    min_year = datetime.now().year - HISTORY_YEARS

    try:
        for orgnr in orgnrs:
            status, payload, url = client.get_company_details(orgnr)

            payload_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None

            with engine.begin() as conn:
                # 1) Store raw payload (even on errors, store status + url)
                conn.execute(
                    text(MERGE_RAW_COMPANY),
                    {
                        "orgnr": orgnr,
                        "http_status": status,
                        "source_url": url,
                        "etag": None,
                        "payload_json": payload_json,
                    },
                )

                # 2) Upsert normalized company data (only if payload present)
                if status == 200 and isinstance(payload, dict):
                    company_row = map_company_fields(orgnr, payload)
                    conn.execute(text(MERGE_COMPANY), company_row)

                    # 3) Upsert financial items (all codes, last 5 years)
                    #    NOTE: iter_financial_items tries to handle multiple shapes;
                    #    adjust after inspecting stored raw JSON.
                    for item in iter_financial_items(payload, min_year=min_year):
                        item["orgnr"] = orgnr  # enforce
                        conn.execute(text(MERGE_FIN_ITEM), item)

            processed += 1

            # checkpoint every N
            if processed % CHECKPOINT_EVERY_N == 0:
                with engine.begin() as conn:
                    conn.execute(
                        text(MERGE_CHECKPOINT),
                        {
                            "run_id": run_id,
                            "phase": PHASE,
                            "last_orgnr": orgnr,
                            "last_offset": processed,
                            "last_cursor": None,
                        },
                    )
                print(f"[{now_utc_iso()}] Processed {processed}/{len(orgnrs)} (last={orgnr})")

        with engine.begin() as conn:
            finish_run(conn, run_id, "succeeded", notes=f"Processed {processed} companies.")
        print(f"[{now_utc_iso()}] Run {run_id} succeeded. Total processed: {processed}")

    except Exception as e:
        with engine.begin() as conn:
            finish_run(conn, run_id, "failed", notes=f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
