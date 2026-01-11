from __future__ import annotations

import os
import re
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote_plus, urlencode, urljoin

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# -----------------------------
# Load .env (backend/.env)
# -----------------------------
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env", override=False)

# -----------------------------
# Config
# -----------------------------
COUNTRY = "NO"
RUN_TYPE = "proff_build_batch_ebit2024"
PHASE = "search"

DEFAULT_PROFF_BASE_URL = os.getenv("PROFF_BASE_URL", "https://api.proff.no").rstrip("/")
PROFF_API_KEY = os.getenv("PROFF_API_KEY")

# Proff search endpoint (NOTE: includes /api)
REGISTER_SEARCH_URL = f"{DEFAULT_PROFF_BASE_URL}/api/companies/register/{COUNTRY}"

# defaults for the "2396 companies with EBIT>50m in 2024"
DEFAULT_YEAR = 2024
DEFAULT_ACCOUNT_CODE = "DR"          # Operating profit (EBIT) from your Regnkoder sheet
DEFAULT_MIN_VALUE = 50_000           # NB, Norwegian accounts (inc proff) typically measures in kNOK (50_000 = 50 MNOK)

# Pagination / throttling
PAGE_SIZE = int(os.getenv("PROFF_PAGE_SIZE", "100"))
MAX_RETRIES = 6
BACKOFF_BASE_SECONDS = 1.0
REQUEST_TIMEOUT = 30

CHECKPOINT_EVERY_PAGES = 1  # checkpoint after every page, cheap and safe


# -----------------------------
# DB Engine (same pattern you already use)
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
# Proff client
# -----------------------------
class ProffClient:
    def __init__(self, api_key: str):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Token {api_key}",
            "Accept": "application/json",
            "api-version": os.getenv("PROFF_API_VERSION", "1.1"),
        })

    def get(self, url: str, params: Optional[dict[str, Any]] = None) -> requests.Response:
        for attempt in range(MAX_RETRIES):
            try:
                r = self.s.get(url, params=params, timeout=REQUEST_TIMEOUT)
            except requests.RequestException:
                self._sleep(attempt)
                continue

            if r.status_code in (429, 500, 502, 503, 504):
                self._sleep(attempt, retry_after=r.headers.get("Retry-After"))
                continue

            return r

        # final attempt (no swallow)
        r = self.s.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return r

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
# Helpers
# -----------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_orgnr(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = "".join(ch for ch in str(x) if ch.isdigit())
    return s if len(s) == 9 else None

def extract_orgnrs_from_search_response(data: dict[str, Any]) -> list[str]:
    """
    Proff search response typically contains a list of companies.
    We try multiple common keys to be schema-tolerant.
    """
    candidates: list[Any] = []
    for key in ("companies", "results", "items", "hits", "entities"):
        if isinstance(data.get(key), list):
            candidates = data[key]
            break

    orgnrs: list[str] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        # common orgnr keys
        for k in ("organisationNumber", "organizationNumber", "orgnr", "id", "businessId"):
            orgnr = normalize_orgnr(c.get(k))
            if orgnr:
                orgnrs.append(orgnr)
                break

    # de-dupe while preserving order
    seen = set()
    out = []
    for o in orgnrs:
        if o not in seen:
            seen.add(o)
            out.append(o)
    return out

def get_next_href(data: dict[str, Any]) -> Optional[str]:
    """
    Pagination doc says: pagination.next.href
    """
    pagination = data.get("pagination") or {}
    nxt = pagination.get("next") or {}
    href = nxt.get("href")
    if isinstance(href, str) and href.strip():
        return href
    return None


def build_url(base_url: str, params: dict[str, Any]) -> str:
    qs = urlencode(params, safe="|:", quote_via=quote_plus)
    return f"{base_url}?{qs}"


def find_working_account_scope(
    client: ProffClient,
    base_url: str,
    code: str,
    year: int,
    min_value: int,
) -> str:
    """
    Probes which 3rd token Proff accepts in the accounts filter.
    We try several likely values and pick the first that returns 200.
    """
    range_value = f"{min_value}:"

    candidates = [
        os.getenv("PROFF_ACCOUNT_VIEW"),
        "companyAccounts",
        "annualAccounts",
        "corporateAccounts",
        "company",
        "annual",
        "corporate",
        "COMPANY",
        "ANNUAL",
        "CORPORATE",
    ]
    candidates = [c for c in candidates if c]

    last_error = None
    for scope in candidates:
        params = {
            "pageSize": 1,
            "accounts": f"{code}|{year}|{scope}",
            "accountRange": range_value,
        }
        url = build_url(base_url, params)
        r = client.get(url, params=None)
        if r.status_code == 200:
            return scope
        last_error = f"{r.status_code} {r.text[:300]}"

    raise RuntimeError(
        "Could not find a working accounts scope token. "
        f"Last response: {last_error}"
    )


# -----------------------------
# SQL MERGE statements
# -----------------------------
MERGE_IMPORT_BATCH = """
MERGE dbo.import_batch WITH (HOLDLOCK) AS tgt
USING (SELECT :batch_name AS batch_name, :criteria AS criteria) AS src
ON tgt.batch_name = src.batch_name
WHEN MATCHED THEN
    UPDATE SET criteria = COALESCE(src.criteria, tgt.criteria)
WHEN NOT MATCHED THEN
    INSERT (batch_name, criteria) VALUES (src.batch_name, src.criteria);
"""

GET_BATCH_ID = """
SELECT batch_id FROM dbo.import_batch WHERE batch_name = :batch_name;
"""

MERGE_BATCH_ITEM = """
MERGE dbo.import_batch_item WITH (HOLDLOCK) AS tgt
USING (SELECT :batch_id AS batch_id, :orgnr AS orgnr, :include_reason AS include_reason) AS src
ON tgt.batch_id = src.batch_id AND tgt.orgnr = src.orgnr
WHEN MATCHED THEN
    UPDATE SET include_reason = COALESCE(src.include_reason, tgt.include_reason)
WHEN NOT MATCHED THEN
    INSERT (batch_id, orgnr, include_reason) VALUES (src.batch_id, src.orgnr, src.include_reason);
"""

INSERT_RUN = """
INSERT INTO dbo.ingestion_run(run_type, batch_name, status, started_at_utc)
OUTPUT inserted.run_id
VALUES (:run_type, :batch_name, 'running', SYSUTCDATETIME());
"""

MERGE_CHECKPOINT = """
MERGE dbo.ingestion_checkpoint WITH (HOLDLOCK) AS tgt
USING (SELECT
    :run_id AS run_id,
    :phase AS phase,
    :last_orgnr AS last_orgnr,
    :last_offset AS last_offset,
    :last_cursor AS last_cursor,
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

GET_CHECKPOINT_CURSOR = """
SELECT last_cursor
FROM dbo.ingestion_checkpoint
WHERE run_id = :run_id AND phase = :phase;
"""

FINISH_RUN = """
UPDATE dbo.ingestion_run
SET status = :status,
    finished_at_utc = SYSUTCDATETIME(),
    notes = COALESCE(:notes, notes)
WHERE run_id = :run_id;
"""


# -----------------------------
# Main
# -----------------------------
def main():
    if not PROFF_API_KEY:
        raise SystemExit("PROFF_API_KEY is not set (env or backend/.env).")

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-name", default="ebit_gt_50_2024", help="import batch name")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    parser.add_argument("--account-code", default=DEFAULT_ACCOUNT_CODE)
    parser.add_argument("--min-value", type=int, default=DEFAULT_MIN_VALUE)
    parser.add_argument("--resume", action="store_true", help="resume using checkpoint cursor within this run")
    parser.add_argument("--limit-pages", type=int, default=None, help="for testing: stop after N pages")
    args = parser.parse_args()

    engine = make_engine()
    client = ProffClient(PROFF_API_KEY)

    # Sanity check: should return 200, not 401
    test_url = f"{DEFAULT_PROFF_BASE_URL}/api/companies/register/NO"
    r = client.get(test_url, params={"pageSize": 1})
    print("Auth test:", r.status_code, r.text[:200])
    if r.status_code == 401:
        raise SystemExit("Proff token rejected (401). Check PROFF_API_KEY and whether trial is activated.")


    batch_name = args.batch_name
    year = args.year
    code = args.account_code
    min_value = args.min_value

    account_range_env = os.getenv("PROFF_ACCOUNT_RANGE")
    if account_range_env:
        account_range_value = account_range_env
    else:
        max_value = os.getenv("PROFF_MAX_VALUE", "9999999999999")
        account_range_value = f"{code}|{year}|{min_value}:{max_value}"

    # Optional extra query params (JSON dict) for segmentation
    # Example: {"companyTypes":"AS,ASA","status":"active"} depending on what Proff supports in Swagger.
    extra_params_raw = os.getenv("PROFF_REGISTER_EXTRA_PARAMS")
    extra_params = {}
    if extra_params_raw:
        try:
            extra_params = json.loads(extra_params_raw)
            if not isinstance(extra_params, dict):
                extra_params = {}
        except Exception:
            extra_params = {}

    # 1) Create run + batch
    with engine.begin() as conn:
        conn.execute(text(MERGE_IMPORT_BATCH), {
            "batch_name": batch_name,
            "criteria": f"{code} (EBIT) >= {min_value} in {year} via Proff RegisterCompany",
        })
        batch_id = conn.execute(text(GET_BATCH_ID), {"batch_name": batch_name}).scalar_one()

        run_id = conn.execute(text(INSERT_RUN), {"run_type": RUN_TYPE, "batch_name": batch_name}).scalar_one()
        run_id = str(run_id)

    # 2) Build first request
    # Which account view to use in search: Proff expects 'company' or 'corporate'
    accounts_view = os.getenv("PROFF_ACCOUNTS_VIEW", "company")
    if accounts_view not in ("company", "corporate"):
        raise SystemExit("PROFF_ACCOUNTS_VIEW must be 'company' or 'corporate'")
    accounts_param = code  # f.eks. "DR"
    account_range_param = f"{code}|{year}|{min_value}:9999999999999"

    params = {
        "pageSize": PAGE_SIZE,
        "accounts": accounts_view,
        "accountRange": account_range_value,
    }
    params.update(extra_params)
#    print(f"[{now_utc_iso()}] Using accounts scope token: {account_scope}")
    start_url = build_url(REGISTER_SEARCH_URL, params)
    next_url = start_url
    next_params = None

    probe = client.get(next_url, params=None)
    print("Probe:", probe.status_code)
    print("Probe URL:", probe.request.url)
    print("Probe body:", probe.text[:300])
    if probe.status_code != 200:
        raise SystemExit("Probe failed; adjust accounts/accountRange.")

    # 3) Resume cursor (within this run)
    if args.resume:
        with engine.begin() as conn:
            cur = conn.execute(text(GET_CHECKPOINT_CURSOR), {"run_id": run_id, "phase": PHASE}).scalar()
        if cur:
            next_url = cur
            next_params = None  # already baked into href
            print(f"[{now_utc_iso()}] Resuming from cursor: {next_url}")

    print(f"[{now_utc_iso()}] Run {run_id} starting batch '{batch_name}' (batch_id={batch_id}).")
    print(f"[{now_utc_iso()}] First request: {start_url} params={params}")

    inserted_total = 0
    page = 0
    last_orgnr = None

    try:
        while next_url:
            page += 1
            if args.limit_pages and page > args.limit_pages:
                break

            r = client.get(next_url, params=None)
            if not r.ok:
                raise RuntimeError(
                    f"Proff search failed: {r.status_code} {r.text[:500]}\n"
                    f"Request URL: {r.request.url}"
                )

            data = r.json()
            orgnrs = extract_orgnrs_from_search_response(data)

            if orgnrs:
                last_orgnr = orgnrs[-1]

            # Upsert items
            with engine.begin() as conn:
                for orgnr in orgnrs:
                    conn.execute(text(MERGE_BATCH_ITEM), {
                        "batch_id": batch_id,
                        "orgnr": orgnr,
                        "include_reason": f"{code}>={min_value} ({year})",
                    })

                # checkpoint cursor = next.href if present, else None
                cursor = get_next_href(data)
                conn.execute(text(MERGE_CHECKPOINT), {
                    "run_id": run_id,
                    "phase": PHASE,
                    "last_orgnr": last_orgnr,
                    "last_offset": page,
                    "last_cursor": cursor,
                })

            inserted_total += len(orgnrs)

            # Determine next page
            href = get_next_href(data)
            if href:
                # href may be absolute or relative
                next_url = href if href.startswith("http") else urljoin(DEFAULT_PROFF_BASE_URL, href)
                next_params = None  # href already contains query string in most cases
            else:
                next_url = None

            hits = data.get("numberOfHits")
            print(f"[{now_utc_iso()}] Page {page}: got {len(orgnrs)} orgnrs (total so far ~{inserted_total}). numberOfHits={hits}")

        with engine.begin() as conn:
            conn.execute(text(FINISH_RUN), {
                "run_id": run_id,
                "status": "succeeded",
                "notes": f"Inserted/updated ~{inserted_total} orgnrs into batch '{batch_name}'. Pages: {page}",
            })

        print(f"[{now_utc_iso()}] Run {run_id} succeeded. Total orgnrs processed (including duplicates across pages) ~{inserted_total}.")
        print("Tip: The de-duplicated count is in SQL: SELECT COUNT(*) FROM dbo.import_batch_item WHERE batch_id=...")

    except Exception as e:
        with engine.begin() as conn:
            conn.execute(text(FINISH_RUN), {
                "run_id": run_id,
                "status": "failed",
                "notes": f"Error: {e}",
            })
        raise


if __name__ == "__main__":
    main()
