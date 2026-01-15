from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

UTILS_ROOT = PROJECT_ROOT / "utils"
if str(UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(UTILS_ROOT))

from app.db import SessionLocal
import llm


DEFAULT_LIMIT = 10


def build_prompt(company_name: str | None, orgnr: str, compounder_score: float, year: int) -> str:
    name = company_name or "Unknown company"
    return (
        "Estimate a deployability score for AWC AS to take a partial investment in this company.\n"
        "Definitions:\n"
        "- 0.0: Not possible for AWC AS to make a partial investment because the company is fully owned by a corporation (subsidiary).\n"
        "- 1.0: Ideal investment because the shareholder base is fragmented (mostly retail) or family-owned with a likely generational transfer that could need partial divestment.\n"
        "- 0.5: Single shareholder might invite AWC AS as minority shareholder to realize profit or raise capital.\n"
        "Return a JSON object ONLY with keys 'deployability' (number between 0 and 1) and 'explanation' (string).\n"
        f"Company name: {name}\n"
        f"Organization number: {orgnr}\n"
        f"Compounder score: {compounder_score}\n"
        f"Year: {year}\n"
    )


def extract_json_payload(response_text: str) -> dict[str, object] | None:
    response_text = response_text.strip()
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", response_text, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def normalize_payload(payload: dict[str, object]) -> tuple[float, str] | None:
    if "deployability" not in payload or "explanation" not in payload:
        return None

    try:
        deployability = float(payload["deployability"])
    except (TypeError, ValueError):
        return None

    explanation = str(payload["explanation"]).strip()
    if not explanation:
        return None

    deployability = max(0.0, min(1.0, deployability))
    return deployability, explanation


def fetch_top_scores(session: Session, limit: int) -> list[dict[str, object]]:
    rows = session.execute(
        text(
            """
            SELECT TOP (:limit)
                s.id,
                s.orgnr,
                s.year,
                s.compounder_score,
                c.name AS company_name
            FROM dbo.score AS s
            LEFT JOIN dbo.company AS c ON c.orgnr = s.orgnr
            ORDER BY s.compounder_score DESC
            """
        ),
        {"limit": limit},
    ).mappings()
    return list(rows)


def update_deployability(session: Session, score_id: int, deployability: float, explanation: str) -> None:
    session.execute(
        text(
            """
            UPDATE dbo.score
            SET deployability = :deployability,
                deployability_explanation = :explanation
            WHERE id = :score_id
            """
        ),
        {
            "deployability": deployability,
            "explanation": explanation,
            "score_id": score_id,
        },
    )


def run(limit: int) -> None:
    with SessionLocal() as session:
        rows = fetch_top_scores(session, limit)

    if not rows:
        print("No score rows found.")
        return

    with SessionLocal() as session:
        for row in rows:
            score_id = int(row["id"])
            orgnr = str(row["orgnr"])
            year = int(row["year"])
            compounder_score = float(row["compounder_score"])
            company_name = row.get("company_name")

            prompt = build_prompt(company_name, orgnr, compounder_score, year)
            response_text = llm.stream_grok_agent_response(prompt)
            payload = extract_json_payload(response_text)
            if payload is None:
                print(f"Skipping orgnr {orgnr}: could not parse JSON.")
                continue

            normalized = normalize_payload(payload)
            if normalized is None:
                print(f"Skipping orgnr {orgnr}: invalid payload {payload}.")
                continue

            deployability, explanation = normalized
            update_deployability(session, score_id, deployability, explanation)
            session.commit()
            print(
                f"Updated score id {score_id} (orgnr {orgnr}) with deployability {deployability:.2f}."
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Update deployability scores for top compounders.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of rows to update.")
    args = parser.parse_args()
    run(args.limit)


if __name__ == "__main__":
    main()
