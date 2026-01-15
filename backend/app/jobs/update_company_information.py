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


DEFAULT_LIMIT = 300


def build_prompt(company_name: str | None, orgnr: str, context_text: str = "") -> str:
    """
    Builds a prompt for the LLM to analyze a company's investability and urgency
    based on provided context (search results, news, financials).
    """
    name = company_name or "Unknown company"

    return (
        "You are an investment analyst for AWC (Awilhelmsen Capital). AWC is a long-term, family-owned investment company "
        "looking for minority positions (20-40%) in high-quality Norwegian companies (AS/ASA).\n\n"
        f"Analyze the following information about the company '{name}' (Org nr: {orgnr}):\n"
        f"--- START CONTEXT ---\n{context_text}\n--- END CONTEXT ---\n\n"
        "Based ONLY on the context above, generate a JSON response with the following 4 components:\n\n"
        "1. COMPANY DESCRIPTION\n"
        "   - Summarize what the company does and its industry.\n"
        "   - Describe its recent financial performance (revenue, EBIT, growth) if available.\n"
        "   - Identify the key shareholders (families, corporations, PE funds, or employees).\n\n"
        "2. INVESTABILITY SCORE (float 0.0 - 1.0)\n"
        "   Estimate the likelihood that AWC can secure a proprietary, minority investment.\n"
        "   - 0.0 (Locked): Wholly owned subsidiary of a strategic corporation (e.g., owned by Telenor or Equinor). No entry point.\n"
        "   - 0.3 (Auction Risk): Private Equity owned (likely 100% exit/auction) or Public Sector owned.\n"
        "   - 0.5 (Neutral): Single private owner who might need growth capital, but no clear transition signs.\n"
        "   - 0.8 (Attractive): Fragmented shareholder base (many small owners) or family-owned with potential for generational transfer.\n"
        "   - 1.0 (Ideal): Family-owned with clear generational succession issues or explicit desire for a long-term minority partner.\n\n"
        "3. URGENCY SCORE (int 0 - 10)\n"
        "   Rate the immediate need to contact the company based on recent news (last 6 months).\n"
        "   - 0: No recent news or information available in the last 6 months.\n"
        "   - 2-4: Standard operational news (new contracts, minor hires) but no strategic triggers.\n"
        "   - 5-7: Signals of change: CEO departure, board changes, strategic review announcements, or declining performance requiring restructuring.\n"
        "   - 8-10: Immediate Opportunity: Explicit news of capital raise, M&A rumors, distress/restructuring, or shareholder disputes indicating a deal is happening NOW.\n\n"
        "4. OUTPUT FORMAT\n"
        "   Return ONLY a raw JSON object (no markdown formatting) with these keys:\n"
        "   {\n"
        "     \"company_description\": \"string\",\n"
        "     \"deployability\": number,\n"
        "     \"deployability_explanation\": \"string\",\n"
        "     \"urgency\": number,\n"
        "     \"urgency_explanation\": \"string\"\n"
        "   }"
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


def normalize_payload(payload: dict[str, object]) -> dict[str, object] | None:
    required_keys = {
        "company_description",
        "deployability",
        "deployability_explanation",
        "urgency",
        "urgency_explanation",
    }
    if not required_keys.issubset(payload):
        return None

    description = str(payload["company_description"]).strip()
    if not description:
        return None

    try:
        deployability = float(payload["deployability"])
    except (TypeError, ValueError):
        return None

    deployability = max(0.0, min(1.0, deployability))

    deployability_explanation = str(payload["deployability_explanation"]).strip()
    if not deployability_explanation:
        return None

    try:
        urgency = int(float(payload["urgency"]))
    except (TypeError, ValueError):
        return None

    urgency = max(0, min(10, urgency))

    urgency_explanation = str(payload["urgency_explanation"]).strip()
    if not urgency_explanation:
        return None

    return {
        "company_description": description,
        "deployability": deployability,
        "deployability_explanation": deployability_explanation,
        "urgency": urgency,
        "urgency_explanation": urgency_explanation,
    }


def fetch_top_scores(session: Session, limit: int) -> list[dict[str, object]]:
    rows = session.execute(
        text(
            """
            SELECT TOP (:limit)
                s.id,
                s.orgnr,
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


def update_company_description(session: Session, orgnr: str, description: str) -> None:
    session.execute(
        text(
            """
            UPDATE dbo.company
            SET description = :description
            WHERE orgnr = :orgnr
            """
        ),
        {
            "description": description,
            "orgnr": orgnr,
        },
    )


def update_score_details(
    session: Session,
    score_id: int,
    deployability: float,
    deployability_explanation: str,
    urgency: int,
    urgency_explanation: str,
) -> None:
    session.execute(
        text(
            """
            UPDATE dbo.score
            SET deployability = :deployability,
                score_deployability_explanation = :deployability_explanation,
                urgency = :urgency,
                urgency_explanation = :urgency_explanation
            WHERE id = :score_id
            """
        ),
        {
            "deployability": deployability,
            "deployability_explanation": deployability_explanation,
            "urgency": urgency,
            "urgency_explanation": urgency_explanation,
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
            company_name = row.get("company_name")

            prompt = build_prompt(company_name, orgnr)
            response_text = llm.stream_grok_agent_response(prompt)
            payload = extract_json_payload(response_text)
            if payload is None:
                print(f"Skipping orgnr {orgnr}: could not parse JSON.")
                continue

            normalized = normalize_payload(payload)
            if normalized is None:
                print(f"Skipping orgnr {orgnr}: invalid payload {payload}.")
                continue

            update_company_description(session, orgnr, normalized["company_description"])
            update_score_details(
                session,
                score_id,
                normalized["deployability"],
                normalized["deployability_explanation"],
                normalized["urgency"],
                normalized["urgency_explanation"],
            )
            session.commit()
            print(f"Updated company orgnr {orgnr} and score id {score_id}.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update company description and deployability/urgency for top compounders."
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of rows to update.")
    args = parser.parse_args()
    run(args.limit)


if __name__ == "__main__":
    main()
