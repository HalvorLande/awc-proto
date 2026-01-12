from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import SessionLocal, engine
from app import models


AVG_EBIT_BANDS = [
    (300_000, 100),
    (150_000, 85),
    (75_000, 70),
    (40_000, 55),
    (20_000, 40),
]

REVENUE_BANDS = [
    (5_000_000, 100),
    (2_000_000, 85),
    (1_000_000, 70),
    (500_000, 55),
    (200_000, 40),
]


@dataclass
class FeatureRow:
    orgnr: str
    year: int
    revenue: float | None
    ebit: float | None
    ebitda: float | None
    assets: float | None
    equity: float | None
    avg_ebit_3yr: float | None
    ebit_margin: float | None
    roe_proxy: float | None
    equity_ratio: float | None


@dataclass
class ScoreRow:
    orgnr: str
    year: int
    quality_score: float
    compounder_score: float
    catalyst_score: float
    tags: str
    revenue: float | None
    ebit: float | None
    ebitda: float | None
    assets: float | None
    equity: float | None
    avg_ebit_3yr: float | None
    ebit_margin: float | None
    roe_proxy: float | None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def score_from_bands(value: float | None, bands: list[tuple[float, int]], fallback: int) -> float:
    if value is None:
        return 0
    if not bands:
        return float(fallback)

    ordered_bands = sorted(bands, key=lambda band: band[0], reverse=True)
    for index, (upper_threshold, upper_score) in enumerate(ordered_bands):
        if value >= upper_threshold:
            return float(upper_score)

        lower_band = ordered_bands[index + 1] if index + 1 < len(ordered_bands) else None
        if lower_band is None:
            break
        lower_threshold, lower_score = lower_band
        if value >= lower_threshold:
            span = upper_threshold - lower_threshold
            if span == 0:
                return float(lower_score)
            ratio = (value - lower_threshold) / span
            return lower_score + ratio * (upper_score - lower_score)

    lowest_threshold, lowest_score = ordered_bands[-1]
    if value <= 0:
        return float(fallback)
    span = lowest_threshold
    if span == 0:
        return float(lowest_score)
    ratio = min(max(value / span, 0.0), 1.0)
    return fallback + ratio * (lowest_score - fallback)


def score_roe(roe_proxy: float | None) -> float:
    if roe_proxy is None or roe_proxy <= 0:
        return 0
    if roe_proxy >= 0.25:
        return 100
    return (roe_proxy / 0.25) * 100


def score_ebit_margin(ebit_margin: float | None) -> float:
    if ebit_margin is None or ebit_margin <= 0:
        return 0
    if ebit_margin >= 0.30:
        return 100
    return (ebit_margin / 0.30) * 100


def score_equity_ratio(equity_ratio: float | None) -> float:
    if equity_ratio is None or equity_ratio <= 0.10:
        return 0
    if equity_ratio >= 0.50:
        return 100
    return ((equity_ratio - 0.10) / (0.50 - 0.10)) * 100

# Compute Business Quality Score (BQS)
def compute_bqs(features: FeatureRow) -> float:
    avg_ebit_score = score_from_bands(features.avg_ebit_3yr, AVG_EBIT_BANDS, fallback=20)
    return (
        0.30 * score_roe(features.roe_proxy)
        + 0.25 * score_ebit_margin(features.ebit_margin)
        + 0.15 * score_equity_ratio(features.equity_ratio)
        + 0.30 * avg_ebit_score
    )

# Compute Deployability Score (DPS): Everything else being equal, AWC favors largers transactions, i.e. higher ebit and revenue
def compute_dps(features: FeatureRow) -> float:
    avg_ebit_score = score_from_bands(features.avg_ebit_3yr, AVG_EBIT_BANDS, fallback=20)
    revenue_score = score_from_bands(features.revenue, REVENUE_BANDS, fallback=20)
    return 0.60 * avg_ebit_score + 0.40 * revenue_score


def revenue_band(revenue: float | None) -> str:
    if revenue is None:
        return "na"
    if revenue >= 5_000_000:
        return ">=5bn"
    if revenue >= 2_000_000:
        return "2-5bn"
    if revenue >= 1_000_000:
        return "1-2bn"
    if revenue >= 500_000:
        return "0.5-1bn"
    return "<0.5bn"


def ebit_band(avg_ebit_3yr: float | None) -> str:
    if avg_ebit_3yr is None:
        return "na"
    if avg_ebit_3yr >= 300_000:
        return ">=300m"
    if avg_ebit_3yr >= 150_000:
        return "150-300m"
    if avg_ebit_3yr >= 75_000:
        return "75-150m"
    if avg_ebit_3yr >= 40_000:
        return "40-75m"
    return "<40m"


def margin_band(ebit_margin: float | None) -> str:
    if ebit_margin is None:
        return "na"
    if ebit_margin >= 0.30:
        return ">=30%"
    if ebit_margin >= 0.20:
        return "20-30%"
    if ebit_margin >= 0.10:
        return "10-20%"
    if ebit_margin >= 0.05:
        return "5-10%"
    return "<5%"


def build_tags(features: FeatureRow) -> str:
    return (
        "QS_v1;"
        "view=company;"
        f"rev_band={revenue_band(features.revenue)};"
        f"ebit_band={ebit_band(features.avg_ebit_3yr)};"
        f"mrg={margin_band(features.ebit_margin)}"
    )


def fetch_financial_rows(year: int) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]]]:
    fs_current_sql = text(
        """
        SELECT orgnr, [year], revenue, ebit, ebitda, assets, equity
        FROM dbo.financial_statement
        WHERE [year] = :year
          AND source IN (N'proff', N'proff_forvalt_excel')
          AND account_view = N'company'
        """
    )
    ebit_history_sql = text(
        """
        SELECT orgnr, ebit
        FROM dbo.financial_statement
        WHERE [year] BETWEEN :start_year AND :year
          AND source IN (N'proff', N'proff_forvalt_excel')
          AND account_view = N'company'
        """
    )

    with engine.connect() as connection:
        fs_current = connection.execute(fs_current_sql, {"year": year}).mappings().all()
        ebit_history = connection.execute(
            ebit_history_sql,
            {"start_year": year - 2, "year": year},
        ).mappings().all()

    return fs_current, ebit_history


def compute_avg_ebit(ebit_history: Iterable[Mapping[str, object]]) -> dict[str, float | None]:
    sums: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for row in ebit_history:
        orgnr = row["orgnr"]
        ebit = row["ebit"]
        if ebit is None:
            continue
        sums[orgnr] += float(ebit)
        counts[orgnr] += 1

    averages: dict[str, float | None] = {}
    for orgnr in set(list(sums.keys()) + list(counts.keys())):
        if counts.get(orgnr):
            averages[orgnr] = sums[orgnr] / counts[orgnr]
        else:
            averages[orgnr] = None
    return averages


def build_features(
    fs_current: Iterable[Mapping[str, object]],
    avg_ebit: Mapping[str, float | None],
) -> list[FeatureRow]:
    features: list[FeatureRow] = []
    for row in fs_current:
        revenue = row["revenue"]
        ebit = row["ebit"]
        assets = row["assets"]
        equity = row["equity"]

        ebit_margin = safe_divide(ebit, revenue)
        roe_proxy = safe_divide(ebit, equity)
        equity_ratio = safe_divide(equity, assets)

        features.append(
            FeatureRow(
                orgnr=row["orgnr"],
                year=row["year"],
                revenue=revenue,
                ebit=ebit,
                ebitda=row["ebitda"],
                assets=assets,
                equity=equity,
                avg_ebit_3yr=avg_ebit.get(row["orgnr"]),
                ebit_margin=ebit_margin,
                roe_proxy=roe_proxy,
                equity_ratio=equity_ratio,
            )
        )
    return features


def compute_scores(features: Iterable[FeatureRow]) -> list[ScoreRow]:
    scored: list[ScoreRow] = []
    for feat in features:
        bqs_score = compute_bqs(feat)
        dps_score = compute_dps(feat)
        quality_score = 0.70 * bqs_score + 0.30 * dps_score

        scored.append(
            ScoreRow(
                orgnr=feat.orgnr,
                year=feat.year,
                quality_score=float(quality_score),
                compounder_score=float(quality_score),
                catalyst_score=0.0,
                tags=build_tags(feat),
                revenue=feat.revenue,
                ebit=feat.ebit,
                ebitda=feat.ebitda,
                assets=feat.assets,
                equity=feat.equity,
                avg_ebit_3yr=feat.avg_ebit_3yr,
                ebit_margin=feat.ebit_margin,
                roe_proxy=feat.roe_proxy,
            )
        )
    return scored


def merge_tags(existing: str | None, new_tags: str) -> str:
    if existing is None or existing.strip() == "":
        return new_tags
    return f"{existing} | {new_tags}"


def upsert_scores(session: Session, scores: Iterable[ScoreRow]) -> None:
    now = now_utc()
    for score in scores:
        existing = (
            session.query(models.Score)
            .filter(
                models.Score.orgnr == score.orgnr,
                models.Score.year == score.year,
            )
            .one_or_none()
        )
        if existing:
            existing.compounder_score = score.compounder_score
            existing.total_score = score.quality_score
            existing.catalyst_score = existing.catalyst_score if existing.catalyst_score is not None else 0.0
            existing.tags = merge_tags(existing.tags, score.tags)
            existing.computed_at = now
        else:
            session.add(
                models.Score(
                    orgnr=score.orgnr,
                    year=score.year,
                    total_score=score.quality_score,
                    compounder_score=score.compounder_score,
                    catalyst_score=0.0,
                    tags=score.tags,
                    computed_at=now,
                )
            )


def print_quick_check(session: Session, year: int, limit: int = 50) -> None:
    rows = (
        session.query(models.Score)
        .filter(models.Score.year == year)
        .order_by(models.Score.compounder_score.desc())
        .limit(limit)
        .all()
    )
    for row in rows:
        print(
            row.orgnr,
            row.year,
            row.total_score,
            row.compounder_score,
            row.catalyst_score,
            row.tags,
            row.computed_at,
        )


def compute_quality_scores(year: int) -> None:
    fs_current, ebit_history = fetch_financial_rows(year)
    avg_ebit = compute_avg_ebit(ebit_history)
    features = build_features(fs_current, avg_ebit)
    scores = compute_scores(features)

    with SessionLocal() as session:
        upsert_scores(session, scores)
        session.commit()
        print_quick_check(session, year)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute quality scores in Python.")
    parser.add_argument("--year", type=int, default=datetime.now().year)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    compute_quality_scores(args.year)


if __name__ == "__main__":
    main()
