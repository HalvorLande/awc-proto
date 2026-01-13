from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
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
    ebit_cagr: float | None
    revenue_cagr: float | None
    ebit_stability: float | None
    margin_stability: float | None
    avg_roe_3yr: float | None
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


def score_growth(cagr: float | None) -> float:
    """Rewards robust growth with a target of ~20% CAGR and penalizes shrinkage."""
    if cagr is None:
        return 0
    if cagr < 0:
        return 0
    if cagr >= 0.20:
        return 100
    return (cagr / 0.20) * 100


def score_stability(stability: float | None) -> float:
    """Rewards predictability: 1.0 means highly stable, 0.0 means highly volatile."""
    if stability is None:
        return 0
    return stability * 100


def score_roe_persistence(current_roe: float | None, avg_roe: float | None) -> float:
    """
    Checks if high returns are sustainable by blending current ROE with a multi-year average.
    """
    return 0.4 * score_roe(current_roe) + 0.6 * score_roe(avg_roe)


def score_equity_ratio(equity_ratio: float | None) -> float:
    if equity_ratio is None or equity_ratio <= 0.10:
        return 0
    if equity_ratio >= 0.50:
        return 100
    return ((equity_ratio - 0.10) / (0.50 - 0.10)) * 100


# Compute Business Quality Score (BQS)
def compute_bqs(features: FeatureRow) -> float:
    # AWC's compounder framework is built on quality, not magnitude.
    # 1) "Time is an ally": stability of EBIT over time, so volatility is penalized.
    stability_score = score_stability(features.ebit_stability)

    # 2) "Robust Growth": positive trajectory is rewarded, shrinking companies are penalized.
    # We blend EBIT and revenue growth to avoid a single-metric distortion.
    growth_score = 0.6 * score_growth(features.ebit_cagr) + 0.4 * score_growth(features.revenue_cagr)

    # 3) "High ROC": persistence is more important than a one-off good year.
    profitability_score = score_roe_persistence(features.roe_proxy, features.avg_roe_3yr)

    # 4) "Moat": high margins should persist despite competition.
    # We score both the absolute margin and the stability of margins over time.
    moat_score = 0.6 * score_ebit_margin(features.ebit_margin) + 0.4 * score_stability(
        features.margin_stability
    )

    # Keep a modest balance sheet check, but do not let size inflate quality.
    return (
        0.30 * profitability_score
        + 0.20 * moat_score
        + 0.25 * growth_score
        + 0.15 * stability_score
        + 0.10 * score_equity_ratio(features.equity_ratio)
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
        SELECT orgnr, [year], ebit, revenue, equity
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
            {"start_year": year - 4, "year": year},
        ).mappings().all()

    return fs_current, ebit_history


@dataclass
class HistoryMetrics:
    avg_ebit_3yr: float | None
    ebit_cagr: float | None
    revenue_cagr: float | None
    ebit_stability: float | None
    margin_stability: float | None
    avg_roe_3yr: float | None


def compute_cagr(start_value: float | None, end_value: float | None, years: int) -> float | None:
    if start_value is None or end_value is None or years <= 0:
        return None
    if start_value <= 0 or end_value <= 0:
        return None
    return (end_value / start_value) ** (1 / years) - 1


def compute_stability(values: list[float]) -> float | None:
    """
    Stability uses coefficient of variation (CV).
    Lower CV means more predictable earnings; we invert to [0, 1].
    """
    if len(values) < 3:
        return None
    avg_value = mean(values)
    if avg_value <= 0:
        return None
    cv = pstdev(values) / avg_value
    return max(0.0, 1.0 - (cv * 2))


def compute_history_metrics(
    history_rows: Iterable[Mapping[str, object]],
) -> dict[str, HistoryMetrics]:
    """
    Convert raw financial history into quality-centric metrics aligned with AWC's
    "Investment DNA":
    - Time is an ally -> stability (predictability) of EBIT and margins.
    - Robust growth -> CAGR for EBIT and revenue.
    - Moat & high ROC -> stability of margins and persistence of ROE.
    """
    grouped: dict[str, list[tuple[int, float | None, float | None, float | None]]] = defaultdict(list)
    for row in history_rows:
        grouped[row["orgnr"]].append(
            (
                int(row["year"]),
                float(row["ebit"]) if row["ebit"] is not None else None,
                float(row["revenue"]) if row["revenue"] is not None else None,
                float(row["equity"]) if row["equity"] is not None else None,
            )
        )

    metrics: dict[str, HistoryMetrics] = {}
    for orgnr, rows in grouped.items():
        rows.sort(key=lambda item: item[0])

        ebits = [value for _, value, _, _ in rows if value is not None]
        revenues = [value for _, _, value, _ in rows if value is not None]
        margins = [
            ebit / revenue
            for _, ebit, revenue, _ in rows
            if ebit is not None and revenue not in (None, 0)
        ]
        roes = [
            ebit / equity
            for _, ebit, _, equity in rows
            if ebit is not None and equity not in (None, 0)
        ]

        avg_ebit_3yr = mean(ebits) if ebits else None
        avg_roe_3yr = mean(roes) if roes else None

        start_year, end_year = rows[0][0], rows[-1][0]
        years = end_year - start_year
        ebit_cagr = compute_cagr(rows[0][1], rows[-1][1], years)
        revenue_cagr = compute_cagr(rows[0][2], rows[-1][2], years)

        # "Time is an ally": stable EBIT implies predictability in compounding.
        ebit_stability = compute_stability(ebits)

        # "Moat": stable margins signal pricing power that persists under competition.
        margin_stability = compute_stability(margins)

        metrics[orgnr] = HistoryMetrics(
            avg_ebit_3yr=avg_ebit_3yr,
            ebit_cagr=ebit_cagr,
            revenue_cagr=revenue_cagr,
            ebit_stability=ebit_stability if ebit_stability is not None else 0.5,
            margin_stability=margin_stability if margin_stability is not None else 0.5,
            avg_roe_3yr=avg_roe_3yr,
        )

    return metrics


def build_features(
    fs_current: Iterable[Mapping[str, object]],
    history_metrics: Mapping[str, HistoryMetrics],
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

        metrics = history_metrics.get(
            row["orgnr"],
            HistoryMetrics(
                avg_ebit_3yr=None,
                ebit_cagr=None,
                revenue_cagr=None,
                ebit_stability=0.5,
                margin_stability=0.5,
                avg_roe_3yr=None,
            ),
        )

        features.append(
            FeatureRow(
                orgnr=row["orgnr"],
                year=row["year"],
                revenue=revenue,
                ebit=ebit,
                ebitda=row["ebitda"],
                assets=assets,
                equity=equity,
                avg_ebit_3yr=metrics.avg_ebit_3yr,
                ebit_cagr=metrics.ebit_cagr,
                revenue_cagr=metrics.revenue_cagr,
                ebit_stability=metrics.ebit_stability,
                margin_stability=metrics.margin_stability,
                avg_roe_3yr=metrics.avg_roe_3yr,
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
    history_metrics = compute_history_metrics(ebit_history)
    features = build_features(fs_current, history_metrics)
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
    print_quick_check(SessionLocal(), 2024)


if __name__ == "__main__":
    main()
