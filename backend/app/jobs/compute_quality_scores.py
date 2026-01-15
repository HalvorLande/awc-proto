from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import SessionLocal, engine
from app import models



@dataclass
class FeatureRow:
    orgnr: str
    year: int
    revenue: float | None
    ebit: float | None
    equity: float | None
    revenue_cagr: float | None
    avg_roic: float | None
    margin_change_pp: float | None
    avg_cash_conversion: float | None
    avg_nwc_sales: float | None
    goodwill_ratio: float | None


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
    equity: float | None
    goodwill_ratio: float | None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def clamped_linear_score(
    val: float | None,
    neutral_val: float,
    neutral_score: float,
    max_val: float,
    max_score: float,
    min_val: float,
    min_score: float,
) -> float:
    """
    Interpolates a score linearly based on a neutral point, a cap, and a floor.
    """
    if val is None:
        return 0.0

    if val >= max_val:
        return max_score
    if val <= min_val:
        return min_score

    if val > neutral_val:
        slope = (max_score - neutral_score) / (max_val - neutral_val)
        return neutral_score + (val - neutral_val) * slope

    if val < neutral_val:
        slope = (neutral_score - min_score) / (neutral_val - min_val)
        return neutral_score - (neutral_val - val) * slope

    return neutral_score




def compute_compounder_score(features: FeatureRow) -> float:
    """
    Calculates AWC Compounder Score (-100 to +100) using continuous interpolation.
    """
    score_roic = clamped_linear_score(
        features.avg_roic,
        neutral_val=0.10,
        neutral_score=0,
        max_val=0.25,
        max_score=30,
        min_val=0.00,
        min_score=-20,
    )

    score_growth = clamped_linear_score(
        features.revenue_cagr,
        neutral_val=0.05,
        neutral_score=0,
        max_val=0.20,
        max_score=20,
        min_val=-0.05,
        min_score=-20,
    )

    score_moat = clamped_linear_score(
        features.margin_change_pp,
        neutral_val=0.00,
        neutral_score=5,
        max_val=0.10,
        max_score=20,
        min_val=-0.10,
        min_score=-20,
    )

    score_cash = clamped_linear_score(
        features.avg_cash_conversion,
        neutral_val=0.70,
        neutral_score=0,
        max_val=1.00,
        max_score=10,
        min_val=0.40,
        min_score=-10,
    )

    score_efficiency = clamped_linear_score(
        features.avg_nwc_sales,
        neutral_val=0.15,
        neutral_score=0,
        max_val=0.00,
        max_score=10,
        min_val=0.30,
        min_score=-10,
    )

    score_risk = clamped_linear_score(
        features.goodwill_ratio,
        neutral_val=0.40,
        neutral_score=0,
        max_val=0.00,
        max_score=10,
        min_val=0.80,
        min_score=-10,
    )

    total_score = (
        score_roic
        + score_growth
        + score_moat
        + score_cash
        + score_efficiency
        + score_risk
    )

    return max(-100.0, min(100.0, total_score))


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


def build_tags(features: FeatureRow) -> str:
    return (
        "QS_v2;"
        "view=company;"
        f"rev_band={revenue_band(features.revenue)};"
        f"gm_trend={'na' if features.margin_change_pp is None else f'{features.margin_change_pp:.3f}'}"
    )


def fetch_financial_rows(year: int) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]]]:
    fs_current_sql = text(
        """
        SELECT orgnr,
               [year],
               revenue,
               ebit,
               equity,
               goodwill,
               net_debt,
               cash_equivalents,
               cogs,
               payroll_expenses,
               depreciation,
               inventory,
               trade_receivables,
               trade_payables,
               dividend
        FROM dbo.financial_statement
        WHERE [year] = :year
          AND source IN (N'proff', N'proff_forvalt_excel')
          AND account_view = N'company'
        """
    )
    ebit_history_sql = text(
        """
        SELECT orgnr,
               [year],
               revenue,
               ebit,
               equity,
               net_debt,
               cash_equivalents,
               cogs,
               depreciation,
               inventory,
               trade_receivables,
               trade_payables,
               dividend
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
    revenue_cagr: float | None
    avg_roic: float | None
    margin_change_pp: float | None
    avg_cash_conversion: float | None
    avg_nwc_sales: float | None


def compute_cagr(start_value: float | None, end_value: float | None, years: int) -> float | None:
    if start_value is None or end_value is None or years <= 0:
        return None
    if start_value <= 0 or end_value <= 0:
        return None
    return (end_value / start_value) ** (1 / years) - 1


def compute_history_metrics(
    history_rows: Iterable[Mapping[str, object]],
) -> dict[str, HistoryMetrics]:
    grouped = defaultdict(list)
    for row in history_rows:
        grouped[row["orgnr"]].append(row)

    metrics: dict[str, HistoryMetrics] = {}
    for orgnr, rows in grouped.items():
        rows.sort(key=lambda item: item["year"])
        end_year = rows[-1]["year"]
        start_avg_year = end_year - 3
        avg_rows = [row for row in rows if row["year"] >= start_avg_year]

        roics: list[float] = []
        cash_convs: list[float] = []
        nwc_sales_ratios: list[float] = []
        gross_margins: dict[int, float] = {}

        for row in avg_rows:
            revenue = float(row["revenue"] or 0.0)
            ebit = float(row["ebit"] or 0.0)
            equity = float(row["equity"] or 0.0)
            net_debt = float(row["net_debt"] or 0.0)
            cogs = float(row["cogs"] or 0.0)
            depreciation = float(row["depreciation"] or 0.0)
            inventory = float(row["inventory"] or 0.0)
            receivables = float(row["trade_receivables"] or 0.0)
            payables = float(row["trade_payables"] or 0.0)

            invested_capital = equity + net_debt
            if invested_capital > 0:
                nopat = ebit * 0.78
                roics.append(nopat / invested_capital)

            if ebit > 0:
                cash_convs.append((ebit + depreciation) / ebit)

            if revenue > 0:
                nwc = receivables + inventory - payables
                nwc_sales_ratios.append(nwc / revenue)
                gross_margins[row["year"]] = (revenue - cogs) / revenue

        start_year = avg_rows[0]["year"]
        years = end_year - start_year
        revenue_cagr = compute_cagr(
            float(avg_rows[0]["revenue"] or 0.0),
            float(avg_rows[-1]["revenue"] or 0.0),
            years,
        )

        margin_change_pp = None
        if start_year in gross_margins and end_year in gross_margins:
            margin_change_pp = gross_margins[end_year] - gross_margins[start_year]

        metrics[orgnr] = HistoryMetrics(
            revenue_cagr=revenue_cagr,
            avg_roic=mean(roics) if roics else None,
            margin_change_pp=margin_change_pp,
            avg_cash_conversion=mean(cash_convs) if cash_convs else None,
            avg_nwc_sales=mean(nwc_sales_ratios) if nwc_sales_ratios else None,
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
        equity = row["equity"]

        metrics = history_metrics.get(
            row["orgnr"],
            HistoryMetrics(
                revenue_cagr=None,
                avg_roic=None,
                margin_change_pp=None,
                avg_cash_conversion=None,
                avg_nwc_sales=None,
            ),
        )

        goodwill = float(row["goodwill"] or 0.0)
        equity_value = float(equity or 0.0)
        goodwill_ratio = None
        if equity_value > 0:
            goodwill_ratio = goodwill / equity_value

        features.append(
            FeatureRow(
                orgnr=row["orgnr"],
                year=row["year"],
                revenue=revenue,
                ebit=ebit,
                equity=equity,
                revenue_cagr=metrics.revenue_cagr,
                avg_roic=metrics.avg_roic,
                margin_change_pp=metrics.margin_change_pp,
                avg_cash_conversion=metrics.avg_cash_conversion,
                avg_nwc_sales=metrics.avg_nwc_sales,
                goodwill_ratio=goodwill_ratio,
            )
        )
    return features


def compute_scores(features: Iterable[FeatureRow]) -> list[ScoreRow]:
    scored: list[ScoreRow] = []
    for feat in features:
        compounder_score = compute_compounder_score(feat)

        scored.append(
            ScoreRow(
                orgnr=feat.orgnr,
                year=feat.year,
                quality_score=float(compounder_score),
                compounder_score=float(compounder_score),
                catalyst_score=0.0,
                tags=build_tags(feat),
                revenue=feat.revenue,
                ebit=feat.ebit,
                equity=feat.equity,
                goodwill_ratio=feat.goodwill_ratio,
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
    compute_quality_scores(2024)
    print_quick_check(SessionLocal(), 2024)


if __name__ == "__main__":
    print("running main")
    main()
