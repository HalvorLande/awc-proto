from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Iterable, Mapping, Any

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
    
    # Raw Metrics for Scoring
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
    
    # Top Level Scores
    quality_score: float
    compounder_score: float
    catalyst_score: float
    tags: str
    
    # Context Data
    revenue: float | None
    ebit: float | None
    equity: float | None
    
    # --- NEW: Detailed Metrics & Sub-Scores ---
    # We store both the raw value (e.g. 0.25 for 25% ROIC) 
    # and the score (e.g. 30.0 points)
    
    roic: float | None
    roic_score: float | None
    
    revenue_cagr: float | None
    revenue_cagr_score: float | None
    
    margin_change: float | None
    margin_change_score: float | None
    
    nwc_sales: float | None
    nwc_sales_score: float | None
    
    goodwill_ratio: float | None
    goodwill_ratio_score: float | None


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

    # 1. Cap
    if val >= max_val:
        return max_score
    # 2. Floor
    if val <= min_val:
        return min_score

    # 3. Interpolate Neutral -> Max
    if val > neutral_val:
        slope = (max_score - neutral_score) / (max_val - neutral_val)
        return neutral_score + (val - neutral_val) * slope

    # 4. Interpolate Neutral -> Min
    if val < neutral_val:
        slope = (neutral_score - min_score) / (neutral_val - min_val)
        return neutral_score - (neutral_val - val) * slope

    return neutral_score


def compute_compounder_score_details(features: FeatureRow) -> dict[str, float]:
    """
    Calculates AWC Compounder Score and returns a dictionary with 
    the total and all component scores.
    """
    
    # 1. ROIC (30 pts)
    score_roic = clamped_linear_score(
        features.avg_roic,
        neutral_val=0.10, neutral_score=0,
        max_val=0.25, max_score=30,
        min_val=0.00, min_score=-20,
    )

    # 2. Growth (20 pts)
    score_growth = clamped_linear_score(
        features.revenue_cagr,
        neutral_val=0.05, neutral_score=0,
        max_val=0.20, max_score=20,
        min_val=-0.05, min_score=-20,
    )

    # 3. Moat / Margin Trend (20 pts)
    score_moat = clamped_linear_score(
        features.margin_change_pp,
        neutral_val=0.00, neutral_score=5,
        max_val=0.10, max_score=20,
        min_val=-0.10, min_score=-20,
    )

    # 4. Cash Conversion (10 pts)
    score_cash = clamped_linear_score(
        features.avg_cash_conversion,
        neutral_val=0.70, neutral_score=0,
        max_val=1.00, max_score=10,
        min_val=0.40, min_score=-10,
    )

    # 5. Efficiency / NWC (10 pts)
    # Note: Lower NWC is better, so max_val is 0.00
    score_efficiency = clamped_linear_score(
        features.avg_nwc_sales,
        neutral_val=0.15, neutral_score=0,
        max_val=0.00, max_score=10,
        min_val=0.30, min_score=-10,
    )

    # 6. Risk / Goodwill (10 pts)
    # Note: Lower Goodwill is better, so max_val is 0.00
    score_risk = clamped_linear_score(
        features.goodwill_ratio,
        neutral_val=0.40, neutral_score=0,
        max_val=0.00, max_score=10,
        min_val=0.80, min_score=-10,
    )

    total_score = (
        score_roic
        + score_growth
        + score_moat
        + score_cash
        + score_efficiency
        + score_risk
    )
    
    # Cap between -100 and 100
    total_score = max(-100.0, min(100.0, total_score))

    return {
        "total": total_score,
        "roic_score": score_roic,
        "revenue_cagr_score": score_growth,
        "margin_change_score": score_moat,
        "cash_score": score_cash, # Not currently storing cash score in DB, but good to have
        "nwc_sales_score": score_efficiency,
        "goodwill_ratio_score": score_risk
    }


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
    # Helper to format percentages cleanly
    def p(val):
        return f"{val:.1%}" if val is not None else "na"
        
    return (
        "QS_v2;"
        "view=company;"
        f"rev_band={revenue_band(features.revenue)};"
        f"roic={p(features.avg_roic)};"
        f"cagr={p(features.revenue_cagr)}"
    )


def fetch_financial_rows(year: int) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]]]:
    fs_current_sql = text(
        """
        SELECT orgnr, [year], revenue, ebit, equity, 
               goodwill, total_debt, cash_equivalents
        FROM dbo.financial_statement
        WHERE [year] = :year
          AND source IN (N'proff', N'proff_forvalt_excel')
          AND account_view = N'company'
        """
    )
    ebit_history_sql = text(
        """
        SELECT orgnr, [year], revenue, ebit, equity,
               total_debt, cash_equivalents,
               cogs, depreciation,
               inventory, trade_receivables, trade_payables
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
        
        # We need historical data to calculate trends
        roics: list[float] = []
        cash_convs: list[float] = []
        nwc_sales_ratios: list[float] = []
        gross_margins: dict[int, float] = {}

        for row in rows:
            # Safe float conversion
            revenue = float(row["revenue"] or 0.0)
            ebit = float(row["ebit"] or 0.0)
            equity = float(row["equity"] or 0.0)
            net_debt = float(row["total_debt"] or 0.0) # total_debt used as proxy or adjust if you have net_debt col
            cash = float(row["cash_equivalents"] or 0.0)
            
            cogs = float(row["cogs"] or 0.0)
            depr = float(row["depreciation"] or 0.0)
            inventory = float(row["inventory"] or 0.0)
            receivables = float(row["trade_receivables"] or 0.0)
            payables = float(row["trade_payables"] or 0.0)

            # Invested Capital = Equity + Debt - Cash
            invested_capital = equity + net_debt - cash
            
            # A. ROIC (Threshold to avoid division by near-zero)
            if invested_capital > 1000:
                nopat = ebit * 0.78
                roics.append(nopat / invested_capital)

            # B. Cash Conversion
            if ebit > 0:
                cash_convs.append((ebit + depr) / ebit)

            # C. NWC & Gross Margin
            if revenue > 0:
                nwc = receivables + inventory - payables
                nwc_sales_ratios.append(nwc / revenue)
                gross_margins[row["year"]] = (revenue - cogs) / revenue

        # --- Aggregation ---
        
        # 1. CAGR
        start_year = rows[0]["year"]
        end_year = rows[-1]["year"]
        years = end_year - start_year
        
        revenue_cagr = compute_cagr(
            float(rows[0]["revenue"] or 0.0),
            float(rows[-1]["revenue"] or 0.0),
            years,
        )

        # 2. Margin Trend
        margin_change_pp = None
        if start_year in gross_margins and end_year in gross_margins:
            margin_change_pp = gross_margins[end_year] - gross_margins[start_year]

        # 3. Averages
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
        # Get history or default
        metrics = history_metrics.get(
            row["orgnr"],
            HistoryMetrics(None, None, None, None, None)
        )

        # Snapshot Metrics
        goodwill = float(row["goodwill"] or 0.0)
        equity_value = float(row["equity"] or 0.0)
        
        goodwill_ratio = None
        if equity_value > 0:
            goodwill_ratio = goodwill / equity_value

        features.append(
            FeatureRow(
                orgnr=row["orgnr"],
                year=row["year"],
                revenue=row["revenue"],
                ebit=row["ebit"],
                equity=row["equity"],
                # Mapped Metrics
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
        # Calculate scores and get breakdown
        results = compute_compounder_score_details(feat)
        total_score = results["total"]

        scored.append(
            ScoreRow(
                orgnr=feat.orgnr,
                year=feat.year,
                quality_score=total_score,
                compounder_score=total_score,
                catalyst_score=0.0,
                tags=build_tags(feat),
                
                # Context
                revenue=feat.revenue,
                ebit=feat.ebit,
                equity=feat.equity,
                
                # --- NEW MAPPING ---
                
                # 1. ROIC
                roic=feat.avg_roic,
                roic_score=results["roic_score"],
                
                # 2. Growth
                revenue_cagr=feat.revenue_cagr,
                revenue_cagr_score=results["revenue_cagr_score"],
                
                # 3. Moat
                margin_change=feat.margin_change_pp,
                margin_change_score=results["margin_change_score"],
                
                # 4. Efficiency
                nwc_sales=feat.avg_nwc_sales,
                nwc_sales_score=results["nwc_sales_score"],
                
                # 5. Risk / Goodwill
                goodwill_ratio=feat.goodwill_ratio,
                goodwill_ratio_score=results["goodwill_ratio_score"],
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
            # Update basics
            existing.compounder_score = score.compounder_score
            existing.total_score = score.quality_score
            existing.tags = merge_tags(existing.tags, score.tags)
            existing.computed_at = now
            
            # Update NEW fields (Metrics & Scores)
            # Assuming app.models.Score has been updated to match dbo.score
            existing.roic = score.roic
            existing.roic_score = score.roic_score
            existing.revenue_cagr = score.revenue_cagr
            existing.revenue_cagr_score = score.revenue_cagr_score
            existing.margin_change = score.margin_change
            existing.margin_change_score = score.margin_change_score
            existing.nwc_sales = score.nwc_sales
            existing.nwc_sales_score = score.nwc_sales_score
            existing.goodwill_ratio = score.goodwill_ratio
            existing.goodwill_ratio_score = score.goodwill_ratio_score

        else:
            # Create new
            new_obj = models.Score(
                orgnr=score.orgnr,
                year=score.year,
                total_score=score.quality_score,
                compounder_score=score.compounder_score,
                catalyst_score=0.0,
                tags=score.tags,
                computed_at=now,
                
                # New Fields
                roic=score.roic,
                roic_score=score.roic_score,
                revenue_cagr=score.revenue_cagr,
                revenue_cagr_score=score.revenue_cagr_score,
                margin_change=score.margin_change,
                margin_change_score=score.margin_change_score,
                nwc_sales=score.nwc_sales,
                nwc_sales_score=score.nwc_sales_score,
                goodwill_ratio=score.goodwill_ratio,
                goodwill_ratio_score=score.goodwill_ratio_score
            )
            session.add(new_obj)


def print_quick_check(session: Session, year: int, limit: int = 50) -> None:
    rows = (
        session.query(models.Score)
        .filter(models.Score.year == year)
        .order_by(models.Score.compounder_score.desc())
        .limit(limit)
        .all()
    )
    print(f"\n--- TOP {limit} COMPOUNDERS ({year}) ---")
    print(f"{'ORGNR':<10} {'SCORE':<6} {'ROIC':<6} {'CAGR':<6} {'TAGS'}")
    for row in rows:
        # Helper to safely print optional floats
        def f(val): return f"{val:.1%}" if val is not None else "-"
        
        print(
            f"{row.orgnr:<10} "
            f"{row.compounder_score:<6.1f} "
            f"{f(row.roic):<6} "
            f"{f(row.revenue_cagr):<6} "
            f"{row.tags}"
        )


def compute_quality_scores(year: int) -> None:
    print(f"Fetching data for {year}...")
    fs_current, ebit_history = fetch_financial_rows(year)
    
    print("Computing history metrics...")
    history_metrics = compute_history_metrics(ebit_history)
    
    print("Building features...")
    features = build_features(fs_current, history_metrics)
    
    print("Computing scores...")
    scores = compute_scores(features)

    print("Upserting to database...")
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
    # Hardcoded to 2024 per your previous logic, or use args.year
    target_year = 2024 
    compute_quality_scores(target_year)


if __name__ == "__main__":
    main()