from __future__ import annotations

from datetime import date
import random
from sqlalchemy.orm import Session

from .db import SessionLocal, engine, Base
from . import models

def run():
    Base.metadata.create_all(bind=engine)
    db: Session = SessionLocal()

    # Clear existing for repeatable demo
    db.query(models.DailyTopPick).delete()
    db.query(models.Outreach).delete()
    db.query(models.Score).delete()
    db.query(models.FinancialStatement).delete()
    db.query(models.Company).delete()
    db.commit()

    # Create 200 demo companies
    companies = []
    for i in range(200):
        orgnr = f"{900000000 + i}"
        c = models.Company(
            orgnr=orgnr,
            name=f"Demo Company {i:03d}",
            nace="62.010",
            municipality="Oslo",
            website=None,
        )
        companies.append(c)
    db.add_all(companies)
    db.commit()

    # Financials + scores for last 3 years
    years = [2022, 2023, 2024]
    for c in companies:
        base_rev = random.uniform(200, 2000)  # MNOK
        for y in years:
            rev = base_rev * random.uniform(0.9, 1.2)
            ebitda = rev * random.uniform(0.08, 0.30)
            ebit = ebitda * random.uniform(0.6, 0.9)
            cfo = ebitda * random.uniform(0.5, 1.1)
            assets = rev * random.uniform(0.4, 1.5)
            equity = assets * random.uniform(0.2, 0.6)
            net_debt = max(0.0, (assets - equity) * random.uniform(0.1, 0.6))

            db.add(models.FinancialStatement(
                orgnr=c.orgnr, year=y,
                revenue=float(rev), ebitda=float(ebitda), ebit=float(ebit), cfo=float(cfo),
                assets=float(assets), equity=float(equity), net_debt=float(net_debt)
            ))

            # A toy score: emphasize EBITDA and cash conversion
            cash_conv = (cfo / ebitda) if ebitda > 0 else 0.0
            total = (ebitda * 1.0) + (cash_conv * 20.0)
            tags = "ebitda>=50" if ebitda >= 50 else None

            db.add(models.Score(
                orgnr=c.orgnr, year=y,
                total_score=float(total),
                compounder_score=float(total * 0.7),
                catalyst_score=float(total * 0.3),
                tags=tags,
            ))

    db.commit()

    # Create today's top 10 based on latest year total_score
    today = date.today()
    latest_year = 2024
    top = (
        db.query(models.Score)
        .filter(models.Score.year == latest_year)
        .order_by(models.Score.total_score.desc())
        .limit(10)
        .all()
    )

    for idx, s in enumerate(top, start=1):
        db.add(models.DailyTopPick(
            pick_date=today,
            rank=idx,
            orgnr=s.orgnr,
            reason_summary="High EBITDA + good cash conversion (demo)",
            total_score_snapshot=s.total_score
        ))

    db.commit()
    db.close()
    print("Seed complete. Top 10 created for today.")

if __name__ == "__main__":
    run()
