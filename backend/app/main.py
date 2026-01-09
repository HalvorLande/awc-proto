from __future__ import annotations

from datetime import date
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from .db import engine, Base, get_db
from . import models
from .schemas import TopPickItem, CompanyDetail, OutreachUpdateIn

app = FastAPI(title="AWC Prototype API")

# For a local prototype, create tables automatically.
# Later you’ll replace this with Alembic migrations.
Base.metadata.create_all(bind=engine)


@app.get("/top-picks/today", response_model=list[TopPickItem])
def get_top_picks_today(db: Session = Depends(get_db)):
    today = date.today()

    stmt = (
        select(models.DailyTopPick, models.Company, models.Score, models.FinancialStatement)
        .join(models.Company, models.Company.orgnr == models.DailyTopPick.orgnr)
        .join(models.Score, (models.Score.orgnr == models.DailyTopPick.orgnr), isouter=True)
        .join(
            models.FinancialStatement,
            (models.FinancialStatement.orgnr == models.DailyTopPick.orgnr)
            & (models.FinancialStatement.year == models.Score.year),
            isouter=True,
        )
        .where(models.DailyTopPick.pick_date == today)
        .order_by(models.DailyTopPick.rank.asc())
    )

    rows = db.execute(stmt).all()
    result: list[TopPickItem] = []
    for pick, company, score, fin in rows:
        result.append(
            TopPickItem(
                rank=pick.rank,
                orgnr=company.orgnr,
                name=company.name,
                total_score=pick.total_score_snapshot,
                ebitda=getattr(fin, "ebitda", None),
                revenue=getattr(fin, "revenue", None),
                tags=getattr(score, "tags", None),
                reason_summary=pick.reason_summary,
            )
        )
    return result


@app.get("/companies/{orgnr}", response_model=CompanyDetail)
def get_company(orgnr: str, db: Session = Depends(get_db)):
    company = db.get(models.Company, orgnr)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Latest score by year
    score_stmt = (
        select(models.Score)
        .where(models.Score.orgnr == orgnr)
        .order_by(desc(models.Score.year))
    )
    score = db.execute(score_stmt).scalars().first()

    fin = None
    if score:
        fin_stmt = select(models.FinancialStatement).where(
            (models.FinancialStatement.orgnr == orgnr) & (models.FinancialStatement.year == score.year)
        )
        fin = db.execute(fin_stmt).scalars().first()

    outreach = db.get(models.Outreach, orgnr)

    return CompanyDetail(
        orgnr=company.orgnr,
        name=company.name,
        nace=company.nace,
        municipality=company.municipality,
        website=company.website,
        latest_year=getattr(score, "year", None),
        revenue=getattr(fin, "revenue", None),
        ebitda=getattr(fin, "ebitda", None),
        ebit=getattr(fin, "ebit", None),
        cfo=getattr(fin, "cfo", None),
        total_score=getattr(score, "total_score", None),
        tags=getattr(score, "tags", None),
        outreach_owner=getattr(outreach, "owner", None),
        outreach_status=getattr(outreach, "status", None),
        outreach_note=getattr(outreach, "note", None),
    )


@app.post("/outreach/{orgnr}/update")
def update_outreach(orgnr: str, payload: OutreachUpdateIn, db: Session = Depends(get_db)):
    company = db.get(models.Company, orgnr)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    outreach = db.get(models.Outreach, orgnr)
    if not outreach:
        outreach = models.Outreach(orgnr=orgnr)
        db.add(outreach)

    if payload.owner is not None:
        outreach.owner = payload.owner
    if payload.status is not None:
        outreach.status = payload.status
    if payload.note is not None:
        outreach.note = payload.note
    if payload.next_step_at is not None:
        outreach.next_step_at = payload.next_step_at

    db.commit()
    return {"ok": True}
