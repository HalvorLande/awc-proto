from __future__ import annotations

from datetime import date
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import select, desc, func

from .db import engine, Base, get_db
from . import models
from .schemas import TopPickItem, CompanyDetail, CompanySummary, OutreachUpdateIn

app = FastAPI(title="AWC Prototype API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


@app.get("/companies", response_model=list[CompanySummary])
def list_companies(db: Session = Depends(get_db)):
    latest_score = (
        select(models.Score.orgnr, func.max(models.Score.year).label("max_year"))
        .group_by(models.Score.orgnr)
        .subquery()
    )

    stmt = (
        select(models.Company, models.Score)
        .join(latest_score, models.Company.orgnr == latest_score.c.orgnr, isouter=True)
        .join(
            models.Score,
            (models.Score.orgnr == latest_score.c.orgnr)
            & (models.Score.year == latest_score.c.max_year),
            isouter=True,
        )
        .order_by(models.Company.name.asc())
    )

    rows = db.execute(stmt).all()
    return [
        CompanySummary(
            orgnr=company.orgnr,
            name=company.name,
            total_score=getattr(score, "total_score", None),
            compounder_score=getattr(score, "compounder_score", None),
            deployability=getattr(score, "deployability", None),
            urgency=getattr(score, "urgency", None),
        )
        for company, score in rows
    ]


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

    cash_conversion = None
    fin_history_stmt = (
        select(models.FinancialStatement)
        .where(models.FinancialStatement.orgnr == orgnr)
        .order_by(desc(models.FinancialStatement.year))
        .limit(4)
    )
    fin_history = db.execute(fin_history_stmt).scalars().all()
    cash_values: list[float] = []
    for entry in fin_history:
        ebit = getattr(entry, "ebit", None)
        depreciation = getattr(entry, "depreciation", None)
        if ebit and ebit != 0 and depreciation is not None:
            cash_values.append((ebit + depreciation) / ebit)
    if cash_values:
        cash_conversion = sum(cash_values) / len(cash_values)

    outreach = db.get(models.Outreach, orgnr)

    return CompanyDetail(
        orgnr=company.orgnr,
        name=company.name,
        nace=company.nace,
        municipality=company.municipality,
        website=company.website,
        description=company.description,
        latest_year=getattr(score, "year", None),
        revenue=getattr(fin, "revenue", None),
        ebitda=getattr(fin, "ebitda", None),
        ebit=getattr(fin, "ebit", None),
        cfo=getattr(fin, "cfo", None),
        total_score=getattr(score, "total_score", None),
        tags=getattr(score, "tags", None),
        deployability=getattr(score, "deployability", None),
        deployability_explanation=getattr(score, "deployability_explanation", None),
        urgency=getattr(score, "urgency", None),
        urgency_explanation=getattr(score, "urgency_explanation", None),
        roic=getattr(score, "roic", None),
        roic_score=getattr(score, "roic_score", None),
        revenue_cagr=getattr(score, "revenue_cagr", None),
        revenue_cagr_score=getattr(score, "revenue_cagr_score", None),
        margin_change=getattr(score, "margin_change", None),
        margin_change_score=getattr(score, "margin_change_score", None),
        cash_conversion=cash_conversion,
        nwc_sales=getattr(score, "nwc_sales", None),
        nwc_sales_score=getattr(score, "nwc_sales_score", None),
        goodwill_ratio=getattr(score, "goodwill_ratio", None),
        goodwill_ratio_score=getattr(score, "goodwill_ratio_score", None),
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
