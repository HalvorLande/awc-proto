from __future__ import annotations

from datetime import date, datetime
from sqlalchemy import (
    String, Integer, Date, DateTime, Float, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class Company(Base):
    __tablename__ = "company"

    orgnr: Mapped[str] = mapped_column(String(9), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    nace: Mapped[str | None] = mapped_column(String(10), nullable=True)
    municipality: Mapped[str | None] = mapped_column(String(100), nullable=True)
    website: Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    financials: Mapped[list["FinancialStatement"]] = relationship(back_populates="company")
    scores: Mapped[list["Score"]] = relationship(back_populates="company")
    outreach: Mapped["Outreach | None"] = relationship(back_populates="company", uselist=False)


class FinancialStatement(Base):
    __tablename__ = "financial_statement"
    __table_args__ = (
        UniqueConstraint("orgnr", "year", name="uq_fin_orgnr_year"),
        Index("ix_fin_orgnr_year", "orgnr", "year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    orgnr: Mapped[str] = mapped_column(String(9), ForeignKey("company.orgnr"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)

    # Core P&L / Balance Sheet
    revenue: Mapped[float | None] = mapped_column(Float, nullable=True)
    ebitda: Mapped[float | None] = mapped_column(Float, nullable=True)
    ebit: Mapped[float | None] = mapped_column(Float, nullable=True)
    cfo: Mapped[float | None] = mapped_column(Float, nullable=True)
    assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_debt: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- NEW FIELDS FOR AWC COMPOUNDER SCORE ---
    cogs: Mapped[float | None] = mapped_column(Float, nullable=True)
    payroll_expenses: Mapped[float | None] = mapped_column(Float, nullable=True)
    depreciation: Mapped[float | None] = mapped_column(Float, nullable=True)
    inventory: Mapped[float | None] = mapped_column(Float, nullable=True)
    trade_receivables: Mapped[float | None] = mapped_column(Float, nullable=True)
    trade_payables: Mapped[float | None] = mapped_column(Float, nullable=True)
    cash_equivalents: Mapped[float | None] = mapped_column(Float, nullable=True)
    goodwill: Mapped[float | None] = mapped_column(Float, nullable=True)
    dividend: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_debt: Mapped[float | None] = mapped_column(Float, nullable=True)

    company: Mapped["Company"] = relationship(back_populates="financials")


class Score(Base):
    __tablename__ = "score"
    __table_args__ = (
        UniqueConstraint("orgnr", "year", name="uq_score_orgnr_year"),
        Index("ix_score_orgnr_year", "orgnr", "year"),
        Index("ix_score_total", "total_score"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    orgnr: Mapped[str] = mapped_column(String(9), ForeignKey("company.orgnr"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)

    total_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    compounder_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    catalyst_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    tags: Mapped[str | None] = mapped_column(String(500), nullable=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    # --- NEW DETAILED METRICS & SCORES ---
    roic: Mapped[float | None] = mapped_column(Float, nullable=True)
    roic_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    revenue_cagr: Mapped[float | None] = mapped_column(Float, nullable=True)
    revenue_cagr_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    margin_change: Mapped[float | None] = mapped_column(Float, nullable=True)
    margin_change_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    nwc_sales: Mapped[float | None] = mapped_column(Float, nullable=True)
    nwc_sales_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    goodwill_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    goodwill_ratio_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    company: Mapped["Company"] = relationship(back_populates="scores")


class DailyTopPick(Base):
    __tablename__ = "daily_top_pick"
    __table_args__ = (
        UniqueConstraint("pick_date", "rank", name="uq_pick_date_rank"),
        UniqueConstraint("pick_date", "orgnr", name="uq_pick_date_orgnr"),
        Index("ix_pick_date", "pick_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pick_date: Mapped[date] = mapped_column(Date, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    orgnr: Mapped[str] = mapped_column(String(9), ForeignKey("company.orgnr"), nullable=False)

    reason_summary: Mapped[str | None] = mapped_column(String(500), nullable=True)
    total_score_snapshot: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class Outreach(Base):
    __tablename__ = "outreach"

    orgnr: Mapped[str] = mapped_column(String(9), ForeignKey("company.orgnr"), primary_key=True)
    owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="new")
    last_contact_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_step_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    note: Mapped[str | None] = mapped_column(String(2000), nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    company: Mapped["Company"] = relationship(back_populates="outreach")