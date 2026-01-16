from __future__ import annotations
from datetime import date, datetime
from pydantic import BaseModel


class TopPickItem(BaseModel):
    rank: int
    orgnr: str
    name: str
    total_score: float
    ebitda: float | None = None
    revenue: float | None = None
    tags: str | None = None
    reason_summary: str | None = None


class CompanySummary(BaseModel):
    orgnr: str
    name: str
    total_score: float | None = None
    compounder_score: float | None = None
    deployability: float | None = None
    urgency: float | None = None


class OutreachUpdateIn(BaseModel):
    owner: str | None = None
    status: str | None = None
    note: str | None = None
    next_step_at: datetime | None = None


class CompanyDetail(BaseModel):
    orgnr: str
    name: str
    nace: str | None = None
    municipality: str | None = None
    website: str | None = None
    description: str | None = None

    latest_year: int | None = None
    revenue: float | None = None
    ebitda: float | None = None
    ebit: float | None = None
    cfo: float | None = None
    total_score: float | None = None
    tags: str | None = None
    deployability: float | None = None
    deployability_explanation: str | None = None
    urgency: float | None = None
    urgency_explanation: str | None = None

    roic: float | None = None
    roic_score: float | None = None
    revenue_cagr: float | None = None
    revenue_cagr_score: float | None = None
    margin_change: float | None = None
    margin_change_score: float | None = None
    cash_conversion: float | None = None
    nwc_sales: float | None = None
    nwc_sales_score: float | None = None
    goodwill_ratio: float | None = None
    goodwill_ratio_score: float | None = None

    outreach_owner: str | None = None
    outreach_status: str | None = None
    outreach_note: str | None = None
