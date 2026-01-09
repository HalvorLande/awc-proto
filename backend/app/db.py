from __future__ import annotations

import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

load_dotenv()

# --- connection ---
server = os.getenv("SQL_SERVER", "AAD-GM12FD8W")
database = os.getenv("SQL_DATABASE", "AwcProto")
driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

odbc_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

DATABASE_URL = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str)

engine = create_engine(DATABASE_URL, echo=False, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# --- Base lives here ---
class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
