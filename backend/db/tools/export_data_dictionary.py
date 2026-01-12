from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pyodbc
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)


# -------------------------
# Config: env vars
# -------------------------
SQL_SERVER = os.getenv("SQL_SERVER", "AAD-GM12FD8W")
SQL_DATABASE = os.getenv("SQL_DATABASE", "AwcProto")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
INCLUDE_ROW_COUNTS = os.getenv("INCLUDE_ROW_COUNTS", "0") == "1"

# Output path (repo-relative)
OUTPUT_MD = Path("db/docs/data_dictionary.md")


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def conn_str() -> str:
    # Trusted Connection for Windows auth
    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )


def connect() -> pyodbc.Connection:
    return pyodbc.connect(conn_str(), timeout=30)


def fetchall_dict(cur: pyodbc.Cursor) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


@dataclass
class ColumnInfo:
    table: str
    column_id: int
    name: str
    data_type: str
    max_length: int
    precision: int
    scale: int
    is_nullable: bool
    is_identity: bool
    default_definition: str | None


@dataclass
class ConstraintInfo:
    table: str
    constraint_name: str
    constraint_type: str  # PRIMARY KEY / UNIQUE
    columns: List[str]


@dataclass
class ForeignKeyInfo:
    table: str
    fk_name: str
    columns: List[str]
    ref_table: str
    ref_columns: List[str]
    on_delete: str
    on_update: str


@dataclass
class IndexInfo:
    table: str
    index_name: str
    is_unique: bool
    type_desc: str
    columns: List[str]
    includes: List[str]
    filter_definition: str | None


def main() -> int:
    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)

    with connect() as cn:
        cn.autocommit = True
        cur = cn.cursor()

        # -------------------------
        # Tables (dbo only)
        # -------------------------
        cur.execute(
            """
            SELECT
                t.object_id,
                t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = 'dbo'
              AND t.is_ms_shipped = 0
            ORDER BY t.name;
            """
        )
        tables = fetchall_dict(cur)
        table_names = [t["table_name"] for t in tables]
        table_ids = {t["table_name"]: t["object_id"] for t in tables}

        # -------------------------
        # Columns
        # -------------------------
        cur.execute(
            """
            SELECT
                t.name AS table_name,
                c.column_id,
                c.name AS column_name,
                ty.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                dc.definition AS default_definition
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            LEFT JOIN sys.default_constraints dc
                ON dc.parent_object_id = t.object_id
               AND dc.parent_column_id = c.column_id
            WHERE s.name = 'dbo'
              AND t.is_ms_shipped = 0
            ORDER BY t.name, c.column_id;
            """
        )
        col_rows = fetchall_dict(cur)
        columns_by_table: Dict[str, List[ColumnInfo]] = {}
        for r in col_rows:
            ci = ColumnInfo(
                table=r["table_name"],
                column_id=int(r["column_id"]),
                name=r["column_name"],
                data_type=r["data_type"],
                max_length=int(r["max_length"]),
                precision=int(r["precision"]),
                scale=int(r["scale"]),
                is_nullable=bool(r["is_nullable"]),
                is_identity=bool(r["is_identity"]),
                default_definition=r["default_definition"],
            )
            columns_by_table.setdefault(ci.table, []).append(ci)

        # -------------------------
        # PK + UNIQUE constraints with ordered columns
        # -------------------------
        cur.execute(
            """
            SELECT
                t.name AS table_name,
                kc.name AS constraint_name,
                kc.type_desc AS constraint_type, -- 'PRIMARY_KEY_CONSTRAINT' / 'UNIQUE_CONSTRAINT'
                ic.key_ordinal,
                col.name AS column_name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.key_constraints kc ON kc.parent_object_id = t.object_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id = kc.unique_index_id
            JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
            JOIN sys.columns col ON col.object_id = t.object_id AND col.column_id = ic.column_id
            WHERE s.name = 'dbo'
              AND t.is_ms_shipped = 0
            ORDER BY t.name, kc.name, ic.key_ordinal;
            """
        )
        cons_rows = fetchall_dict(cur)
        constraints_by_table: Dict[str, Dict[str, ConstraintInfo]] = {}
        for r in cons_rows:
            table = r["table_name"]
            cname = r["constraint_name"]
            ctype = r["constraint_type"]
            col = r["column_name"]

            human_type = "PRIMARY KEY" if ctype == "PRIMARY_KEY_CONSTRAINT" else "UNIQUE"
            by_name = constraints_by_table.setdefault(table, {})
            if cname not in by_name:
                by_name[cname] = ConstraintInfo(
                    table=table,
                    constraint_name=cname,
                    constraint_type=human_type,
                    columns=[],
                )
            by_name[cname].columns.append(col)

        # -------------------------
        # Foreign keys (dbo only)
        # -------------------------
        cur.execute(
            """
            SELECT
                t.name AS table_name,
                fk.name AS fk_name,
                fkc.constraint_column_id,
                c1.name AS column_name,
                rt.name AS ref_table_name,
                c2.name AS ref_column_name,
                fk.delete_referential_action_desc AS on_delete,
                fk.update_referential_action_desc AS on_update
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            JOIN sys.tables t ON t.object_id = fk.parent_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.columns c1 ON c1.object_id = t.object_id AND c1.column_id = fkc.parent_column_id
            JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
            JOIN sys.columns c2 ON c2.object_id = rt.object_id AND c2.column_id = fkc.referenced_column_id
            WHERE s.name = 'dbo'
              AND t.is_ms_shipped = 0
            ORDER BY t.name, fk.name, fkc.constraint_column_id;
            """
        )
        fk_rows = fetchall_dict(cur)
        fks_by_table: Dict[str, Dict[str, ForeignKeyInfo]] = {}
        for r in fk_rows:
            table = r["table_name"]
            fk_name = r["fk_name"]
            by_name = fks_by_table.setdefault(table, {})
            if fk_name not in by_name:
                by_name[fk_name] = ForeignKeyInfo(
                    table=table,
                    fk_name=fk_name,
                    columns=[],
                    ref_table=r["ref_table_name"],
                    ref_columns=[],
                    on_delete=r["on_delete"],
                    on_update=r["on_update"],
                )
            by_name[fk_name].columns.append(r["column_name"])
            by_name[fk_name].ref_columns.append(r["ref_column_name"])

        # -------------------------
        # Indexes (excluding PK backing indexes handled above still shown here)
        # Include columns and filter definition
        # -------------------------
        cur.execute(
            """
            SELECT
                t.name AS table_name,
                i.name AS index_name,
                i.is_unique,
                i.type_desc,
                i.filter_definition,
                ic.key_ordinal,
                ic.is_included_column,
                col.name AS column_name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id
            JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
            JOIN sys.columns col ON col.object_id = t.object_id AND col.column_id = ic.column_id
            WHERE s.name = 'dbo'
              AND t.is_ms_shipped = 0
              AND i.name IS NOT NULL
              AND i.is_hypothetical = 0
            ORDER BY t.name, i.name, ic.is_included_column, ic.key_ordinal, col.name;
            """
        )
        idx_rows = fetchall_dict(cur)
        indexes_by_table: Dict[str, Dict[str, IndexInfo]] = {}
        for r in idx_rows:
            table = r["table_name"]
            iname = r["index_name"]
            by_name = indexes_by_table.setdefault(table, {})
            if iname not in by_name:
                by_name[iname] = IndexInfo(
                    table=table,
                    index_name=iname,
                    is_unique=bool(r["is_unique"]),
                    type_desc=r["type_desc"],
                    columns=[],
                    includes=[],
                    filter_definition=r["filter_definition"],
                )
            if bool(r["is_included_column"]):
                if r["column_name"] not in by_name[iname].includes:
                    by_name[iname].includes.append(r["column_name"])
            else:
                # key columns ordered by key_ordinal; if key_ordinal is 0 (rare), append
                by_name[iname].columns.append(r["column_name"])

        # -------------------------
        # Optional: row counts (approx via partition stats)
        # -------------------------
        row_counts: Dict[str, int] = {}
        if INCLUDE_ROW_COUNTS:
            cur.execute(
                """
                SELECT
                    t.name AS table_name,
                    SUM(ps.row_count) AS row_count
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                JOIN sys.dm_db_partition_stats ps ON ps.object_id = t.object_id AND ps.index_id IN (0,1)
                WHERE s.name='dbo'
                  AND t.is_ms_shipped = 0
                GROUP BY t.name
                ORDER BY t.name;
                """
            )
            rc_rows = fetchall_dict(cur)
            row_counts = {r["table_name"]: int(r["row_count"]) for r in rc_rows}

    # -------------------------
    # Render Markdown
    # -------------------------
    lines: List[str] = []
    lines.append(f"# DealRadar Database Data Dictionary (dbo)\n")
    lines.append(f"- Generated: **{utc_now_str()}**\n")
    lines.append(f"- Server: `{SQL_SERVER}`\n")
    lines.append(f"- Database: `{SQL_DATABASE}`\n")
    lines.append(f"- Include row counts: `{INCLUDE_ROW_COUNTS}`\n")
    lines.append("\n---\n")

    for table in table_names:
        lines.append(f"## dbo.{table}\n")

        if INCLUDE_ROW_COUNTS and table in row_counts:
            lines.append(f"- Approx rows: **{row_counts[table]}**\n")

        # Columns
        lines.append("### Columns\n")
        lines.append("| # | Name | Type | Nullable | Identity | Default |\n")
        lines.append("|---:|---|---|:---:|:---:|---|\n")

        for c in columns_by_table.get(table, []):
            # Render type with precision/scale/length when relevant
            dtype = c.data_type
            if dtype in ("nvarchar", "varchar", "char", "nchar"):
                # max_length is in bytes for nvarchar; for display, show characters:
                if dtype.startswith("n"):
                    length = "MAX" if c.max_length == -1 else str(int(c.max_length / 2))
                else:
                    length = "MAX" if c.max_length == -1 else str(c.max_length)
                dtype_disp = f"{dtype}({length})"
            elif dtype in ("decimal", "numeric"):
                dtype_disp = f"{dtype}({c.precision},{c.scale})"
            else:
                dtype_disp = dtype

            default = c.default_definition or ""
            default = default.replace("\n", " ").replace("\r", " ")
            lines.append(
                f"| {c.column_id} | `{c.name}` | {dtype_disp} | "
                f"{'YES' if c.is_nullable else 'NO'} | "
                f"{'YES' if c.is_identity else 'NO'} | "
                f"{default} |\n"
            )

        # Constraints
        cons = list((constraints_by_table.get(table) or {}).values())
        if cons:
            lines.append("\n### Key Constraints\n")
            for con in cons:
                cols = ", ".join(f"`{x}`" for x in con.columns)
                lines.append(f"- **{con.constraint_type}** `{con.constraint_name}`: ({cols})\n")
        else:
            lines.append("\n### Key Constraints\n- _(none)_\n")

        # Foreign keys
        fks = list((fks_by_table.get(table) or {}).values())
        if fks:
            lines.append("\n### Foreign Keys\n")
            for fk in fks:
                cols = ", ".join(f"`{x}`" for x in fk.columns)
                ref_cols = ", ".join(f"`{x}`" for x in fk.ref_columns)
                lines.append(
                    f"- `{fk.fk_name}`: ({cols}) → `dbo.{fk.ref_table}` ({ref_cols}) "
                    f"[ON DELETE {fk.on_delete}, ON UPDATE {fk.on_update}]\n"
                )
        else:
            lines.append("\n### Foreign Keys\n- _(none)_\n")

        # Indexes
        idxs = list((indexes_by_table.get(table) or {}).values())
        if idxs:
            lines.append("\n### Indexes\n")
            for ix in idxs:
                cols = ", ".join(f"`{x}`" for x in ix.columns) if ix.columns else ""
                incs = ", ".join(f"`{x}`" for x in ix.includes) if ix.includes else ""
                filt = f" FILTER ({ix.filter_definition})" if ix.filter_definition else ""
                uniq = "UNIQUE " if ix.is_unique else ""
                lines.append(
                    f"- {uniq}{ix.type_desc} `{ix.index_name}`: ({cols})"
                    + (f" INCLUDE ({incs})" if incs else "")
                    + filt
                    + "\n"
                )
        else:
            lines.append("\n### Indexes\n- _(none)_\n")

        lines.append("\n---\n")

    OUTPUT_MD.write_text("".join(lines), encoding="utf-8")
    print(f"Wrote: {OUTPUT_MD.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
