/* ============================================================
   DealRadar - Health Dashboard (per batch)
   ============================================================ */

WITH batch AS (
    SELECT b.batch_id, b.batch_name, i.orgnr
    FROM dbo.import_batch b
    JOIN dbo.import_batch_item i ON i.batch_id = b.batch_id
),
raw_ok AS (
    SELECT orgnr, fetched_at_utc
    FROM dbo.proff_raw_company
    WHERE http_status = 200 AND payload_json IS NOT NULL
),
fin_items AS (
    SELECT DISTINCT orgnr
    FROM dbo.proff_financial_item
),
score_2024 AS (
    SELECT DISTINCT orgnr
    FROM dbo.score
    WHERE [year] = 2024
),
batch_agg AS (
    SELECT
        b.batch_id,
        b.batch_name,
        COUNT(*) AS batch_size,

        SUM(CASE WHEN r.orgnr IS NOT NULL THEN 1 ELSE 0 END) AS raw_ok_200,
        SUM(CASE WHEN f.orgnr IS NOT NULL THEN 1 ELSE 0 END) AS has_fin_items,
        SUM(CASE WHEN s.orgnr IS NOT NULL THEN 1 ELSE 0 END) AS has_score_2024,

        MAX(r.fetched_at_utc) AS last_successful_ingestion_utc
    FROM batch b
    LEFT JOIN raw_ok r ON r.orgnr = b.orgnr
    LEFT JOIN fin_items f ON f.orgnr = b.orgnr
    LEFT JOIN score_2024 s ON s.orgnr = b.orgnr
    GROUP BY b.batch_id, b.batch_name
)
SELECT
    batch_id,
    batch_name,
    batch_size,

    raw_ok_200,
    CAST(100.0 * raw_ok_200 / NULLIF(batch_size, 0) AS DECIMAL(6,2)) AS pct_raw_ok,

    has_fin_items,
    CAST(100.0 * has_fin_items / NULLIF(batch_size, 0) AS DECIMAL(6,2)) AS pct_fin_items,

    has_score_2024,
    CAST(100.0 * has_score_2024 / NULLIF(batch_size, 0) AS DECIMAL(6,2)) AS pct_score_2024,

    last_successful_ingestion_utc,
    CASE
        WHEN last_successful_ingestion_utc IS NULL THEN NULL
        ELSE DATEDIFF(DAY, last_successful_ingestion_utc, SYSUTCDATETIME())
    END AS days_since_last_successful_ingestion
FROM batch_agg
ORDER BY batch_name;
