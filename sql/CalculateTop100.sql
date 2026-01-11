DECLARE @pick_date DATE = CAST(GETDATE() AS DATE);
DECLARE @year INT = 2024;
DECLARE @top_n INT = 20;

-- 1) Clear existing picks for today (prototype-safe)
DELETE FROM dbo.daily_top_pick
WHERE pick_date = @pick_date;

-- 2) Insert new Top N
;WITH ranked AS (
    SELECT
        s.orgnr,
        s.[year],
        CAST(COALESCE(s.total_score, s.compounder_score, 0) AS FLOAT) AS score_value,
        ROW_NUMBER() OVER (
            ORDER BY COALESCE(s.total_score, s.compounder_score, 0) DESC, s.orgnr ASC
        ) AS rn
    FROM dbo.score s
    WHERE s.[year] = @year
)
INSERT INTO dbo.daily_top_pick (pick_date, rank, orgnr, reason_summary, total_score_snapshot)
SELECT
    @pick_date AS pick_date,
    r.rn AS rank,
    r.orgnr,
    CONCAT(
        'Top score in ', @year,
        ' (total=', FORMAT(r.score_value, 'N2'),
        ')'
    ) AS reason_summary,
    r.score_value AS total_score_snapshot
FROM ranked r
WHERE r.rn <= @top_n;

-- 3) View result
SELECT
    p.pick_date,
    p.rank,
    p.orgnr,
    c.name,
    p.total_score_snapshot,
    p.reason_summary
FROM dbo.daily_top_pick p
LEFT JOIN dbo.company c ON c.orgnr = p.orgnr
WHERE p.pick_date = @pick_date
ORDER BY p.rank;
