DECLARE @year INT = 2024;

;WITH fs_current AS (
    SELECT
        orgnr,
        [year],
        revenue,
        ebit,
        ebitda,
        assets,
        equity
    FROM dbo.financial_statement
    WHERE [year] = @year
      AND source IN (N'proff', N'proff_forvalt_excel')
      AND account_view = N'company'
),
ebit_history AS (
    SELECT
        orgnr,
        ebit
    FROM dbo.financial_statement
    WHERE [year] BETWEEN (@year - 2) AND @year
      AND source IN (N'proff', N'proff_forvalt_excel')
      AND account_view = N'company'
),
ebit_agg AS (
    SELECT
        orgnr,
        AVG(CAST(ebit AS FLOAT)) AS avg_ebit_3yr
    FROM ebit_history
    GROUP BY orgnr
),
feat AS (
    SELECT
        fs_current.orgnr,
        fs_current.[year],
        fs_current.revenue,
        fs_current.ebit,
        fs_current.ebitda,
        fs_current.assets,
        fs_current.equity,
        ebit_agg.avg_ebit_3yr,
        CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE ebit / revenue END AS ebit_margin,
        CASE WHEN equity  IS NULL OR equity  = 0 THEN NULL ELSE ebit / equity  END AS roe_proxy,
        CASE WHEN assets  IS NULL OR assets  = 0 THEN NULL ELSE equity / assets END AS equity_ratio
    FROM fs_current
    LEFT JOIN ebit_agg
        ON ebit_agg.orgnr = fs_current.orgnr
),
scored AS (
    SELECT
        orgnr,
        [year],

        -- ---------- BQS (0-100) ----------
        (
            0.30 * (CASE
                WHEN roe_proxy IS NULL THEN 0
                WHEN roe_proxy <= 0 THEN 0
                WHEN roe_proxy >= 0.25 THEN 100
                ELSE (roe_proxy / 0.25) * 100
            END)
          + 0.25 * (CASE
                WHEN ebit_margin IS NULL THEN 0
                WHEN ebit_margin <= 0 THEN 0
                WHEN ebit_margin >= 0.30 THEN 100
                ELSE (ebit_margin / 0.30) * 100
            END)
          + 0.15 * (CASE
                WHEN equity_ratio IS NULL THEN 0
                WHEN equity_ratio <= 0.10 THEN 0
                WHEN equity_ratio >= 0.50 THEN 100
                ELSE ((equity_ratio - 0.10) / (0.50 - 0.10)) * 100
            END)
          + 0.30 * (CASE
                WHEN avg_ebit_3yr IS NULL THEN 0
                WHEN avg_ebit_3yr >= 300000 THEN 100   -- 300 MNOK avg EBIT (3yr)
                WHEN avg_ebit_3yr >= 150000 THEN 85
                WHEN avg_ebit_3yr >=  75000 THEN 70
                WHEN avg_ebit_3yr >=  40000 THEN 55
                WHEN avg_ebit_3yr >=  20000 THEN 40
                ELSE 20
            END)
        ) AS bqs_score,

        -- ---------- DPS (0-100) ----------
        (
            0.60 * (CASE
                WHEN avg_ebit_3yr IS NULL THEN 0
                WHEN avg_ebit_3yr >= 300000 THEN 100
                WHEN avg_ebit_3yr >= 150000 THEN 85
                WHEN avg_ebit_3yr >=  75000 THEN 70
                WHEN avg_ebit_3yr >=  40000 THEN 55
                WHEN avg_ebit_3yr >=  20000 THEN 40
                ELSE 20
            END)
          + 0.40 * (CASE
                WHEN revenue IS NULL THEN 0
                WHEN revenue >= 5000000 THEN 100   -- 5 BNOK revenue
                WHEN revenue >= 2000000 THEN 85
                WHEN revenue >= 1000000 THEN 70
                WHEN revenue >=  500000 THEN 55
                WHEN revenue >=  200000 THEN 40
                ELSE 20
            END)
        ) AS dps_score,

        revenue,
        ebit,
        ebitda,
        assets,
        equity,
        avg_ebit_3yr,
        ebit_margin,
        roe_proxy
    FROM feat
),
final AS (
    SELECT
        orgnr,
        [year],
        CAST((0.70*bqs_score + 0.30*dps_score) AS FLOAT) AS quality_score,
        revenue,
        ebit,
        ebitda,
        assets,
        equity,
        avg_ebit_3yr,
        ebit_margin,
        roe_proxy,

        -- tags (simple, readable bands)
        CONCAT(
            'QS_v1;',
            'view=company;',
            'rev_band=',
                CASE
                    WHEN revenue IS NULL THEN 'na'
                    WHEN revenue >= 5000000 THEN '>=5bn'
                    WHEN revenue >= 2000000 THEN '2-5bn'
                    WHEN revenue >= 1000000 THEN '1-2bn'
                    WHEN revenue >=  500000 THEN '0.5-1bn'
                    ELSE '<0.5bn'
                END,
            ';ebit_band=',
                CASE
                    WHEN avg_ebit_3yr IS NULL THEN 'na'
                    WHEN avg_ebit_3yr >= 300000 THEN '>=300m'
                    WHEN avg_ebit_3yr >= 150000 THEN '150-300m'
                    WHEN avg_ebit_3yr >=  75000 THEN '75-150m'
                    WHEN avg_ebit_3yr >=  40000 THEN '40-75m'
                    ELSE '<40m'
                END,
            ';mrg=',
                CASE
                    WHEN ebit_margin IS NULL THEN 'na'
                    WHEN ebit_margin >= 0.30 THEN '>=30%'
                    WHEN ebit_margin >= 0.20 THEN '20-30%'
                    WHEN ebit_margin >= 0.10 THEN '10-20%'
                    WHEN ebit_margin >= 0.05 THEN '5-10%'
                    ELSE '<5%'
                END
        ) AS new_tags
    FROM scored
)
MERGE dbo.score WITH (HOLDLOCK) AS tgt
USING final AS src
ON tgt.orgnr = src.orgnr AND tgt.[year] = src.[year]

WHEN MATCHED THEN
    UPDATE SET
        compounder_score = src.quality_score,
        -- keep any existing catalyst_score; if NULL set to 0
        catalyst_score   = COALESCE(tgt.catalyst_score, 0),
        -- for now, total_score == compounder_score until DealLikelihood exists
        total_score      = src.quality_score,
        tags             = CASE
                              WHEN tgt.tags IS NULL OR LTRIM(RTRIM(tgt.tags)) = '' THEN src.new_tags
                              ELSE CONCAT(tgt.tags, ' | ', src.new_tags)
                           END,
        computed_at      = SYSUTCDATETIME()

WHEN NOT MATCHED THEN
    INSERT (orgnr, [year], total_score, compounder_score, catalyst_score, tags, computed_at)
    VALUES (src.orgnr, src.[year], src.quality_score, src.quality_score, 0, src.new_tags, SYSUTCDATETIME());

-- Quick check
SELECT TOP 50 orgnr, [year], total_score, compounder_score, catalyst_score, tags, computed_at
FROM dbo.score
WHERE [year] = @year
ORDER BY compounder_score DESC;
