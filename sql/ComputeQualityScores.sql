DECLARE @year INT = 2024;

;WITH fs_hist AS (
    SELECT
        orgnr,
        [year],
        revenue,
        ebitda
    FROM dbo.financial_statement
    WHERE [year] BETWEEN @year - 4 AND @year
      AND source IN (N'proff', N'proff_forvalt_excel')
      AND account_view = N'company'
),
fs AS (
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
stability AS (
    SELECT
        orgnr,
        COUNT(*) AS ebitda_years,
        SUM(CASE WHEN ebitda > 0 THEN 1 ELSE 0 END) AS ebitda_positive_years,
        SUM(CASE WHEN ebitda <= 0 THEN 1 ELSE 0 END) AS ebitda_negative_years,
        STDEV(CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE ebitda / revenue END) AS ebitda_volatility,
        AVG(CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE ebitda / revenue END) AS ebitda_avg_margin
    FROM fs_hist
    GROUP BY orgnr
),
stability_scored AS (
    SELECT
        orgnr,
        ebitda_years,
        ebitda_positive_years,
        ebitda_negative_years,
        ebitda_volatility,
        ebitda_avg_margin,
        CASE
            WHEN ebitda_volatility IS NULL OR ebitda_avg_margin IS NULL OR ebitda_avg_margin = 0 THEN NULL
            ELSE ebitda_volatility / ABS(ebitda_avg_margin)
        END AS ebitda_cv,
        CASE
            WHEN ebitda_volatility IS NULL OR ebitda_avg_margin IS NULL OR ebitda_avg_margin = 0 THEN 50
            WHEN ebitda_volatility / ABS(ebitda_avg_margin) <= 0.10 THEN 100
            WHEN ebitda_volatility / ABS(ebitda_avg_margin) >= 1.00 THEN 0
            ELSE (1 - ((ebitda_volatility / ABS(ebitda_avg_margin)) - 0.10) / (1.00 - 0.10)) * 100
        END AS volatility_score,
        CASE
            WHEN ebitda_negative_years IS NULL THEN 50
            ELSE CASE
                WHEN (100 - (ebitda_negative_years * 20)) < 0 THEN 0
                ELSE (100 - (ebitda_negative_years * 20))
            END
        END AS negative_year_score
    FROM stability
),
feat AS (
    SELECT
        orgnr,
        [year],
        revenue,
        ebit,
        ebitda,
        assets,
        equity,
        CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE ebit / revenue END AS ebit_margin,
        CASE WHEN equity  IS NULL OR equity  = 0 THEN NULL ELSE ebit / equity  END AS roe_proxy,
        CASE WHEN assets  IS NULL OR assets  = 0 THEN NULL ELSE equity / assets END AS equity_ratio
    FROM fs
),
scored AS (
    SELECT
        feat.orgnr,
        feat.[year],

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
                WHEN ebit IS NULL THEN 0
                WHEN ebit >= 400000 THEN 100   -- 400 MNOK EBIT
                WHEN ebit >= 200000 THEN 85
                WHEN ebit >= 100000 THEN 70
                WHEN ebit >=  50000 THEN 55
                WHEN ebit >=  20000 THEN 40
                ELSE 20
            END)
        ) AS bqs_base_score,

        -- ---------- Stability (0-100) ----------
        COALESCE(
            CASE
                WHEN stability_scored.negative_year_score IS NULL THEN stability_scored.volatility_score
                WHEN stability_scored.volatility_score IS NULL THEN stability_scored.negative_year_score
                ELSE (0.70 * stability_scored.negative_year_score) + (0.30 * stability_scored.volatility_score)
            END,
            50
        ) AS stability_score,

        -- ---------- DPS (0-100) ----------
        (
            0.60 * (CASE
                WHEN ebit IS NULL THEN 0
                WHEN ebit >= 400000 THEN 100
                WHEN ebit >= 200000 THEN 85
                WHEN ebit >= 100000 THEN 70
                WHEN ebit >=  50000 THEN 55
                WHEN ebit >=  20000 THEN 40
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
        ebit_margin,
        roe_proxy,
        stability_scored.ebitda_years,
        stability_scored.ebitda_positive_years,
        stability_scored.ebitda_negative_years,
        stability_scored.ebitda_volatility,
        stability_scored.ebitda_avg_margin,
        stability_scored.ebitda_cv
    FROM feat
    LEFT JOIN stability_scored
        ON stability_scored.orgnr = feat.orgnr
),
final AS (
    SELECT
        orgnr,
        [year],
        CAST((0.85*bqs_base_score + 0.15*stability_score) AS FLOAT) AS bqs_score,
        CAST((0.70*(0.85*bqs_base_score + 0.15*stability_score) + 0.30*dps_score) AS FLOAT) AS quality_score,
        revenue,
        ebit,
        ebitda,
        assets,
        equity,
        ebit_margin,
        roe_proxy,
        stability_score,
        ebitda_years,
        ebitda_positive_years,
        ebitda_negative_years,
        ebitda_volatility,
        ebitda_avg_margin,
        ebitda_cv,

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
                    WHEN ebit IS NULL THEN 'na'
                    WHEN ebit >= 400000 THEN '>=400m'
                    WHEN ebit >= 200000 THEN '200-400m'
                    WHEN ebit >= 100000 THEN '100-200m'
                    WHEN ebit >=  50000 THEN '50-100m'
                    ELSE '<50m'
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
