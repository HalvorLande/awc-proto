DECLARE @year INT = 2024;

;WITH fs AS (
    SELECT orgnr, [year], revenue, ebit, ebitda, assets, equity
    FROM dbo.financial_statement
    WHERE [year] BETWEEN @year-2 AND @year
      AND source IN (N'proff', N'proff_forvalt_excel')
      AND account_view = N'company'
),
feat_y AS (
    SELECT
        orgnr,
        [year],
        revenue,
        ebit,
        ebitda,
        assets,
        equity,
        CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE ebitda / revenue END AS ebitda_margin,
        CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE ebit / revenue END    AS ebit_margin,
        CASE WHEN equity  IS NULL OR equity  = 0 THEN NULL ELSE ebit / equity END     AS roe_proxy,
        CASE WHEN assets  IS NULL OR assets  = 0 THEN NULL ELSE equity / assets END   AS equity_ratio
    FROM fs
),
feat_3y AS (
    SELECT
        orgnr,
        -- levels (use avg/median-ish)
        AVG(revenue)      AS revenue_avg_3y,
        AVG(ebit)         AS ebit_avg_3y,
        AVG(ebitda)       AS ebitda_avg_3y,
        AVG(ebit_margin)  AS ebit_margin_avg_3y,
        AVG(ebitda_margin)AS ebitda_margin_avg_3y,
        AVG(roe_proxy)    AS roe_proxy_avg_3y,
        AVG(equity_ratio) AS equity_ratio_avg_3y,

        -- stability
        MIN(ebitda) AS ebitda_min_3y,
        COUNT(*)    AS yrs,
        SUM(CASE WHEN ebitda > 0 THEN 1 ELSE 0 END) AS ebitda_pos_years_3y,
        STDEV(ebitda_margin) AS ebitda_margin_stdev_3y,
        STDEV(ebitda)        AS ebitda_stdev_3y,

        -- last-year values for DPS sizing and “freshness”
        MAX(CASE WHEN [year] = @year THEN revenue END) AS revenue_y,
        MAX(CASE WHEN [year] = @year THEN ebit END)    AS ebit_y,
        MAX(CASE WHEN [year] = @year THEN ebitda END)  AS ebitda_y
    FROM feat_y
    GROUP BY orgnr
),
growth AS (
    SELECT
        a.orgnr,
        -- revenue CAGR from (@year-2) -> @year (requires both present & >0)
        CASE
          WHEN r0.revenue > 0 AND r2.revenue > 0 THEN POWER(1.0 * r2.revenue / r0.revenue, 1.0/2) - 1
          ELSE NULL
        END AS revenue_cagr_3y,
        CASE
          WHEN e0.ebitda > 0 AND e2.ebitda > 0 THEN POWER(1.0 * e2.ebitda / e0.ebitda, 1.0/2) - 1
          ELSE NULL
        END AS ebitda_cagr_3y
    FROM (SELECT DISTINCT orgnr FROM fs) a
    LEFT JOIN fs r0 ON r0.orgnr=a.orgnr AND r0.[year]=@year-2
    LEFT JOIN fs r2 ON r2.orgnr=a.orgnr AND r2.[year]=@year
    LEFT JOIN fs e0 ON e0.orgnr=a.orgnr AND e0.[year]=@year-2
    LEFT JOIN fs e2 ON e2.orgnr=a.orgnr AND e2.[year]=@year
),
scored AS (
    SELECT
        f.orgnr,
        @year AS [year],

        -- --- Stability multiplier (0.1 .. 1.0) ---
        CAST(
          CASE
            WHEN f.yrs < 3 THEN 0.70  -- less history => cautious
            WHEN f.ebitda_min_3y <= 0 THEN 0.25
            ELSE
              /* volatility penalty using EBITDA-margin stdev */
              CASE
                WHEN f.ebitda_margin_stdev_3y IS NULL THEN 0.85
                WHEN f.ebitda_margin_stdev_3y <= 0.05 THEN 1.00
                WHEN f.ebitda_margin_stdev_3y >= 0.20 THEN 0.70
                ELSE 1.00 - ((f.ebitda_margin_stdev_3y - 0.05) / (0.20 - 0.05)) * 0.30
              END
          END
        AS FLOAT) AS stability_mult,

        -- --- Growth score (0-100) ---
        CAST(
          0.5 * CASE
            WHEN g.revenue_cagr_3y IS NULL THEN 0
            WHEN g.revenue_cagr_3y <= 0 THEN 0
            WHEN g.revenue_cagr_3y >= 0.20 THEN 100
            ELSE (g.revenue_cagr_3y / 0.20) * 100
          END
        + 0.5 * CASE
            WHEN g.ebitda_cagr_3y IS NULL THEN 0
            WHEN g.ebitda_cagr_3y <= 0 THEN 0
            WHEN g.ebitda_cagr_3y >= 0.25 THEN 100
            ELSE (g.ebitda_cagr_3y / 0.25) * 100
          END
        AS FLOAT) AS growth_score,

        -- --- “Profitability” score ---
        CAST(
          CASE
            WHEN f.ebitda_margin_avg_3y IS NULL THEN 0
            WHEN f.ebitda_margin_avg_3y <= 0 THEN 0
            WHEN f.ebitda_margin_avg_3y >= 0.25 THEN 100
            ELSE (f.ebitda_margin_avg_3y / 0.25) * 100
          END
        AS FLOAT) AS profitability_score,

        -- --- ROE proxy ---
        CAST(
          CASE
            WHEN f.roe_proxy_avg_3y IS NULL THEN 0
            WHEN f.roe_proxy_avg_3y <= 0 THEN 0
            WHEN f.roe_proxy_avg_3y >= 0.25 THEN 100
            ELSE (f.roe_proxy_avg_3y / 0.25) * 100
          END
        AS FLOAT) AS roe_score,

        -- --- Robustness ---
        CAST(
          CASE
            WHEN f.equity_ratio_avg_3y IS NULL THEN 0
            WHEN f.equity_ratio_avg_3y <= 0.10 THEN 0
            WHEN f.equity_ratio_avg_3y >= 0.50 THEN 100
            ELSE ((f.equity_ratio_avg_3y - 0.10) / (0.50 - 0.10)) * 100
          END
        AS FLOAT) AS robustness_score,

        -- --- Pass through metrics for tagging ---
        f.revenue_y,
        f.ebit_y,
        f.ebitda_y,
        g.revenue_cagr_3y,
        g.ebitda_cagr_3y,
        f.ebitda_min_3y,
        f.ebitda_margin_stdev_3y
    FROM feat_3y f
    LEFT JOIN growth g ON g.orgnr = f.orgnr
),
final AS (
    SELECT
      orgnr,
      [year],

      /* BQS v2 calculation (kept for reference, though not inserted directly) */
      CAST(
        (
          0.25*roe_score +
          0.30*profitability_score +
          0.25*growth_score +
          0.20*robustness_score
        ) * stability_mult
      AS FLOAT) AS bqs_v2,

      /* DPS calculation */
      CAST(
        0.60 * CASE
          WHEN ebit_y IS NULL THEN 0
          WHEN ebit_y >= 400000 THEN 100
          WHEN ebit_y >= 200000 THEN 85
          WHEN ebit_y >= 100000 THEN 70
          WHEN ebit_y >=  50000 THEN 55
          WHEN ebit_y >=  20000 THEN 40
          ELSE 20
        END
      + 0.40 * CASE
          WHEN revenue_y IS NULL THEN 0
          WHEN revenue_y >= 5000000 THEN 100
          WHEN revenue_y >= 2000000 THEN 85
          WHEN revenue_y >= 1000000 THEN 70
          WHEN revenue_y >=  500000 THEN 55
          WHEN revenue_y >=  200000 THEN 40
          ELSE 20
        END
      AS FLOAT) AS dps,

      /* Final Quality Score V2 */
      CAST(
        0.70 * (
          (0.25*roe_score + 0.30*profitability_score + 0.25*growth_score + 0.20*robustness_score) * stability_mult
        )
        + 0.30 * (
          0.60 * CASE
            WHEN ebit_y IS NULL THEN 0
            WHEN ebit_y >= 400000 THEN 100
            WHEN ebit_y >= 200000 THEN 85
            WHEN ebit_y >= 100000 THEN 70
            WHEN ebit_y >=  50000 THEN 55
            WHEN ebit_y >=  20000 THEN 40
            ELSE 20
          END
          + 0.40 * CASE
            WHEN revenue_y IS NULL THEN 0
            WHEN revenue_y >= 5000000 THEN 100
            WHEN revenue_y >= 2000000 THEN 85
            WHEN revenue_y >= 1000000 THEN 70
            WHEN revenue_y >=  500000 THEN 55
            WHEN revenue_y >=  200000 THEN 40
            ELSE 20
          END
        )
      AS FLOAT) AS quality_score, -- FIXED: Renamed alias from quality_score_v2 to match MERGE

      CONCAT(
        'QS_v2;',
        'yrs=3;',
        'stability_mult=', FORMAT(stability_mult, '0.00'), ';',
        'ebitda_min=', COALESCE(CONVERT(varchar(50), ebitda_min_3y), 'na'), ';',
        'ebitda_vol=', COALESCE(FORMAT(ebitda_margin_stdev_3y, '0.000'), 'na'), ';',
        'rev_cagr=', COALESCE(FORMAT(revenue_cagr_3y, '0.0%'), 'na'), ';',
        'ebitda_cagr=', COALESCE(FORMAT(ebitda_cagr_3y, '0.0%'), 'na')
      ) AS new_tags
    FROM scored
)
MERGE dbo.score WITH (HOLDLOCK) AS tgt
USING final AS src
ON tgt.orgnr = src.orgnr AND tgt.[year] = src.[year]

WHEN MATCHED THEN
    UPDATE SET
        compounder_score = src.quality_score, -- Fixed: Matches alias in 'final' CTE
        catalyst_score   = COALESCE(tgt.catalyst_score, 0),
        total_score      = src.quality_score, -- Fixed: Matches alias in 'final' CTE
        tags             = CASE
                              -- Prevent appending the same tag string if it already exists
                              WHEN tgt.tags LIKE '%' + src.new_tags + '%' THEN tgt.tags
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