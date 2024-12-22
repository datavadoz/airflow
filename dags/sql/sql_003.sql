WITH all_sources AS (
  SELECT `date`
    , SUM(cost) / 23500                             AS total_cost
    , SUM(cost) / 23500 / SUM(landing_page_views)   AS cpc
    , SUM(cost) / 23500 / SUM(website_adds_to_cart) AS cpa
  FROM `datavadoz-438714.cps_monitor_gsheet.facebook_partition`
  WHERE 1 = 1
    {dmc3_condition}
    AND `date` IN ('{today}', '{previous_date}')
  GROUP BY `date`
  ORDER BY `date` DESC
)
, all_sources_report AS (
  SELECT `date`
    , ROUND(total_cost, 2) AS total_cost
    , ROUND(cpc, 2)        AS cpc
    , ROUND(cpa, 2)        AS cpa
    , ROUND((total_cost - LAG(total_cost) OVER (ORDER BY `date`)) / LAG(total_cost) OVER (ORDER BY `date`) * 100.0, 2) AS diff_cost
    , ROUND((cpc - LAG(cpc) OVER (ORDER BY `date`)) / LAG(cpc) OVER (ORDER BY `date`) * 100.0, 2)                      AS diff_cpc
    , ROUND((cpa - LAG(cpa) OVER (ORDER BY `date`)) / LAG(cpa) OVER (ORDER BY `date`) * 100.0, 2)                      AS diff_cpa
    , 'all' AS `source`
  FROM all_sources
  ORDER BY `date` DESC
)
, specific_source AS (
  SELECT `date`
    , `source`
    , SUM(cost) / 23500                             AS total_cost
    , SUM(cost) / 23500 / SUM(landing_page_views)   AS cpc
    , SUM(cost) / 23500 / SUM(website_adds_to_cart) AS cpa
  FROM `datavadoz-438714.cps_monitor_gsheet.facebook_partition`
  WHERE 1 = 1
    {dmc3_condition}
    AND `date` IN ('{today}', '{previous_date}')
  GROUP BY `date`, `source`
  ORDER BY `date` DESC
)
, specific_source_report AS (
  SELECT `date`
    , ROUND(total_cost, 2) AS total_cost
    , ROUND(cpc, 2)        AS cpc
    , ROUND(cpa, 2)        AS cpa
    , ROUND((total_cost - LAG(total_cost) OVER (PARTITION BY `source` ORDER BY `date`)) / LAG(total_cost) OVER (PARTITION BY `source` ORDER BY `date`) * 100.0, 2) AS diff_cost
    , ROUND((cpc - LAG(cpc) OVER (PARTITION BY `source` ORDER BY `date`)) / LAG(cpc) OVER (ORDER BY `date`) * 100.0, 2)                                            AS diff_cpc
    , ROUND((cpa - LAG(cpa) OVER (PARTITION BY `source` ORDER BY `date`)) / LAG(cpa) OVER (ORDER BY `date`) * 100.0, 2)                                            AS diff_cpa
    , `source`
  FROM specific_source
)
, final_report AS (
  SELECT * FROM all_sources_report
  UNION ALL
  SELECT * FROM specific_source_report
)

SELECT *
FROM final_report
ORDER BY `date` DESC, `source` DESC
;
