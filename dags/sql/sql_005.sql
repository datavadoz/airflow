WITH all_channels AS (
  SELECT `date`
    , SUM(cost)                                                     AS total_cost
    , IF(SUM(clicks) = 0, NULL, SUM(cost) / SUM(clicks))            AS cpc
    , IF(SUM(conversions) = 0, NULL, SUM(cost)  / SUM(conversions)) AS cpa
  FROM `datavadoz-438714.cps_monitor_gsheet.google_partition`
  WHERE 1 = 1
    {dmc3_condition}
    AND `date` IN ('{today}', '{previous_date}')
  GROUP BY `date`
  ORDER BY `date` DESC
)
, all_channels_report AS (
  SELECT `date`
    , ROUND(total_cost, 2) AS total_cost
    , ROUND(cpc, 2)        AS cpc
    , ROUND(cpa, 2)        AS cpa
    , CASE
        WHEN total_cost IS NULL AND LAG(total_cost) OVER (ORDER BY `date`) IS NULL THEN NULL
        WHEN total_cost IS NULL AND LAG(total_cost) OVER (ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN total_cost IS NOT NULL AND LAG(total_cost) OVER (ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((total_cost - LAG(total_cost) OVER (ORDER BY `date`)) / LAG(total_cost) OVER (ORDER BY `date`) * 100.0, 2)
      END AS diff_cost
    , CASE
        WHEN cpc IS NULL AND LAG(cpc) OVER (ORDER BY `date`) IS NULL THEN NULL
        WHEN cpc IS NULL AND LAG(cpc) OVER (ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN cpc IS NOT NULL AND LAG(cpc) OVER (ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((cpc - LAG(cpc) OVER (ORDER BY `date`)) / LAG(cpc) OVER (ORDER BY `date`) * 100.0, 2)
      END AS diff_cpc
    , CASE
        WHEN cpa IS NULL AND LAG(cpa) OVER (ORDER BY `date`) IS NULL THEN NULL
        WHEN cpa IS NULL AND LAG(cpa) OVER (ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN cpa IS NOT NULL AND LAG(cpa) OVER (ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((cpa - LAG(cpa) OVER (ORDER BY `date`)) / LAG(cpa) OVER (ORDER BY `date`) * 100.0, 2)
      END AS diff_cpa
    , 'all' AS `source`
  FROM all_channels
  ORDER BY `date` DESC
)
, specific_channel AS (
  SELECT `date`
    , `channel`
    , SUM(cost)                                                     AS total_cost
    , IF(SUM(clicks) = 0, NULL, SUM(cost) / SUM(clicks))            AS cpc
    , IF(SUM(conversions) = 0, NULL, SUM(cost)  / SUM(conversions)) AS cpa
  FROM `datavadoz-438714.cps_monitor_gsheet.google_partition`
  WHERE 1 = 1
    {dmc3_condition}
    AND `date` IN ('{today}', '{previous_date}')
  GROUP BY `date`, `channel`
  ORDER BY `date` DESC
)
, specific_channel_report AS (
  SELECT `date`
    , ROUND(total_cost, 2) AS total_cost
    , ROUND(cpc, 2)        AS cpc
    , ROUND(cpa, 2)        AS cpa
    , CASE
        WHEN total_cost IS NULL AND LAG(total_cost) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN NULL
        WHEN total_cost IS NULL AND LAG(total_cost) OVER (PARTITION BY `channel` ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN total_cost IS NOT NULL AND LAG(total_cost) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((total_cost - LAG(total_cost) OVER (PARTITION BY `channel` ORDER BY `date`)) / LAG(total_cost) OVER (PARTITION BY `channel` ORDER BY `date`) * 100.0, 2)
      END AS diff_cost
    , CASE
        WHEN cpc IS NULL AND LAG(cpc) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN NULL
        WHEN cpc IS NULL AND LAG(cpc) OVER (PARTITION BY `channel` ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN cpc IS NOT NULL AND LAG(cpc) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((cpc - LAG(cpc) OVER (PARTITION BY `channel` ORDER BY `date`)) / LAG(cpc) OVER (PARTITION BY `channel` ORDER BY `date`) * 100.0, 2)
      END AS diff_cpc
    , CASE
        WHEN cpa IS NULL AND LAG(cpa) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN NULL
        WHEN cpa IS NULL AND LAG(cpa) OVER (PARTITION BY `channel` ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN cpa IS NOT NULL AND LAG(cpa) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((cpa - LAG(cpa) OVER (PARTITION BY `channel` ORDER BY `date`)) / LAG(cpa) OVER (PARTITION BY `channel` ORDER BY `date`) * 100.0, 2)
      END AS diff_cpa
    , `channel` AS `source`
  FROM specific_channel
)
, final_report AS (
  SELECT * FROM all_channels_report
  UNION ALL
  SELECT * FROM specific_channel_report
)

SELECT *
FROM final_report
ORDER BY `date` DESC, `source` DESC
;
