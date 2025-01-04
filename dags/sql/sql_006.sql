WITH all_channels AS (
  SELECT `date`
    , SUM(sessions) AS total_sessions
  FROM `datavadoz-438714.cps_monitor_gsheet.ga4_partition`
  WHERE 1 = 1
    {dmc3_condition}
    AND `date` IN ('{today}', '{previous_date}')
  GROUP BY `date`
  ORDER BY `date` DESC
)
, all_channels_report AS (
  SELECT `date`
    , ROUND(total_sessions, 2) AS total_sessions
    , CASE
        WHEN total_sessions IS NULL AND LAG(total_sessions) OVER (ORDER BY `date`) IS NULL THEN NULL
        WHEN total_sessions IS NULL AND LAG(total_sessions) OVER (ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN total_sessions IS NOT NULL AND LAG(total_sessions) OVER (ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((total_sessions - LAG(total_sessions) OVER (ORDER BY `date`)) / LAG(total_sessions) OVER (ORDER BY `date`) * 100.0, 2)
      END AS diff_sessions
    , 'all' AS `source`
  FROM all_channels
  ORDER BY `date` DESC
)
, specific_channel AS (
  SELECT `date`
    , `channel`
    , SUM(sessions) AS total_sessions
  FROM `datavadoz-438714.cps_monitor_gsheet.ga4_partition`
  WHERE 1 = 1
    {dmc3_condition}
    AND `date` IN ('{today}', '{previous_date}')
  GROUP BY `date`, `channel`
  ORDER BY `date` DESC
)
, specific_channel_report AS (
  SELECT `date`
    , ROUND(total_sessions, 2) AS total_sessions
    , CASE
        WHEN total_sessions IS NULL AND LAG(total_sessions) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN NULL
        WHEN total_sessions IS NULL AND LAG(total_sessions) OVER (PARTITION BY `channel` ORDER BY `date`) IS NOT NULL THEN -100.0
        WHEN total_sessions IS NOT NULL AND LAG(total_sessions) OVER (PARTITION BY `channel` ORDER BY `date`) IS NULL THEN 100.0
        ELSE ROUND((total_sessions - LAG(total_sessions) OVER (PARTITION BY `channel` ORDER BY `date`)) / LAG(total_sessions) OVER (PARTITION BY `channel` ORDER BY `date`) * 100.0, 2)
      END AS diff_cost
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
