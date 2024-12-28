SELECT dmc3
  , ROUND(sales * 100, 2)                                                   AS sales
  , ROUND(mom * 100, 2)                                                     AS mom
  , ROUND(actual_budget_perc * 100, 2)                                      AS actual_budget_perc
  , ROUND(plan_budget_perc * 100, 2)                                        AS plan_budget_perc
  , IF(plan_budget = 0, NULL, ROUND(actual_digital / plan_budget * 100, 2))            AS actual_vs_plan_budget
  , IF(plan_fb_catalog = 0, NULL, ROUND(actual_fb_catalog / plan_fb_catalog * 100, 2)) AS actual_vs_plan_fb_catalog
  , IF(plan_fb = 0, NULL, ROUND(actual_fb / plan_fb * 100, 2))                         AS actual_vs_plan_fb
  , IF(plan_gg = 0, NULL, ROUND(actual_gg / plan_gg * 100, 2))                         AS actual_vs_plan_gg
  , IF(plan_tt = 0, NULL, ROUND(actual_tt / plan_tt * 100, 2))                         AS actual_vs_plan_tt
  , IF(plan_new_camp_dynamic_search = 0, NULL, ROUND(actual_new_camp_dynamic_search / plan_new_camp_dynamic_search * 100, 2)) AS actual_vs_plan_dynamic
  , IF(plan_criteo = 0, NULL, ROUND(actual_criteo / plan_criteo * 100, 2))             AS actual_vs_plan_criteo
FROM `datavadoz-438714.cps_monitor_gsheet.cost_run_rate`
;
