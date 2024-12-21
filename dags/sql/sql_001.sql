CREATE OR REPLACE TABLE `{full_table_id}_partition`
PARTITION BY `{partitioned_column}`
AS
SELECT * FROM `{full_table_id}`
;
