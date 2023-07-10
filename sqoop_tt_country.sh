#!/bin/bash
source /opt/gtnexus/sqoop/scripts/dimension/dimension_sqoop_properties.sh

sqoop job --delete job_sqp_dimension_tt_country_full;

sqoop job --options-file "$SQP_GENERIC_PROP" --create job_sqp_dimension_tt_country_full -- --options-file "$SQP_TX_CONN_PROP"  --username "$SQP_USERNAME"  --password-alias "$SQP_PASSWORD" --target-dir "/dimension/common/tt_country" --e "
SELECT
    country_id
,   CASE WHEN country_id = 0 THEN 'None' ELSE name END AS name
,   CASE WHEN country_id = 0 THEN 'None' ELSE code END AS code
FROM tt_country
WHERE 1 = 1 AND \$CONDITIONS" --split-by country_id --delete-target-dir --fields-terminated-by '\001' --lines-terminated-by '\n' -null-string '' -null-non-string ''