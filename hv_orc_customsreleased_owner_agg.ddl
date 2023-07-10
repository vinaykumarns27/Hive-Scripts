SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_customsreleased_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_customsreleased_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    a.carrierorg_id AS carrierorgid
,   a.carriername
,   a.carriercluster
,   a.ownerorg_id AS ownerorgid
,   a.ownerorgname
,   a.lanecityname
,   a.lanecitysubdivision
,   a.lanecitycluster1
,   a.lanecitycluster2
,   a.lanecountryname
,   a.laneregion1
,   a.laneregion2
,   a.polcity_id AS polcityid
,   a.polcityname
,   a.polcityunlocode
,   a.polcitylongitude
,   a.polcitylatitude
,   a.polcitysubdivision
,   a.polcitycluster1
,   a.polcitycluster2
,   a.polcountry_id AS polcountryid
,   a.polcountryname
,   a.polregion1
,   a.polregion2
,   a.podcity_id AS podcityid
,   a.podcityname
,   a.podcityunlocode
,   a.podcitylongitude
,   a.podcitylatitude
,   a.podcitysubdivision
,   a.podcitycluster1
,   a.podcitycluster2
,   a.podcountry_id AS podcountryid
,   a.podcountryname
,   a.podregion1
,   a.podregion2
,   a.transshipcity_id AS transshipcityid
,   a.transshipcityname
,   a.transshipcityunlocode
,   a.transshipcitylongitude
,   a.transshipcitylatitude
,   a.transshipcitysubdivision
,   a.transshipcitycluster1
,   a.transshipcitycluster2
,   a.transshipcountry_id AS transshipcountryid
,   a.transshipcountryname
,   a.transshipregion1
,   a.transshipregion2
,   MIN(a.transshipcityflag) AS transshipcityflag
,   a.containertypecluster
,   a.movetypecluster
,   a.originmovetypecluster
,   MIN(a.originmovetypeclusterflag) AS originmovetypeclusterflag
,   a.destinationmovetypecluster
,   MIN(a.destinationmovetypeclusterflag) AS destinationmovetypeclusterflag
,   MAX(b.date_dim_id) AS datedimid
,   MAX(b.the_date2) AS weekenddate
,   b.the_year AS year
,   b.the_quarter AS quarter
,   b.the_year_quarter AS yearquarter
,   b.the_month AS month
,   b.the_year_month AS yearmonth
,   b.the_week AS week
,   b.the_year_week AS yearweek
,   COUNT(customsreleasedtime) AS customsreleasecontainercount
,   COUNT(customsreleased) AS customsreleasetimecontainercount
,   COUNT(case when customsreleased > 0 THEN customsreleased ELSE NULL END) AS positivecustomsreleasetimecontainercount
,   COUNT(case when customsreleased < 0 THEN customsreleased ELSE NULL END) AS negativecustomsreleasetimecontainercount
,   COUNT(case when customsreleased > 0 AND customsreleased <= 1 THEN customsreleased ELSE NULL END) AS customsreleasetime0lessthanorequalto1dayscontainercount
,   COUNT(case when customsreleased > 1 AND customsreleased <= 2 THEN customsreleased ELSE NULL END) AS customsreleasetime1lessthanorequalto2dayscontainercount
,   COUNT(case when customsreleased > 2 AND customsreleased <= 3 THEN customsreleased ELSE NULL END) AS customsreleasetime2lessthanorequalto3dayscontainercount
,   COUNT(case when customsreleased > 3 AND customsreleased <= 4 THEN customsreleased ELSE NULL END) AS customsreleasetime3lessthanorequalto4dayscontainercount
,   COUNT(case when customsreleased > 4 AND customsreleased <= 5 THEN customsreleased ELSE NULL END) AS customsreleasetime4lessthanorequalto5dayscontainercount
,   COUNT(case when customsreleased > 5 AND customsreleased <= 6 THEN customsreleased ELSE NULL END) AS customsreleasetime5lessthanorequalto6dayscontainercount
,   COUNT(case when customsreleased > 6 AND customsreleased <= 7 THEN customsreleased ELSE NULL END) AS customsreleasetime6lessthanorequalto7dayscontainercount
,   COUNT(case when customsreleased > 7 AND customsreleased <= 8 THEN customsreleased ELSE NULL END) AS customsreleasetime7lessthanorequalto8dayscontainercount
,   COUNT(case when customsreleased > 8 THEN customsreleased ELSE NULL END) AS customsreleasetimegreaterthan8dayscontainercount
,   SUM(customsreleased) AS customsreleasetimedayssum
,   SUM(CASE WHEN customsreleased > 0 THEN customsreleased ELSE NULL END) AS positivecustomsreleasetimedayssum
,   SUM(CASE WHEN customsreleased < 0 THEN customsreleased ELSE NULL END) AS negativecustomsreleasetimedayssum
,   MIN(CASE WHEN customsreleased > 0 THEN customsreleased ELSE NULL END) AS minpositivecustomsreleasetimedayssum
,   MAX(CASE WHEN customsreleased > 0 THEN customsreleased ELSE NULL END) AS maxpositivecustomsreleasetimedayssum
FROM hv_orc_tt_vmcontainermove_owner_base AS a
    INNER JOIN dimension_common.hv_ext_tt_dw_date_dim AS b
    ON b.the_date = to_date(a.customsreleasedtime)
WHERE a.carrierorg_id > 0
AND a.ownerorg_id > 0
AND a.polcity_id > 0
AND a.podcity_id > 0
AND a.movetype_id > 0
GROUP BY
    a.carrierorg_id
,   a.carriername
,   a.carriercluster
,   a.ownerorg_id
,   a.ownerorgname
,   a.lanecityname
,   a.lanecitysubdivision
,   a.lanecitycluster1
,   a.lanecitycluster2
,   a.lanecountryname
,   a.laneregion1
,   a.laneregion2
,   a.polcity_id
,   a.polcityname
,   a.polcityunlocode
,   a.polcitylongitude
,   a.polcitylatitude
,   a.polcitysubdivision
,   a.polcitycluster1
,   a.polcitycluster2
,   a.polcountry_id
,   a.polcountryname
,   a.polregion1
,   a.polregion2
,   a.podcity_id
,   a.podcityname
,   a.podcityunlocode
,   a.podcitylongitude
,   a.podcitylatitude
,   a.podcitysubdivision
,   a.podcitycluster1
,   a.podcitycluster2
,   a.podcountry_id
,   a.podcountryname
,   a.podregion1
,   a.podregion2
,   a.transshipcity_id
,   a.transshipcityname
,   a.transshipcityunlocode
,   a.transshipcitylongitude
,   a.transshipcitylatitude
,   a.transshipcitysubdivision
,   a.transshipcitycluster1
,   a.transshipcitycluster2
,   a.transshipcountry_id
,   a.transshipcountryname
,   a.transshipregion1
,   a.transshipregion2
,   a.containertypecluster
,   a.movetypecluster
,   a.originmovetypecluster
,   a.destinationmovetypecluster
,   b.the_year
,   b.the_quarter
,   b.the_year_quarter
,   b.the_month
,   b.the_year_month
,   b.the_week
,   b.the_year_week;

DROP TABLE IF EXISTS hv_orc_customsreleased_owner_agg;

ALTER TABLE hv_orc_customsreleased_owner_agg_temp RENAME TO hv_orc_customsreleased_owner_agg;