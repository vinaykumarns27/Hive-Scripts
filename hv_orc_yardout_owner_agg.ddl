SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_yardout_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_yardout_owner_agg_temp
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
,   COUNT(yardoutattr) AS yardoutcontainercount
,   COUNT(containertransittime) AS containertransittimecontainercount
,   COUNT(case when containertransittime > 0 THEN containertransittime ELSE NULL END) AS positivecontainertransittimecontainercount
,   COUNT(case when containertransittime < 0 THEN containertransittime ELSE NULL END) AS negativecontainertransittimecontainercount
,   COUNT(case when containertransittime > 0 AND containertransittime <= 5 THEN containertransittime ELSE NULL END) AS containertransittime0lessthanorequalto5dayscontainercount
,   COUNT(case when containertransittime > 5 AND containertransittime <= 10 THEN containertransittime ELSE NULL END) AS containertransittime5lessthanorequalto10dayscontainercount
,   COUNT(case when containertransittime > 10 AND containertransittime <= 15 THEN containertransittime ELSE NULL END) AS containertransittime10lessthanorequalto15dayscontainercount
,   COUNT(case when containertransittime > 15 AND containertransittime <= 20 THEN containertransittime ELSE NULL END) AS containertransittime15lessthanorequalto20dayscontainercount
,   COUNT(case when containertransittime > 20 AND containertransittime <= 25 THEN containertransittime ELSE NULL END) AS containertransittime20lessthanorequalto25dayscontainercount
,   COUNT(case when containertransittime > 25 AND containertransittime <= 30 THEN containertransittime ELSE NULL END) AS containertransittime25lessthanorequalto30dayscontainercount
,   COUNT(case when containertransittime > 30 AND containertransittime <= 35 THEN containertransittime ELSE NULL END) AS containertransittime30lessthanorequalto35dayscontainercount
,   COUNT(case when containertransittime > 35 AND containertransittime <= 40 THEN containertransittime ELSE NULL END) AS containertransittime35lessthanorequalto40dayscontainercount
,   COUNT(case when containertransittime > 40 THEN containertransittime ELSE NULL END) AS containertransittimegreaterthan40dayscontainercount
,   SUM(containertransittime) AS containertransittimedayssum
,   SUM(CASE WHEN containertransittime > 0 THEN containertransittime ELSE NULL END) AS positivecontainertransittimedayssum
,   SUM(CASE WHEN containertransittime < 0 THEN containertransittime ELSE NULL END) AS negativecontainertransittimedayssum
,   MIN(CASE WHEN containertransittime > 0 THEN containertransittime ELSE NULL END) AS minpositivecontainertransittimedayssum
,   MAX(CASE WHEN containertransittime > 0 THEN containertransittime ELSE NULL END) AS maxpositivecontainertransittimedayssum
,   COUNT(dwellatpod) AS dwelltimeatpodcontainercount
,   COUNT(case when dwellatpod > 0 THEN dwellatpod ELSE NULL END) AS positivedwelltimeatpodcontainercount
,   COUNT(case when dwellatpod < 0 THEN dwellatpod ELSE NULL END) AS negativedwelltimeatpodcontainercount
,   COUNT(case when dwellatpod > 0 AND dwellatpod <= 1 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod0lessthanorequalto1dayscontainercount
,   COUNT(case when dwellatpod > 1 AND dwellatpod <= 2 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod1lessthanorequalto2dayscontainercount
,   COUNT(case when dwellatpod > 2 AND dwellatpod <= 3 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod2lessthanorequalto3dayscontainercount
,   COUNT(case when dwellatpod > 3 AND dwellatpod <= 4 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod3lessthanorequalto4dayscontainercount
,   COUNT(case when dwellatpod > 4 AND dwellatpod <= 5 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod4lessthanorequalto5dayscontainercount
,   COUNT(case when dwellatpod > 5 AND dwellatpod <= 6 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod5lessthanorequalto6dayscontainercount
,   COUNT(case when dwellatpod > 6 AND dwellatpod <= 7 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod6lessthanorequalto7dayscontainercount
,   COUNT(case when dwellatpod > 7 AND dwellatpod <= 8 THEN dwellatpod ELSE NULL END) AS dwelltimeatpod7lessthanorequalto8dayscontainercount
,   COUNT(case when dwellatpod > 8 THEN dwellatpod ELSE NULL END) AS dwelltimeatpodgreaterthan8dayscontainercount
,   SUM(dwellatpod) AS dwelltimeatpoddayssum
,   SUM(CASE WHEN dwellatpod > 0 THEN dwellatpod ELSE NULL END) AS positivedwelltimeatpoddayssum
,   SUM(CASE WHEN dwellatpod < 0 THEN dwellatpod ELSE NULL END) AS negativedwelltimeatpoddayssum
,   MIN(CASE WHEN dwellatpod > 0 THEN dwellatpod ELSE NULL END) AS minpositivedwelltimeatpoddayssum
,   MAX(CASE WHEN dwellatpod > 0 THEN dwellatpod ELSE NULL END) AS maxpositivedwelltimeatpoddayssum
,   COUNT(totalpoddwell) AS totaltimeatpodcontainercount
,   COUNT(case when totalpoddwell > 0 THEN totalpoddwell ELSE NULL END) AS positivetotaltimeatpodcontainercount
,   COUNT(case when totalpoddwell < 0 THEN totalpoddwell ELSE NULL END) AS negativetotaltimeatpodcontainercount
,   COUNT(case when totalpoddwell > 0 AND totalpoddwell <= 1 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod0lessthanorequalto1dayscontainercount
,   COUNT(case when totalpoddwell > 1 AND totalpoddwell <= 2 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod1lessthanorequalto2dayscontainercount
,   COUNT(case when totalpoddwell > 2 AND totalpoddwell <= 3 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod2lessthanorequalto3dayscontainercount
,   COUNT(case when totalpoddwell > 3 AND totalpoddwell <= 4 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod3lessthanorequalto4dayscontainercount
,   COUNT(case when totalpoddwell > 4 AND totalpoddwell <= 5 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod4lessthanorequalto5dayscontainercount
,   COUNT(case when totalpoddwell > 5 AND totalpoddwell <= 6 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod5lessthanorequalto6dayscontainercount
,   COUNT(case when totalpoddwell > 6 AND totalpoddwell <= 7 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod6lessthanorequalto7dayscontainercount
,   COUNT(case when totalpoddwell > 7 AND totalpoddwell <= 8 THEN totalpoddwell ELSE NULL END) AS totaltimeatpod7lessthanorequalto8dayscontainercount
,   COUNT(case when totalpoddwell > 8 THEN totalpoddwell ELSE NULL END) AS totaltimeatpodgreaterthan8dayscontainercount
,   SUM(totalpoddwell) AS totaltimeatpoddayssum
,   SUM(CASE WHEN totalpoddwell > 0 THEN totalpoddwell ELSE NULL END) AS positivetotaltimeatpoddayssum
,   SUM(CASE WHEN totalpoddwell < 0 THEN totalpoddwell ELSE NULL END) AS negativetotaltimeatpoddayssum
,   MIN(CASE WHEN totalpoddwell > 0 THEN totalpoddwell ELSE NULL END) AS minpositivetotaltimeatpoddayssum
,   MAX(CASE WHEN totalpoddwell > 0 THEN totalpoddwell ELSE NULL END) AS maxpositivetotaltimeatpoddayssum
FROM hv_orc_tt_vmcontainermove_owner_base AS a
    INNER JOIN dimension_common.hv_ext_tt_dw_date_dim AS b
    ON b.the_date = to_date(a.yardoutattr)
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

DROP TABLE IF EXISTS hv_orc_yardout_owner_agg;

ALTER TABLE hv_orc_yardout_owner_agg_temp RENAME TO hv_orc_yardout_owner_agg;