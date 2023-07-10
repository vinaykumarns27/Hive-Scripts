SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_arrivepod_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_arrivepod_owner_agg_temp
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
,   COUNT(arrivepodtime) AS arrivedatpodcontainercount
,   COUNT(oceantransittime) AS vesseltransittimecontainercount
,   COUNT(case when oceantransittime >= -1 AND oceantransittime <= 1 THEN oceantransittime ELSE NULL END) AS vesseltransittimeperformancecontainercount
,   COUNT(case when oceantransittime > 0 THEN oceantransittime ELSE NULL END) AS positivevesseltransittimecontainercount
,   COUNT(case when oceantransittime < 0 THEN oceantransittime ELSE NULL END) AS negativevesseltransittimecontainercount
,   COUNT(case when oceantransittime > 0 AND oceantransittime <= 5 THEN oceantransittime ELSE NULL END) AS vesseltransittime0lessthanorequalto5dayscontainercount
,   COUNT(case when oceantransittime > 5 AND oceantransittime <= 10 THEN oceantransittime ELSE NULL END) AS vesseltransittime5lessthanorequalto10dayscontainercount
,   COUNT(case when oceantransittime > 10 AND oceantransittime <= 15 THEN oceantransittime ELSE NULL END) AS vesseltransittime10lessthanorequalto15dayscontainercount
,   COUNT(case when oceantransittime > 15 AND oceantransittime <= 20 THEN oceantransittime ELSE NULL END) AS vesseltransittime15lessthanorequalto20dayscontainercount
,   COUNT(case when oceantransittime > 20 AND oceantransittime <= 25 THEN oceantransittime ELSE NULL END) AS vesseltransittime20lessthanorequalto25dayscontainercount
,   COUNT(case when oceantransittime > 25 AND oceantransittime <= 30 THEN oceantransittime ELSE NULL END) AS vesseltransittime25lessthanorequalto30dayscontainercount
,   COUNT(case when oceantransittime > 30 AND oceantransittime <= 35 THEN oceantransittime ELSE NULL END) AS vesseltransittime30lessthanorequalto35dayscontainercount
,   COUNT(case when oceantransittime > 35 AND oceantransittime <= 40 THEN oceantransittime ELSE NULL END) AS vesseltransittime35lessthanorequalto40dayscontainercount
,   COUNT(case when oceantransittime > 40 THEN oceantransittime ELSE NULL END) AS vesseltransittimegreaterthan40dayscontainercount
,   SUM(CASE WHEN oceantransittime > 0 THEN oceantransittime ELSE NULL END) AS positivevesseltransittimedayssum
,   SUM(CASE WHEN oceantransittime < 0 THEN oceantransittime ELSE NULL END) AS negativevesseltransittimedayssum
,   MIN(CASE WHEN oceantransittime > 0 THEN oceantransittime ELSE NULL END) AS minpositivevesseltransittimedayssum
,   MAX(CASE WHEN oceantransittime > 0 THEN oceantransittime ELSE NULL END) AS maxpositivevesseltransittimedayssum
,   COUNT(case when containervesselestimatediff < -10 THEN containervesselestimatediff ELSE NULL END) AS vesselperformancegreaterthan10daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -10 AND containervesselestimatediff <= -9 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance9lessthanorequalto10daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -9 AND containervesselestimatediff <= -8 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance8lessthanorequalto9daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -8 AND containervesselestimatediff <= -7 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance7lessthanorequalto8daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -7 AND containervesselestimatediff <= -6 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance6lessthanorequalto7daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -6 AND containervesselestimatediff <= -5 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance5lessthanorequalto6daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -5 AND containervesselestimatediff <= -4 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance4lessthanorequalto5daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -4 AND containervesselestimatediff <= -3 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance3lessthanorequalto4daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -3 AND containervesselestimatediff <= -2 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance2lessthanorequalto3daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -2 AND containervesselestimatediff <= -1 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance1lessthanorequalto2daysearlycontainercount
,   COUNT(case when containervesselestimatediff > -1 AND containervesselestimatediff <= 0 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance0lessthanorequalto1daysearlycontainercount
,   COUNT(case when containervesselestimatediff > 0 AND containervesselestimatediff <= 1 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance0lessthanorequalto1dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 1 AND containervesselestimatediff <= 2 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance1lessthanorequalto2dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 2 AND containervesselestimatediff <= 3 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance2lessthanorequalto3dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 3 AND containervesselestimatediff <= 4 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance3lessthanorequalto4dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 4 AND containervesselestimatediff <= 5 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance4lessthanorequalto5dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 5 AND containervesselestimatediff <= 6 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance5lessthanorequalto6dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 6 AND containervesselestimatediff <= 7 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance6lessthanorequalto7dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 7 AND containervesselestimatediff <= 8 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance7lessthanorequalto8dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 8 AND containervesselestimatediff <= 9 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance8lessthanorequalto9dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 9 AND containervesselestimatediff <= 10 THEN containervesselestimatediff ELSE NULL END) AS vesselperformance9lessthanorequalto10dayslatecontainercount
,   COUNT(case when containervesselestimatediff > 10 THEN containervesselestimatediff ELSE NULL END) AS vesselperformancegreaterthan10dayslatecontainercount
FROM hv_orc_tt_vmcontainermove_owner_base AS a
    INNER JOIN dimension_common.hv_ext_tt_dw_date_dim AS b
    ON b.the_date = to_date(a.arrivepodtime)
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

DROP TABLE IF EXISTS hv_orc_arrivepod_owner_agg;

ALTER TABLE hv_orc_arrivepod_owner_agg_temp RENAME TO hv_orc_arrivepod_owner_agg;