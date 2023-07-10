SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_emptycontainerreturn_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_emptycontainerreturn_owner_agg_temp
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
,   COUNT(emptycontainerreturntime) AS emptyreturncontainercount
,   COUNT(dwellawayfrompod) AS dwelltimeawayfrompodcontainercount
,   COUNT(case when dwellawayfrompod > 0 THEN dwellawayfrompod ELSE NULL END) AS positivedwelltimeawayfrompodcontainercount
,   COUNT(case when dwellawayfrompod < 0 THEN dwellawayfrompod ELSE NULL END) AS negativedwelltimeawayfrompodcontainercount
,   COUNT(case when dwellawayfrompod > 0 AND dwellawayfrompod <= 1 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod0lessthanorequalto1dayscontainercount
,   COUNT(case when dwellawayfrompod > 1 AND dwellawayfrompod <= 2 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod1lessthanorequalto2dayscontainercount
,   COUNT(case when dwellawayfrompod > 2 AND dwellawayfrompod <= 3 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod2lessthanorequalto3dayscontainercount
,   COUNT(case when dwellawayfrompod > 3 AND dwellawayfrompod <= 4 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod3lessthanorequalto4dayscontainercount
,   COUNT(case when dwellawayfrompod > 4 AND dwellawayfrompod <= 5 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod4lessthanorequalto5dayscontainercount
,   COUNT(case when dwellawayfrompod > 5 AND dwellawayfrompod <= 6 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod5lessthanorequalto6dayscontainercount
,   COUNT(case when dwellawayfrompod > 6 AND dwellawayfrompod <= 7 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod6lessthanorequalto7dayscontainercount
,   COUNT(case when dwellawayfrompod > 7 AND dwellawayfrompod <= 8 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompod7lessthanorequalto8dayscontainercount
,   COUNT(case when dwellawayfrompod > 8 THEN dwellawayfrompod ELSE NULL END) AS dwelltimeawayfrompodgreaterthan8dayscontainercount
,   SUM(dwellawayfrompod) AS dwelltimeawayfrompoddayssum
,   SUM(CASE WHEN dwellawayfrompod > 0 THEN dwellawayfrompod ELSE NULL END) AS positivedwelltimeawayfrompoddayssum
,   SUM(CASE WHEN dwellawayfrompod < 0 THEN dwellawayfrompod ELSE NULL END) AS negativedwelltimeawayfrompoddayssum
,   MIN(CASE WHEN dwellawayfrompod > 0 THEN dwellawayfrompod ELSE NULL END) AS minpositivedwelltimeawayfrompoddayssum
,   MAX(CASE WHEN dwellawayfrompod > 0 THEN dwellawayfrompod ELSE NULL END) AS maxpositivedwelltimeawayfrompoddayssum
,   COUNT(outforstripping) AS outforstrippingcontainercount
,   COUNT(case when outforstripping > 0 THEN outforstripping ELSE NULL END) AS positiveoutforstrippingcontainercount
,   COUNT(case when outforstripping < 0 THEN outforstripping ELSE NULL END) AS negativeoutforstrippingcontainercount
,   COUNT(case when outforstripping > 0 AND outforstripping <= 1 THEN outforstripping ELSE NULL END) AS outforstripping0lessthanorequalto1dayscontainercount
,   COUNT(case when outforstripping > 1 AND outforstripping <= 2 THEN outforstripping ELSE NULL END) AS outforstripping1lessthanorequalto2dayscontainercount
,   COUNT(case when outforstripping > 2 AND outforstripping <= 3 THEN outforstripping ELSE NULL END) AS outforstripping2lessthanorequalto3dayscontainercount
,   COUNT(case when outforstripping > 3 AND outforstripping <= 4 THEN outforstripping ELSE NULL END) AS outforstripping3lessthanorequalto4dayscontainercount
,   COUNT(case when outforstripping > 4 AND outforstripping <= 5 THEN outforstripping ELSE NULL END) AS outforstripping4lessthanorequalto5dayscontainercount
,   COUNT(case when outforstripping > 5 AND outforstripping <= 6 THEN outforstripping ELSE NULL END) AS outforstripping5lessthanorequalto6dayscontainercount
,   COUNT(case when outforstripping > 6 AND outforstripping <= 7 THEN outforstripping ELSE NULL END) AS outforstripping6lessthanorequalto7dayscontainercount
,   COUNT(case when outforstripping > 7 AND outforstripping <= 8 THEN outforstripping ELSE NULL END) AS outforstripping7lessthanorequalto8dayscontainercount
,   COUNT(case when outforstripping > 8 THEN outforstripping ELSE NULL END) AS outforstrippinggreaterthan8dayscontainercount
,   SUM(outforstripping) AS outforstrippingdayssum
,   SUM(CASE WHEN outforstripping > 0 THEN outforstripping ELSE NULL END) AS positiveoutforstrippingdayssum
,   SUM(CASE WHEN outforstripping < 0 THEN outforstripping ELSE NULL END) AS negativeoutforstrippingdayssum
,   MIN(CASE WHEN outforstripping > 0 THEN outforstripping ELSE NULL END) AS minpositiveoutforstrippingdayssum
,   MAX(CASE WHEN outforstripping > 0 THEN outforstripping ELSE NULL END) AS maxpositiveoutforstrippingdayssum
FROM hv_orc_tt_vmcontainermove_owner_base AS a
    INNER JOIN dimension_common.hv_ext_tt_dw_date_dim AS b
    ON b.the_date = to_date(a.emptycontainerreturntime)
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

DROP TABLE IF EXISTS hv_orc_emptycontainerreturn_owner_agg;

ALTER TABLE hv_orc_emptycontainerreturn_owner_agg_temp RENAME TO hv_orc_emptycontainerreturn_owner_agg;