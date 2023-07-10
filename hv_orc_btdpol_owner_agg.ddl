SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_btdpol_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_btdpol_owner_agg_temp
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
,   COUNT(btdpol) AS btdpolcontainercount
,   COUNT(poltimeliness) AS pollatenesscontainercount
,   COUNT(case when poltimeliness >= -1 AND poltimeliness <= 1 THEN poltimeliness ELSE NULL END) AS otdperformancecontainercount
,   COUNT(case when poltimeliness > 0 THEN poltimeliness ELSE NULL END) AS positivepollatenesscontainercount
,   COUNT(case when poltimeliness < 0 THEN poltimeliness ELSE NULL END) AS negativepollatenesscontainercount
,   COUNT(case when poltimeliness < -8 THEN poltimeliness ELSE NULL END) AS otdperformancegreaterthan8daysearly
,   COUNT(case when poltimeliness >= -8 AND poltimeliness < -7 THEN poltimeliness ELSE NULL END) AS otdperformance7lessthanorequalto8daysearly
,   COUNT(case when poltimeliness >= -7 AND poltimeliness < -6 THEN poltimeliness ELSE NULL END) AS otdperformance6lessthanorequalto7daysearly
,   COUNT(case when poltimeliness >= -6 AND poltimeliness < -5 THEN poltimeliness ELSE NULL END) AS otdperformance5lessthanorequalto6daysearly
,   COUNT(case when poltimeliness >= -5 AND poltimeliness < -4 THEN poltimeliness ELSE NULL END) AS otdperformance4lessthanorequalto5daysearly
,   COUNT(case when poltimeliness >= -4 AND poltimeliness < -3 THEN poltimeliness ELSE NULL END) AS otdperformance3lessthanorequalto4daysearly
,   COUNT(case when poltimeliness >= -3 AND poltimeliness < -2 THEN poltimeliness ELSE NULL END) AS otdperformance2lessthanorequalto3daysearly
,   COUNT(case when poltimeliness >= -2 AND poltimeliness < -1 THEN poltimeliness ELSE NULL END) AS otdperformance1lessthanorequalto2daysearly
,   COUNT(case when poltimeliness >= -1 AND poltimeliness < 0 THEN poltimeliness ELSE NULL END) AS otdperformance0lessthanorequalto1daysearly
,   COUNT(case when poltimeliness > 0 AND poltimeliness <= 1 THEN poltimeliness ELSE NULL END) AS otdperformance0lessthanorequalto1dayslate
,   COUNT(case when poltimeliness > 1 AND poltimeliness <= 2 THEN poltimeliness ELSE NULL END) AS otdperformance1lessthanorequalto2dayslate
,   COUNT(case when poltimeliness > 2 AND poltimeliness <= 3 THEN poltimeliness ELSE NULL END) AS otdperformance2lessthanorequalto3dayslate
,   COUNT(case when poltimeliness > 3 AND poltimeliness <= 4 THEN poltimeliness ELSE NULL END) AS otdperformance3lessthanorequalto4dayslate
,   COUNT(case when poltimeliness > 4 AND poltimeliness <= 5 THEN poltimeliness ELSE NULL END) AS otdperformance4lessthanorequalto5dayslate
,   COUNT(case when poltimeliness > 5 THEN poltimeliness ELSE NULL END) AS otdperformancegreaterthan5dayslate
,   COUNT(case when poltimeliness > 5 AND poltimeliness <= 6 THEN poltimeliness ELSE NULL END) AS otdperformance5lessthanorequalto6dayslate
,   COUNT(case when poltimeliness > 6 AND poltimeliness <= 7 THEN poltimeliness ELSE NULL END) AS otdperformance6lessthanorequalto7dayslate
,   COUNT(case when poltimeliness > 7 AND poltimeliness <= 8 THEN poltimeliness ELSE NULL END) AS otdperformance7lessthanorequalto8dayslate
,   COUNT(case when poltimeliness > 8 THEN poltimeliness ELSE NULL END) AS otdperformancegreaterthan8dayslate
,   SUM(poltimeliness) AS pollatenessdayssum
,   SUM(CASE WHEN poltimeliness > 0 THEN poltimeliness ELSE NULL END) AS positivepollatenessdayssum
,   SUM(CASE WHEN poltimeliness < 0 THEN poltimeliness ELSE NULL END) AS negativepollatenessdayssum
,   COUNT(arrivetransshiptime) AS arrivedattransshipmentcompletenesscontainercount
,   COUNT(booketdpol) AS bookedetdpolcompletenesscontainercount
,   COUNT(departpoltime) AS departedfrompolcompletenesscontainercount
,   COUNT(departtransshiptime) AS departedfromtransshipmentcompletenesscontainercount
,   COUNT(emptyoutgateattr) AS emptyoutgatecompletenesscontainercount
,   COUNT(emptyoutgateinlandtime) AS emptyoutfrominlandcompletenesscontainercount
,   COUNT(emptyoutgateoceantime) AS emptyoutfromportcompletenesscontainercount
,   COUNT(firstetd) AS etdpolcompletenesscontainercount
,   COUNT(fulloutgateoceantime) AS fulloutgatefrompolcompletenesscontainercount
,   COUNT(onboardpoltime) AS onboardcompletenesscontainercount
,   COUNT(yardinattr) AS yardincompletenesscontainercount
,   SUM(CASE WHEN distance_km IS NOT NULL AND oceantransittime IS NOT NULL THEN distance_km ELSE NULL END)/SUM(CASE WHEN distance_km IS NOT NULL AND oceantransittime IS NOT NULL THEN oceantransittime ELSE NULL END)*24 AS speedkmshr
,   SUM(teu) AS teusum
,   SUM(polteu) AS teuatpolsum
,   SUM(containertransittime) AS containertransittimesum
,   COUNT(containertransittime) AS containertransittimecount
,   SUM(oceantransittime) AS vesseltransittimesum
,   COUNT(oceantransittime) AS vesseltransittimecount
,   SUM(CASE WHEN poltimeliness >= -1 AND poltimeliness <= 1 THEN poltimeliness ELSE NULL END) AS otdperformancesum
,   COUNT(CASE WHEN poltimeliness >= -1 AND poltimeliness <= 1 THEN poltimeliness ELSE NULL END) AS otdperformancecount
,   SUM(CASE WHEN podtimeliness >= -1 AND podtimeliness <= 1 THEN podtimeliness ELSE NULL END) AS otaperformancesum
,   COUNT(CASE WHEN podtimeliness >= -1 AND podtimeliness <= 1 THEN podtimeliness ELSE NULL END) AS otaperformancecount
,   SUM(CASE WHEN poltimeliness > 0 THEN poltimeliness ELSE NULL END) AS pollatenesssum
,   COUNT(CASE WHEN poltimeliness > 0 THEN poltimeliness ELSE NULL END) AS pollatenesscount
,   SUM(case when podtimeliness > 0 THEN podtimeliness ELSE NULL END) AS podlatenesssum
,   COUNT(case when podtimeliness > 0 THEN podtimeliness ELSE NULL END) AS podlatenesscount
,   SUM(CASE WHEN distance_km IS NOT NULL AND oceantransittime IS NOT NULL THEN distance_km ELSE NULL END) AS speeddistancekms
,   SUM(CASE WHEN distance_km IS NOT NULL AND oceantransittime IS NOT NULL THEN oceantransittime ELSE NULL END)*24 AS speedtimehr
,   SUM(totalpoldwell) AS totaltimeatpolsum
,   COUNT(totalpoldwell) AS totaltimeatpolcount
,   SUM(totalpoddwell) AS totaltimeatpodsum
,   COUNT(totalpoddwell) AS totaltimeatpodcount
,   SUM(teu) AS teuranksum
,   COUNT(teu) AS teurankcount
,   SUM(polteu) AS teuatpolranksum
,   COUNT(polteu) AS teuatpolrankcount
,   SUM(podteu) AS teuatpodranksum
,   COUNT(podteu) AS teuatpodrankcount
FROM hv_orc_tt_vmcontainermove_owner_base AS a
    INNER JOIN dimension_common.hv_ext_tt_dw_date_dim AS b
    ON b.the_date = to_date(a.btdpol)
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

DROP TABLE IF EXISTS hv_orc_btdpol_owner_agg;

ALTER TABLE hv_orc_btdpol_owner_agg_temp RENAME TO hv_orc_btdpol_owner_agg;