SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_ocm_benchmark_arrivepod_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_arrivetransship_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_booketapod_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_booketdpol_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_btapod_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_btdpol_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_create_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_customsreleased_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_departpol_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_departtransship_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_emptycontainerreturn_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_fullcontainerdelivery_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_fullcontainerdischarge_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_fulloutgateocean_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_onboard_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_outforstuffing_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_yardin_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_yardout_no_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_outforstuffing_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   b.outforstuffingcontainercount
,   b.positiveoutforstuffingcontainercount
,   b.negativeoutforstuffingcontainercount
,   b.outforstuffing0lessthanorequalto1dayscontainercount
,   b.outforstuffing1lessthanorequalto2dayscontainercount
,   b.outforstuffing2lessthanorequalto3dayscontainercount
,   b.outforstuffing3lessthanorequalto4dayscontainercount
,   b.outforstuffing4lessthanorequalto5dayscontainercount
,   b.outforstuffing5lessthanorequalto6dayscontainercount
,   b.outforstuffing6lessthanorequalto7dayscontainercount
,   b.outforstuffing7lessthanorequalto8dayscontainercount
,   b.outforstuffinggreaterthan8dayscontainercount
,   b.outforstuffingdayssum
,   b.positiveoutforstuffingdayssum
,   b.negativeoutforstuffingdayssum
,   b.minpositiveoutforstuffingdays
,   b.maxpositiveoutforstuffingdays
FROM hv_orc_emptyoutgate_no_owner_agg AS a
    FULL OUTER JOIN hv_orc_outforstuffing_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_yardin_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   b.yardincontainercount
,   b.dwelltimeawayfrompolcontainercount
,   b.positivedwelltimeawayfrompolcontainercount
,   b.negativedwelltimeawayfrompolcontainercount
,   b.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   b.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   b.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   b.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   b.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   b.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   b.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   b.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   b.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   b.dwelltimeawayfrompoldayssum
,   b.positivedwelltimeawayfrompoldayssum
,   b.negativedwelltimeawayfrompoldayssum
,   b.minpositivedwelltimeawayfrompoldays
,   b.maxpositivedwelltimeawayfrompoldays
,   b.origininlandtransitcontainercount
,   b.positiveorigininlandtransitcontainercount
,   b.negativeorigininlandtransitcontainercount
,   b.origininlandtransit0lessthanorequalto1dayscontainercount
,   b.origininlandtransit1lessthanorequalto2dayscontainercount
,   b.origininlandtransit2lessthanorequalto3dayscontainercount
,   b.origininlandtransit3lessthanorequalto4dayscontainercount
,   b.origininlandtransit4lessthanorequalto5dayscontainercount
,   b.origininlandtransitgreaterthan5dayscontainercount
,   b.origininlandtransitdayssum
,   b.positiveorigininlandtransitdayssum
,   b.negativeorigininlandtransitdayssum
,   b.minpositiveorigininlandtransitdays
,   b.maxpositiveorigininlandtransitdays
FROM hv_orc_ocm_benchmark_outforstuffing_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_yardin_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_onboard_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   b.onboardcontainercount
,   b.dwelltimeatpolcontainercount
,   b.positivedwelltimeatpolcontainercount
,   b.negativedwelltimeatpolcontainercount
,   b.dwelltimeatpoldayssum
,   b.positivedwelltimeatpoldayssum
,   b.negativedwelltimeatpoldayssum
,   b.minpositivedwelltimeatpoldays
,   b.maxpositivedwelltimeatpoldays
FROM hv_orc_ocm_benchmark_yardin_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_onboard_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_booketdpol_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   b.bookedetdpolcontainercount
FROM hv_orc_ocm_benchmark_onboard_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_booketdpol_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_btdpol_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   b.btdpolcontainercount
,   b.pollatenesscontainercount
,   b.otdperformancecontainercount
,   b.positivepollatenesscontainercount
,   b.negativepollatenesscontainercount
,   b.otdperformancegreaterthan8daysearly
,   b.otdperformance7lessthanorequalto8daysearly
,   b.otdperformance6lessthanorequalto7daysearly
,   b.otdperformance5lessthanorequalto6daysearly
,   b.otdperformance4lessthanorequalto5daysearly
,   b.otdperformance3lessthanorequalto4daysearly
,   b.otdperformance2lessthanorequalto3daysearly
,   b.otdperformance1lessthanorequalto2daysearly
,   b.otdperformance0lessthanorequalto1daysearly
,   b.otdperformance0lessthanorequalto1dayslate
,   b.otdperformance1lessthanorequalto2dayslate
,   b.otdperformance2lessthanorequalto3dayslate
,   b.otdperformance3lessthanorequalto4dayslate
,   b.otdperformance4lessthanorequalto5dayslate
,   b.otdperformancegreaterthan5dayslate
,   b.otdperformance5lessthanorequalto6dayslate
,   b.otdperformance6lessthanorequalto7dayslate
,   b.otdperformance7lessthanorequalto8dayslate
,   b.otdperformancegreaterthan8dayslate
,   b.pollatenessdayssum
,   b.positivepollatenessdayssum
,   b.negativepollatenessdayssum
,   b.arrivedattransshipmentcompletenesscontainercount
,   b.bookedetdpolcompletenesscontainercount
,   b.departedfrompolcompletenesscontainercount
,   b.departedfromtransshipmentcompletenesscontainercount
,   b.emptyoutgatecompletenesscontainercount
,   b.emptyoutfrominlandcompletenesscontainercount
,   b.emptyoutfromportcompletenesscontainercount
,   b.etdpolcompletenesscontainercount
,   b.fulloutgatefrompolcompletenesscontainercount
,   b.onboardcompletenesscontainercount
,   b.yardincompletenesscontainercount
,   b.speedkmshr
,   b.teusum
,   b.teuatpolsum
,   b.containertransittimesum
,   b.containertransittimecount
,   b.vesseltransittimesum
,   b.vesseltransittimecount
,   b.otdperformancesum
,   b.otdperformancecount
,   b.otaperformancesum
,   b.otaperformancecount
,   b.pollatenesssum
,   b.pollatenesscount
,   b.podlatenesssum
,   b.podlatenesscount
,   b.speeddistancekms
,   b.speedtimehr
,   b.totaltimeatpolsum
,   b.totaltimeatpolcount
,   b.totaltimeatpodsum
,   b.totaltimeatpodcount
,   b.teuranksum
,   b.teurankcount
,   b.teuatpolranksum
,   b.teuatpolrankcount
,   b.teuatpodranksum
,   b.teuatpodrankcount
FROM hv_orc_ocm_benchmark_booketdpol_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_btdpol_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_departpol_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   b.departedfrompolcontainercount
,   b.totaltimeatpolcontainercount
,   b.positivetotaltimeatpolcontainercount
,   b.negativetotaltimeatpolcontainercount
,   b.totaltimeatpol0lessthanorequalto1dayscontainercount
,   b.totaltimeatpol1lessthanorequalto2dayscontainercount
,   b.totaltimeatpol2lessthanorequalto3dayscontainercount
,   b.totaltimeatpol3lessthanorequalto4dayscontainercount
,   b.totaltimeatpol4lessthanorequalto5dayscontainercount
,   b.totaltimeatpol5lessthanorequalto6dayscontainercount
,   b.totaltimeatpol6lessthanorequalto7dayscontainercount
,   b.totaltimeatpol7lessthanorequalto8dayscontainercount
,   b.totaltimeatpolgreaterthan8dayscontainercount
,   b.totaltimeatpoldayssum
,   b.positivetotaltimeatpoldayssum
,   b.negativetotaltimeatpoldayssum
,   b.minpositivetotaltimeatpoldayssum
,   b.maxpositivetotaltimeatpoldayssum
FROM hv_orc_ocm_benchmark_btdpol_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_departpol_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_arrivetransship_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   b.arrivedattransshipmentcontainercount
FROM hv_orc_ocm_benchmark_departpol_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_arrivetransship_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_departtransship_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   b.departedfromtransshipmentcontainercount
,   b.dwelltimeattransshipmentportcontainercount
,   b.positivedwelltimeattransshipmentportcontainercount
,   b.negativedwelltimeattransshipmentportcontainercount
,   b.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   b.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   b.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   b.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   b.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   b.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   b.dwelltimeattransshipmentportdayssum
,   b.positivedwelltimeattransshipmentportdayssum
,   b.negativedwelltimeattransshipmentportdayssum
,   b.minpositivedwelltimeattransshipmentportdayssum
,   b.maxpositivedwelltimeattransshipmentportdayssum
FROM hv_orc_ocm_benchmark_arrivetransship_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_departtransship_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_btapod_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   b.btapodcontainercount
,   b.podlatenesscontainercount
,   b.otaperformancecontainercount
,   b.positivepodlatenesscontainercount
,   b.negativepodlatenesscontainercount
,   b.otaperformancegreaterthan8daysearly
,   b.otaperformance7lessthanorequalto8daysearly
,   b.otaperformance6lessthanorequalto7daysearly
,   b.otaperformance5lessthanorequalto6daysearly
,   b.otaperformance4lessthanorequalto5daysearly
,   b.otaperformance3lessthanorequalto4daysearly
,   b.otaperformance2lessthanorequalto3daysearly
,   b.otaperformance1lessthanorequalto2daysearly
,   b.otaperformance0lessthanorequalto1daysearly
,   b.otaperformance0lessthanorequalto1dayslate
,   b.otaperformance1lessthanorequalto2dayslate
,   b.otaperformance2lessthanorequalto3dayslate
,   b.otaperformance3lessthanorequalto4dayslate
,   b.otaperformance4lessthanorequalto5dayslate
,   b.otaperformancegreaterthan5dayslate
,   b.otaperformance5lessthanorequalto6dayslate
,   b.otaperformance6lessthanorequalto7dayslate
,   b.otaperformance7lessthanorequalto8dayslate
,   b.otaperformancegreaterthan8dayslate
,   b.podlatenessdayssum
,   b.positivepodlatenessdayssum
,   b.negativepodlatenessdayssum
,   b.arrivedatpodcompletenesscontainercount
,   b.bookedetapodcompletenesscontainercount
,   b.customsreleasecompletenesscontainercount
,   b.emptyreturncompletenesscontainercount
,   b.etapodcompletenesscontainercount
,   b.fullcontainerdischargedatpodcompletenesscontainercount
,   b.yardoutcompletenesscontainercount
,   b.teuatpodsum
FROM hv_orc_ocm_benchmark_departtransship_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_btapod_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_booketapod_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   b.bookedetapodcontainercount
FROM hv_orc_ocm_benchmark_btapod_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_booketapod_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_arrivepod_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   b.arrivedatpodcontainercount
,   b.vesseltransittimecontainercount
,   b.vesseltransittimeperformancecontainercount
,   b.positivevesseltransittimecontainercount
,   b.negativevesseltransittimecontainercount
,   b.vesseltransittime0lessthanorequalto5dayscontainercount
,   b.vesseltransittime5lessthanorequalto10dayscontainercount
,   b.vesseltransittime10lessthanorequalto15dayscontainercount
,   b.vesseltransittime15lessthanorequalto20dayscontainercount
,   b.vesseltransittime20lessthanorequalto25dayscontainercount
,   b.vesseltransittime25lessthanorequalto30dayscontainercount
,   b.vesseltransittime30lessthanorequalto35dayscontainercount
,   b.vesseltransittime35lessthanorequalto40dayscontainercount
,   b.vesseltransittimegreaterthan40dayscontainercount
,   b.positivevesseltransittimedayssum
,   b.negativevesseltransittimedayssum
,   b.minpositivevesseltransittimedayssum
,   b.maxpositivevesseltransittimedayssum
,   b.vesselperformancegreaterthan10daysearlycontainercount
,   b.vesselperformance9lessthanorequalto10daysearlycontainercount
,   b.vesselperformance8lessthanorequalto9daysearlycontainercount
,   b.vesselperformance7lessthanorequalto8daysearlycontainercount
,   b.vesselperformance6lessthanorequalto7daysearlycontainercount
,   b.vesselperformance5lessthanorequalto6daysearlycontainercount
,   b.vesselperformance4lessthanorequalto5daysearlycontainercount
,   b.vesselperformance3lessthanorequalto4daysearlycontainercount
,   b.vesselperformance2lessthanorequalto3daysearlycontainercount
,   b.vesselperformance1lessthanorequalto2daysearlycontainercount
,   b.vesselperformance0lessthanorequalto1daysearlycontainercount
,   b.vesselperformance0lessthanorequalto1dayslatecontainercount
,   b.vesselperformance1lessthanorequalto2dayslatecontainercount
,   b.vesselperformance2lessthanorequalto3dayslatecontainercount
,   b.vesselperformance3lessthanorequalto4dayslatecontainercount
,   b.vesselperformance4lessthanorequalto5dayslatecontainercount
,   b.vesselperformance5lessthanorequalto6dayslatecontainercount
,   b.vesselperformance6lessthanorequalto7dayslatecontainercount
,   b.vesselperformance7lessthanorequalto8dayslatecontainercount
,   b.vesselperformance8lessthanorequalto9dayslatecontainercount
,   b.vesselperformance9lessthanorequalto10dayslatecontainercount
,   b.vesselperformancegreaterthan10dayslatecontainercount
FROM hv_orc_ocm_benchmark_booketapod_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_arrivepod_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_customsreleased_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   b.customsreleasecontainercount
,   b.customsreleasetimecontainercount
,   b.positivecustomsreleasetimecontainercount
,   b.negativecustomsreleasetimecontainercount
,   b.customsreleasetime0lessthanorequalto1dayscontainercount
,   b.customsreleasetime1lessthanorequalto2dayscontainercount
,   b.customsreleasetime2lessthanorequalto3dayscontainercount
,   b.customsreleasetime3lessthanorequalto4dayscontainercount
,   b.customsreleasetime4lessthanorequalto5dayscontainercount
,   b.customsreleasetime5lessthanorequalto6dayscontainercount
,   b.customsreleasetime6lessthanorequalto7dayscontainercount
,   b.customsreleasetime7lessthanorequalto8dayscontainercount
,   b.customsreleasetimegreaterthan8dayscontainercount
,   b.customsreleasetimedayssum
,   b.positivecustomsreleasetimedayssum
,   b.negativecustomsreleasetimedayssum
,   b.minpositivecustomsreleasetimedayssum
,   b.maxpositivecustomsreleasetimedayssum
FROM hv_orc_ocm_benchmark_arrivepod_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_customsreleased_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_fullcontainerdischarge_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   a.customsreleasecontainercount
,   a.customsreleasetimecontainercount
,   a.positivecustomsreleasetimecontainercount
,   a.negativecustomsreleasetimecontainercount
,   a.customsreleasetime0lessthanorequalto1dayscontainercount
,   a.customsreleasetime1lessthanorequalto2dayscontainercount
,   a.customsreleasetime2lessthanorequalto3dayscontainercount
,   a.customsreleasetime3lessthanorequalto4dayscontainercount
,   a.customsreleasetime4lessthanorequalto5dayscontainercount
,   a.customsreleasetime5lessthanorequalto6dayscontainercount
,   a.customsreleasetime6lessthanorequalto7dayscontainercount
,   a.customsreleasetime7lessthanorequalto8dayscontainercount
,   a.customsreleasetimegreaterthan8dayscontainercount
,   a.customsreleasetimedayssum
,   a.positivecustomsreleasetimedayssum
,   a.negativecustomsreleasetimedayssum
,   a.minpositivecustomsreleasetimedayssum
,   a.maxpositivecustomsreleasetimedayssum
,   b.fullcontainerdischargedatpodcontainercount
FROM hv_orc_ocm_benchmark_customsreleased_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_fullcontainerdischarge_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_yardout_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   a.customsreleasecontainercount
,   a.customsreleasetimecontainercount
,   a.positivecustomsreleasetimecontainercount
,   a.negativecustomsreleasetimecontainercount
,   a.customsreleasetime0lessthanorequalto1dayscontainercount
,   a.customsreleasetime1lessthanorequalto2dayscontainercount
,   a.customsreleasetime2lessthanorequalto3dayscontainercount
,   a.customsreleasetime3lessthanorequalto4dayscontainercount
,   a.customsreleasetime4lessthanorequalto5dayscontainercount
,   a.customsreleasetime5lessthanorequalto6dayscontainercount
,   a.customsreleasetime6lessthanorequalto7dayscontainercount
,   a.customsreleasetime7lessthanorequalto8dayscontainercount
,   a.customsreleasetimegreaterthan8dayscontainercount
,   a.customsreleasetimedayssum
,   a.positivecustomsreleasetimedayssum
,   a.negativecustomsreleasetimedayssum
,   a.minpositivecustomsreleasetimedayssum
,   a.maxpositivecustomsreleasetimedayssum
,   a.fullcontainerdischargedatpodcontainercount
,   b.yardoutcontainercount
,   b.containertransittimecontainercount
,   b.positivecontainertransittimecontainercount
,   b.negativecontainertransittimecontainercount
,   b.containertransittime0lessthanorequalto5dayscontainercount
,   b.containertransittime5lessthanorequalto10dayscontainercount
,   b.containertransittime10lessthanorequalto15dayscontainercount
,   b.containertransittime15lessthanorequalto20dayscontainercount
,   b.containertransittime20lessthanorequalto25dayscontainercount
,   b.containertransittime25lessthanorequalto30dayscontainercount
,   b.containertransittime30lessthanorequalto35dayscontainercount
,   b.containertransittime35lessthanorequalto40dayscontainercount
,   b.containertransittimegreaterthan40dayscontainercount
,   b.containertransittimedayssum
,   b.positivecontainertransittimedayssum
,   b.negativecontainertransittimedayssum
,   b.minpositivecontainertransittimedayssum
,   b.maxpositivecontainertransittimedayssum
,   b.dwelltimeatpodcontainercount
,   b.positivedwelltimeatpodcontainercount
,   b.negativedwelltimeatpodcontainercount
,   b.dwelltimeatpod0lessthanorequalto1dayscontainercount
,   b.dwelltimeatpod1lessthanorequalto2dayscontainercount
,   b.dwelltimeatpod2lessthanorequalto3dayscontainercount
,   b.dwelltimeatpod3lessthanorequalto4dayscontainercount
,   b.dwelltimeatpod4lessthanorequalto5dayscontainercount
,   b.dwelltimeatpod5lessthanorequalto6dayscontainercount
,   b.dwelltimeatpod6lessthanorequalto7dayscontainercount
,   b.dwelltimeatpod7lessthanorequalto8dayscontainercount
,   b.dwelltimeatpodgreaterthan8dayscontainercount
,   b.dwelltimeatpoddayssum
,   b.positivedwelltimeatpoddayssum
,   b.negativedwelltimeatpoddayssum
,   b.minpositivedwelltimeatpoddayssum
,   b.maxpositivedwelltimeatpoddayssum
,   b.totaltimeatpodcontainercount
,   b.positivetotaltimeatpodcontainercount
,   b.negativetotaltimeatpodcontainercount
,   b.totaltimeatpod0lessthanorequalto1dayscontainercount
,   b.totaltimeatpod1lessthanorequalto2dayscontainercount
,   b.totaltimeatpod2lessthanorequalto3dayscontainercount
,   b.totaltimeatpod3lessthanorequalto4dayscontainercount
,   b.totaltimeatpod4lessthanorequalto5dayscontainercount
,   b.totaltimeatpod5lessthanorequalto6dayscontainercount
,   b.totaltimeatpod6lessthanorequalto7dayscontainercount
,   b.totaltimeatpod7lessthanorequalto8dayscontainercount
,   b.totaltimeatpodgreaterthan8dayscontainercount
,   b.totaltimeatpoddayssum
,   b.positivetotaltimeatpoddayssum
,   b.negativetotaltimeatpoddayssum
,   b.minpositivetotaltimeatpoddayssum
,   b.maxpositivetotaltimeatpoddayssum
FROM hv_orc_ocm_benchmark_fullcontainerdischarge_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_yardout_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_fulloutgateocean_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   a.customsreleasecontainercount
,   a.customsreleasetimecontainercount
,   a.positivecustomsreleasetimecontainercount
,   a.negativecustomsreleasetimecontainercount
,   a.customsreleasetime0lessthanorequalto1dayscontainercount
,   a.customsreleasetime1lessthanorequalto2dayscontainercount
,   a.customsreleasetime2lessthanorequalto3dayscontainercount
,   a.customsreleasetime3lessthanorequalto4dayscontainercount
,   a.customsreleasetime4lessthanorequalto5dayscontainercount
,   a.customsreleasetime5lessthanorequalto6dayscontainercount
,   a.customsreleasetime6lessthanorequalto7dayscontainercount
,   a.customsreleasetime7lessthanorequalto8dayscontainercount
,   a.customsreleasetimegreaterthan8dayscontainercount
,   a.customsreleasetimedayssum
,   a.positivecustomsreleasetimedayssum
,   a.negativecustomsreleasetimedayssum
,   a.minpositivecustomsreleasetimedayssum
,   a.maxpositivecustomsreleasetimedayssum
,   a.fullcontainerdischargedatpodcontainercount
,   a.yardoutcontainercount
,   a.containertransittimecontainercount
,   a.positivecontainertransittimecontainercount
,   a.negativecontainertransittimecontainercount
,   a.containertransittime0lessthanorequalto5dayscontainercount
,   a.containertransittime5lessthanorequalto10dayscontainercount
,   a.containertransittime10lessthanorequalto15dayscontainercount
,   a.containertransittime15lessthanorequalto20dayscontainercount
,   a.containertransittime20lessthanorequalto25dayscontainercount
,   a.containertransittime25lessthanorequalto30dayscontainercount
,   a.containertransittime30lessthanorequalto35dayscontainercount
,   a.containertransittime35lessthanorequalto40dayscontainercount
,   a.containertransittimegreaterthan40dayscontainercount
,   a.containertransittimedayssum
,   a.positivecontainertransittimedayssum
,   a.negativecontainertransittimedayssum
,   a.minpositivecontainertransittimedayssum
,   a.maxpositivecontainertransittimedayssum
,   a.dwelltimeatpodcontainercount
,   a.positivedwelltimeatpodcontainercount
,   a.negativedwelltimeatpodcontainercount
,   a.dwelltimeatpod0lessthanorequalto1dayscontainercount
,   a.dwelltimeatpod1lessthanorequalto2dayscontainercount
,   a.dwelltimeatpod2lessthanorequalto3dayscontainercount
,   a.dwelltimeatpod3lessthanorequalto4dayscontainercount
,   a.dwelltimeatpod4lessthanorequalto5dayscontainercount
,   a.dwelltimeatpod5lessthanorequalto6dayscontainercount
,   a.dwelltimeatpod6lessthanorequalto7dayscontainercount
,   a.dwelltimeatpod7lessthanorequalto8dayscontainercount
,   a.dwelltimeatpodgreaterthan8dayscontainercount
,   a.dwelltimeatpoddayssum
,   a.positivedwelltimeatpoddayssum
,   a.negativedwelltimeatpoddayssum
,   a.minpositivedwelltimeatpoddayssum
,   a.maxpositivedwelltimeatpoddayssum
,   a.totaltimeatpodcontainercount
,   a.positivetotaltimeatpodcontainercount
,   a.negativetotaltimeatpodcontainercount
,   a.totaltimeatpod0lessthanorequalto1dayscontainercount
,   a.totaltimeatpod1lessthanorequalto2dayscontainercount
,   a.totaltimeatpod2lessthanorequalto3dayscontainercount
,   a.totaltimeatpod3lessthanorequalto4dayscontainercount
,   a.totaltimeatpod4lessthanorequalto5dayscontainercount
,   a.totaltimeatpod5lessthanorequalto6dayscontainercount
,   a.totaltimeatpod6lessthanorequalto7dayscontainercount
,   a.totaltimeatpod7lessthanorequalto8dayscontainercount
,   a.totaltimeatpodgreaterthan8dayscontainercount
,   a.totaltimeatpoddayssum
,   a.positivetotaltimeatpoddayssum
,   a.negativetotaltimeatpoddayssum
,   a.minpositivetotaltimeatpoddayssum
,   a.maxpositivetotaltimeatpoddayssum
,   b.fulloutgatefrompolcontainercount
FROM hv_orc_ocm_benchmark_yardout_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_fulloutgateocean_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_fullcontainerdelivery_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   a.customsreleasecontainercount
,   a.customsreleasetimecontainercount
,   a.positivecustomsreleasetimecontainercount
,   a.negativecustomsreleasetimecontainercount
,   a.customsreleasetime0lessthanorequalto1dayscontainercount
,   a.customsreleasetime1lessthanorequalto2dayscontainercount
,   a.customsreleasetime2lessthanorequalto3dayscontainercount
,   a.customsreleasetime3lessthanorequalto4dayscontainercount
,   a.customsreleasetime4lessthanorequalto5dayscontainercount
,   a.customsreleasetime5lessthanorequalto6dayscontainercount
,   a.customsreleasetime6lessthanorequalto7dayscontainercount
,   a.customsreleasetime7lessthanorequalto8dayscontainercount
,   a.customsreleasetimegreaterthan8dayscontainercount
,   a.customsreleasetimedayssum
,   a.positivecustomsreleasetimedayssum
,   a.negativecustomsreleasetimedayssum
,   a.minpositivecustomsreleasetimedayssum
,   a.maxpositivecustomsreleasetimedayssum
,   a.fullcontainerdischargedatpodcontainercount
,   a.yardoutcontainercount
,   a.containertransittimecontainercount
,   a.positivecontainertransittimecontainercount
,   a.negativecontainertransittimecontainercount
,   a.containertransittime0lessthanorequalto5dayscontainercount
,   a.containertransittime5lessthanorequalto10dayscontainercount
,   a.containertransittime10lessthanorequalto15dayscontainercount
,   a.containertransittime15lessthanorequalto20dayscontainercount
,   a.containertransittime20lessthanorequalto25dayscontainercount
,   a.containertransittime25lessthanorequalto30dayscontainercount
,   a.containertransittime30lessthanorequalto35dayscontainercount
,   a.containertransittime35lessthanorequalto40dayscontainercount
,   a.containertransittimegreaterthan40dayscontainercount
,   a.containertransittimedayssum
,   a.positivecontainertransittimedayssum
,   a.negativecontainertransittimedayssum
,   a.minpositivecontainertransittimedayssum
,   a.maxpositivecontainertransittimedayssum
,   a.dwelltimeatpodcontainercount
,   a.positivedwelltimeatpodcontainercount
,   a.negativedwelltimeatpodcontainercount
,   a.dwelltimeatpod0lessthanorequalto1dayscontainercount
,   a.dwelltimeatpod1lessthanorequalto2dayscontainercount
,   a.dwelltimeatpod2lessthanorequalto3dayscontainercount
,   a.dwelltimeatpod3lessthanorequalto4dayscontainercount
,   a.dwelltimeatpod4lessthanorequalto5dayscontainercount
,   a.dwelltimeatpod5lessthanorequalto6dayscontainercount
,   a.dwelltimeatpod6lessthanorequalto7dayscontainercount
,   a.dwelltimeatpod7lessthanorequalto8dayscontainercount
,   a.dwelltimeatpodgreaterthan8dayscontainercount
,   a.dwelltimeatpoddayssum
,   a.positivedwelltimeatpoddayssum
,   a.negativedwelltimeatpoddayssum
,   a.minpositivedwelltimeatpoddayssum
,   a.maxpositivedwelltimeatpoddayssum
,   a.totaltimeatpodcontainercount
,   a.positivetotaltimeatpodcontainercount
,   a.negativetotaltimeatpodcontainercount
,   a.totaltimeatpod0lessthanorequalto1dayscontainercount
,   a.totaltimeatpod1lessthanorequalto2dayscontainercount
,   a.totaltimeatpod2lessthanorequalto3dayscontainercount
,   a.totaltimeatpod3lessthanorequalto4dayscontainercount
,   a.totaltimeatpod4lessthanorequalto5dayscontainercount
,   a.totaltimeatpod5lessthanorequalto6dayscontainercount
,   a.totaltimeatpod6lessthanorequalto7dayscontainercount
,   a.totaltimeatpod7lessthanorequalto8dayscontainercount
,   a.totaltimeatpodgreaterthan8dayscontainercount
,   a.totaltimeatpoddayssum
,   a.positivetotaltimeatpoddayssum
,   a.negativetotaltimeatpoddayssum
,   a.minpositivetotaltimeatpoddayssum
,   a.maxpositivetotaltimeatpoddayssum
,   a.fulloutgatefrompolcontainercount
,   b.fullcontainerdeliveryattrcontainercount
,   b.destinationinlandtransitcontainercount
,   b.positivedestinationinlandtransitcontainercount
,   b.negativedestinationinlandtransitcontainercount
,   b.destinationinlandtransit0lessthanorequalto1dayscontainercount
,   b.destinationinlandtransit1lessthanorequalto2dayscontainercount
,   b.destinationinlandtransit2lessthanorequalto3dayscontainercount
,   b.destinationinlandtransit3lessthanorequalto4dayscontainercount
,   b.destinationinlandtransit4lessthanorequalto5dayscontainercount
,   b.destinationinlandtransitgreaterthan5dayscontainercount
,   b.destinationinlandtransitdayssum
,   b.positivedestinationinlandtransitdayssum
,   b.negativedestinationinlandtransitdayssum
,   b.minpositivedestinationinlandtransitdayssum
,   b.maxpositivedestinationinlandtransitdayssum
FROM hv_orc_ocm_benchmark_fulloutgateocean_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_fullcontainerdelivery_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_emptycontainerreturn_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   a.customsreleasecontainercount
,   a.customsreleasetimecontainercount
,   a.positivecustomsreleasetimecontainercount
,   a.negativecustomsreleasetimecontainercount
,   a.customsreleasetime0lessthanorequalto1dayscontainercount
,   a.customsreleasetime1lessthanorequalto2dayscontainercount
,   a.customsreleasetime2lessthanorequalto3dayscontainercount
,   a.customsreleasetime3lessthanorequalto4dayscontainercount
,   a.customsreleasetime4lessthanorequalto5dayscontainercount
,   a.customsreleasetime5lessthanorequalto6dayscontainercount
,   a.customsreleasetime6lessthanorequalto7dayscontainercount
,   a.customsreleasetime7lessthanorequalto8dayscontainercount
,   a.customsreleasetimegreaterthan8dayscontainercount
,   a.customsreleasetimedayssum
,   a.positivecustomsreleasetimedayssum
,   a.negativecustomsreleasetimedayssum
,   a.minpositivecustomsreleasetimedayssum
,   a.maxpositivecustomsreleasetimedayssum
,   a.fullcontainerdischargedatpodcontainercount
,   a.yardoutcontainercount
,   a.containertransittimecontainercount
,   a.positivecontainertransittimecontainercount
,   a.negativecontainertransittimecontainercount
,   a.containertransittime0lessthanorequalto5dayscontainercount
,   a.containertransittime5lessthanorequalto10dayscontainercount
,   a.containertransittime10lessthanorequalto15dayscontainercount
,   a.containertransittime15lessthanorequalto20dayscontainercount
,   a.containertransittime20lessthanorequalto25dayscontainercount
,   a.containertransittime25lessthanorequalto30dayscontainercount
,   a.containertransittime30lessthanorequalto35dayscontainercount
,   a.containertransittime35lessthanorequalto40dayscontainercount
,   a.containertransittimegreaterthan40dayscontainercount
,   a.containertransittimedayssum
,   a.positivecontainertransittimedayssum
,   a.negativecontainertransittimedayssum
,   a.minpositivecontainertransittimedayssum
,   a.maxpositivecontainertransittimedayssum
,   a.dwelltimeatpodcontainercount
,   a.positivedwelltimeatpodcontainercount
,   a.negativedwelltimeatpodcontainercount
,   a.dwelltimeatpod0lessthanorequalto1dayscontainercount
,   a.dwelltimeatpod1lessthanorequalto2dayscontainercount
,   a.dwelltimeatpod2lessthanorequalto3dayscontainercount
,   a.dwelltimeatpod3lessthanorequalto4dayscontainercount
,   a.dwelltimeatpod4lessthanorequalto5dayscontainercount
,   a.dwelltimeatpod5lessthanorequalto6dayscontainercount
,   a.dwelltimeatpod6lessthanorequalto7dayscontainercount
,   a.dwelltimeatpod7lessthanorequalto8dayscontainercount
,   a.dwelltimeatpodgreaterthan8dayscontainercount
,   a.dwelltimeatpoddayssum
,   a.positivedwelltimeatpoddayssum
,   a.negativedwelltimeatpoddayssum
,   a.minpositivedwelltimeatpoddayssum
,   a.maxpositivedwelltimeatpoddayssum
,   a.totaltimeatpodcontainercount
,   a.positivetotaltimeatpodcontainercount
,   a.negativetotaltimeatpodcontainercount
,   a.totaltimeatpod0lessthanorequalto1dayscontainercount
,   a.totaltimeatpod1lessthanorequalto2dayscontainercount
,   a.totaltimeatpod2lessthanorequalto3dayscontainercount
,   a.totaltimeatpod3lessthanorequalto4dayscontainercount
,   a.totaltimeatpod4lessthanorequalto5dayscontainercount
,   a.totaltimeatpod5lessthanorequalto6dayscontainercount
,   a.totaltimeatpod6lessthanorequalto7dayscontainercount
,   a.totaltimeatpod7lessthanorequalto8dayscontainercount
,   a.totaltimeatpodgreaterthan8dayscontainercount
,   a.totaltimeatpoddayssum
,   a.positivetotaltimeatpoddayssum
,   a.negativetotaltimeatpoddayssum
,   a.minpositivetotaltimeatpoddayssum
,   a.maxpositivetotaltimeatpoddayssum
,   a.fulloutgatefrompolcontainercount
,   a.fullcontainerdeliveryattrcontainercount
,   a.destinationinlandtransitcontainercount
,   a.positivedestinationinlandtransitcontainercount
,   a.negativedestinationinlandtransitcontainercount
,   a.destinationinlandtransit0lessthanorequalto1dayscontainercount
,   a.destinationinlandtransit1lessthanorequalto2dayscontainercount
,   a.destinationinlandtransit2lessthanorequalto3dayscontainercount
,   a.destinationinlandtransit3lessthanorequalto4dayscontainercount
,   a.destinationinlandtransit4lessthanorequalto5dayscontainercount
,   a.destinationinlandtransitgreaterthan5dayscontainercount
,   a.destinationinlandtransitdayssum
,   a.positivedestinationinlandtransitdayssum
,   a.negativedestinationinlandtransitdayssum
,   a.minpositivedestinationinlandtransitdayssum
,   a.maxpositivedestinationinlandtransitdayssum
,   b.emptyreturncontainercount
,   b.dwelltimeawayfrompodcontainercount
,   b.positivedwelltimeawayfrompodcontainercount
,   b.negativedwelltimeawayfrompodcontainercount
,   b.dwelltimeawayfrompod0lessthanorequalto1dayscontainercount
,   b.dwelltimeawayfrompod1lessthanorequalto2dayscontainercount
,   b.dwelltimeawayfrompod2lessthanorequalto3dayscontainercount
,   b.dwelltimeawayfrompod3lessthanorequalto4dayscontainercount
,   b.dwelltimeawayfrompod4lessthanorequalto5dayscontainercount
,   b.dwelltimeawayfrompod5lessthanorequalto6dayscontainercount
,   b.dwelltimeawayfrompod6lessthanorequalto7dayscontainercount
,   b.dwelltimeawayfrompod7lessthanorequalto8dayscontainercount
,   b.dwelltimeawayfrompodgreaterthan8dayscontainercount
,   b.dwelltimeawayfrompoddayssum
,   b.positivedwelltimeawayfrompoddayssum
,   b.negativedwelltimeawayfrompoddayssum
,   b.minpositivedwelltimeawayfrompoddayssum
,   b.maxpositivedwelltimeawayfrompoddayssum
,   b.outforstrippingcontainercount
,   b.positiveoutforstrippingcontainercount
,   b.negativeoutforstrippingcontainercount
,   b.outforstripping0lessthanorequalto1dayscontainercount
,   b.outforstripping1lessthanorequalto2dayscontainercount
,   b.outforstripping2lessthanorequalto3dayscontainercount
,   b.outforstripping3lessthanorequalto4dayscontainercount
,   b.outforstripping4lessthanorequalto5dayscontainercount
,   b.outforstripping5lessthanorequalto6dayscontainercount
,   b.outforstripping6lessthanorequalto7dayscontainercount
,   b.outforstripping7lessthanorequalto8dayscontainercount
,   b.outforstrippinggreaterthan8dayscontainercount
,   b.outforstrippingdayssum
,   b.positiveoutforstrippingdayssum
,   b.negativeoutforstrippingdayssum
,   b.minpositiveoutforstrippingdayssum
,   b.maxpositiveoutforstrippingdayssum
FROM hv_orc_ocm_benchmark_fullcontainerdelivery_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_emptycontainerreturn_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

CREATE TABLE IF NOT EXISTS hv_orc_ocm_benchmark_create_no_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    COALESCE(a.carrierorgid, b.carrierorgid) AS carrierorgid
,   COALESCE(a.carriername, b.carriername) AS carriername
,   COALESCE(a.carriercluster, b.carriercluster) AS carriercluster
,   COALESCE(a.lanecityname, b.lanecityname) AS lanecityname
,   COALESCE(a.lanecitysubdivision, b.lanecitysubdivision) AS lanecitysubdivision
,   COALESCE(a.lanecitycluster1, b.lanecitycluster1) AS lanecitycluster1
,   COALESCE(a.lanecitycluster2, b.lanecitycluster2) AS lanecitycluster2
,   COALESCE(a.lanecountryname, b.lanecountryname) AS lanecountryname
,   COALESCE(a.laneregion1, b.laneregion1) AS laneregion1
,   COALESCE(a.laneregion2, b.laneregion2) AS laneregion2
,   COALESCE(a.polcityid, b.polcityid) AS polcityid
,   COALESCE(a.polcityname, b.polcityname) AS polcityname
,   COALESCE(a.polcityunlocode, b.polcityunlocode) AS polcityunlocode
,   COALESCE(a.polcitylongitude, b.polcitylongitude) AS polcitylongitude
,   COALESCE(a.polcitylatitude, b.polcitylatitude) AS polcitylatitude
,   COALESCE(a.polcitysubdivision, b.polcitysubdivision) AS polcitysubdivision
,   COALESCE(a.polcitycluster1, b.polcitycluster1) AS polcitycluster1
,   COALESCE(a.polcitycluster2, b.polcitycluster2) AS polcitycluster2
,   COALESCE(a.polcountryid, b.polcountryid) AS polcountryid
,   COALESCE(a.polcountryname, b.polcountryname) AS polcountryname
,   COALESCE(a.polregion1, b.polregion1) AS polregion1
,   COALESCE(a.polregion2, b.polregion2) AS polregion2
,   COALESCE(a.podcityid, b.podcityid) AS podcityid
,   COALESCE(a.podcityname, b.podcityname) AS podcityname
,   COALESCE(a.podcityunlocode, b.podcityunlocode) AS podcityunlocode
,   COALESCE(a.podcitylongitude, b.podcitylongitude) AS podcitylongitude
,   COALESCE(a.podcitylatitude, b.podcitylatitude) AS podcitylatitude
,   COALESCE(a.podcitysubdivision, b.podcitysubdivision) AS podcitysubdivision
,   COALESCE(a.podcitycluster1, b.podcitycluster1) AS podcitycluster1
,   COALESCE(a.podcitycluster2, b.podcitycluster2) AS podcitycluster2
,   COALESCE(a.podcountryid, b.podcountryid) AS podcountryid
,   COALESCE(a.podcountryname, b.podcountryname) AS podcountryname
,   COALESCE(a.podregion1, b.podregion1) AS podregion1
,   COALESCE(a.podregion2, b.podregion2) AS podregion2
,   COALESCE(a.transshipcityid, b.transshipcityid) AS transshipcityid
,   COALESCE(a.transshipcityname, b.transshipcityname) AS transshipcityname
,   COALESCE(a.transshipcityunlocode, b.transshipcityunlocode) AS transshipcityunlocode
,   COALESCE(a.transshipcitylongitude, b.transshipcitylongitude) AS transshipcitylongitude
,   COALESCE(a.transshipcitylatitude, b.transshipcitylatitude) AS transshipcitylatitude
,   COALESCE(a.transshipcitysubdivision, b.transshipcitysubdivision) AS transshipcitysubdivision
,   COALESCE(a.transshipcitycluster1, b.transshipcitycluster1) AS transshipcitycluster1
,   COALESCE(a.transshipcitycluster2, b.transshipcitycluster2) AS transshipcitycluster2
,   COALESCE(a.transshipcountryid, b.transshipcountryid) AS transshipcountryid
,   COALESCE(a.transshipcountryname, b.transshipcountryname) AS transshipcountryname
,   COALESCE(a.transshipregion1, b.transshipregion1) AS transshipregion1
,   COALESCE(a.transshipregion2, b.transshipregion2) AS transshipregion2
,   (CASE WHEN COALESCE(a.transshipcityflag, 0) = COALESCE(b.transshipcityflag, 0) AND COALESCE(a.transshipcityflag, 0) = 1 THEN 1 ELSE 0 END) AS transshipcityflag
,   COALESCE(a.containertypecluster, b.containertypecluster) AS containertypecluster
,   COALESCE(a.movetypecluster, b.movetypecluster) AS movetypecluster
,   COALESCE(a.originmovetypecluster, b.originmovetypecluster) AS originmovetypecluster
,   (CASE WHEN COALESCE(a.originmovetypeclusterflag, 0) = COALESCE(b.originmovetypeclusterflag, 0) AND COALESCE(a.originmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS originmovetypeclusterflag
,   COALESCE(a.destinationmovetypecluster, b.destinationmovetypecluster) AS destinationmovetypecluster
,   (CASE WHEN COALESCE(a.destinationmovetypeclusterflag, 0) = COALESCE(b.destinationmovetypeclusterflag, 0) AND COALESCE(a.destinationmovetypeclusterflag, 0) = 1 THEN 1 ELSE 0 END) AS destinationmovetypeclusterflag
,   COALESCE(a.datedimid, b.datedimid) AS datedimid
,   COALESCE(a.weekenddate, b.weekenddate) AS weekenddate
,   COALESCE(a.year, b.year) AS year
,   COALESCE(a.quarter, b.quarter) AS quarter
,   COALESCE(a.yearquarter, b.yearquarter) AS yearquarter
,   COALESCE(a.month, b.month) AS month
,   COALESCE(a.yearmonth, b.yearmonth) AS yearmonth
,   COALESCE(a.week, b.week) AS week
,   COALESCE(a.yearweek, b.yearweek) AS yearweek
,   a.emptyoutgatecontainercount
,   a.outforstuffingcontainercount
,   a.positiveoutforstuffingcontainercount
,   a.negativeoutforstuffingcontainercount
,   a.outforstuffing0lessthanorequalto1dayscontainercount
,   a.outforstuffing1lessthanorequalto2dayscontainercount
,   a.outforstuffing2lessthanorequalto3dayscontainercount
,   a.outforstuffing3lessthanorequalto4dayscontainercount
,   a.outforstuffing4lessthanorequalto5dayscontainercount
,   a.outforstuffing5lessthanorequalto6dayscontainercount
,   a.outforstuffing6lessthanorequalto7dayscontainercount
,   a.outforstuffing7lessthanorequalto8dayscontainercount
,   a.outforstuffinggreaterthan8dayscontainercount
,   a.outforstuffingdayssum
,   a.positiveoutforstuffingdayssum
,   a.negativeoutforstuffingdayssum
,   a.minpositiveoutforstuffingdays
,   a.maxpositiveoutforstuffingdays
,   a.yardincontainercount
,   a.dwelltimeawayfrompolcontainercount
,   a.positivedwelltimeawayfrompolcontainercount
,   a.negativedwelltimeawayfrompolcontainercount
,   a.dwelltimeawayfrompol0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompol1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompol2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompol3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompol4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompol5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompol6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompol7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompolgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoldayssum
,   a.positivedwelltimeawayfrompoldayssum
,   a.negativedwelltimeawayfrompoldayssum
,   a.minpositivedwelltimeawayfrompoldays
,   a.maxpositivedwelltimeawayfrompoldays
,   a.origininlandtransitcontainercount
,   a.positiveorigininlandtransitcontainercount
,   a.negativeorigininlandtransitcontainercount
,   a.origininlandtransit0lessthanorequalto1dayscontainercount
,   a.origininlandtransit1lessthanorequalto2dayscontainercount
,   a.origininlandtransit2lessthanorequalto3dayscontainercount
,   a.origininlandtransit3lessthanorequalto4dayscontainercount
,   a.origininlandtransit4lessthanorequalto5dayscontainercount
,   a.origininlandtransitgreaterthan5dayscontainercount
,   a.origininlandtransitdayssum
,   a.positiveorigininlandtransitdayssum
,   a.negativeorigininlandtransitdayssum
,   a.minpositiveorigininlandtransitdays
,   a.maxpositiveorigininlandtransitdays
,   a.onboardcontainercount
,   a.dwelltimeatpolcontainercount
,   a.positivedwelltimeatpolcontainercount
,   a.negativedwelltimeatpolcontainercount
,   a.dwelltimeatpoldayssum
,   a.positivedwelltimeatpoldayssum
,   a.negativedwelltimeatpoldayssum
,   a.minpositivedwelltimeatpoldays
,   a.maxpositivedwelltimeatpoldays
,   a.bookedetdpolcontainercount
,   a.btdpolcontainercount
,   a.pollatenesscontainercount
,   a.otdperformancecontainercount
,   a.positivepollatenesscontainercount
,   a.negativepollatenesscontainercount
,   a.otdperformancegreaterthan8daysearly
,   a.otdperformance7lessthanorequalto8daysearly
,   a.otdperformance6lessthanorequalto7daysearly
,   a.otdperformance5lessthanorequalto6daysearly
,   a.otdperformance4lessthanorequalto5daysearly
,   a.otdperformance3lessthanorequalto4daysearly
,   a.otdperformance2lessthanorequalto3daysearly
,   a.otdperformance1lessthanorequalto2daysearly
,   a.otdperformance0lessthanorequalto1daysearly
,   a.otdperformance0lessthanorequalto1dayslate
,   a.otdperformance1lessthanorequalto2dayslate
,   a.otdperformance2lessthanorequalto3dayslate
,   a.otdperformance3lessthanorequalto4dayslate
,   a.otdperformance4lessthanorequalto5dayslate
,   a.otdperformancegreaterthan5dayslate
,   a.otdperformance5lessthanorequalto6dayslate
,   a.otdperformance6lessthanorequalto7dayslate
,   a.otdperformance7lessthanorequalto8dayslate
,   a.otdperformancegreaterthan8dayslate
,   a.pollatenessdayssum
,   a.positivepollatenessdayssum
,   a.negativepollatenessdayssum
,   a.arrivedattransshipmentcompletenesscontainercount
,   a.bookedetdpolcompletenesscontainercount
,   a.departedfrompolcompletenesscontainercount
,   a.departedfromtransshipmentcompletenesscontainercount
,   a.emptyoutgatecompletenesscontainercount
,   a.emptyoutfrominlandcompletenesscontainercount
,   a.emptyoutfromportcompletenesscontainercount
,   a.etdpolcompletenesscontainercount
,   a.fulloutgatefrompolcompletenesscontainercount
,   a.onboardcompletenesscontainercount
,   a.yardincompletenesscontainercount
,   a.speedkmshr
,   a.teusum
,   a.teuatpolsum
,   a.containertransittimesum
,   a.containertransittimecount
,   a.vesseltransittimesum
,   a.vesseltransittimecount
,   a.otdperformancesum
,   a.otdperformancecount
,   a.otaperformancesum
,   a.otaperformancecount
,   a.pollatenesssum
,   a.pollatenesscount
,   a.podlatenesssum
,   a.podlatenesscount
,   a.speeddistancekms
,   a.speedtimehr
,   a.totaltimeatpolsum
,   a.totaltimeatpolcount
,   a.totaltimeatpodsum
,   a.totaltimeatpodcount
,   a.teuranksum
,   a.teurankcount
,   a.teuatpolranksum
,   a.teuatpolrankcount
,   a.teuatpodranksum
,   a.teuatpodrankcount
,   a.departedfrompolcontainercount
,   a.totaltimeatpolcontainercount
,   a.positivetotaltimeatpolcontainercount
,   a.negativetotaltimeatpolcontainercount
,   a.totaltimeatpol0lessthanorequalto1dayscontainercount
,   a.totaltimeatpol1lessthanorequalto2dayscontainercount
,   a.totaltimeatpol2lessthanorequalto3dayscontainercount
,   a.totaltimeatpol3lessthanorequalto4dayscontainercount
,   a.totaltimeatpol4lessthanorequalto5dayscontainercount
,   a.totaltimeatpol5lessthanorequalto6dayscontainercount
,   a.totaltimeatpol6lessthanorequalto7dayscontainercount
,   a.totaltimeatpol7lessthanorequalto8dayscontainercount
,   a.totaltimeatpolgreaterthan8dayscontainercount
,   a.totaltimeatpoldayssum
,   a.positivetotaltimeatpoldayssum
,   a.negativetotaltimeatpoldayssum
,   a.minpositivetotaltimeatpoldayssum
,   a.maxpositivetotaltimeatpoldayssum
,   a.arrivedattransshipmentcontainercount
,   a.departedfromtransshipmentcontainercount
,   a.dwelltimeattransshipmentportcontainercount
,   a.positivedwelltimeattransshipmentportcontainercount
,   a.negativedwelltimeattransshipmentportcontainercount
,   a.dwelltimeattransshipmentport0lessthanorequalto1dayscontainercount
,   a.dwelltimeattransshipmentport1lessthanorequalto2dayscontainercount
,   a.dwelltimeattransshipmentport2lessthanorequalto3dayscontainercount
,   a.dwelltimeattransshipmentport3lessthanorequalto4dayscontainercount
,   a.dwelltimeattransshipmentport4lessthanorequalto5dayscontainercount
,   a.dwelltimeattransshipmentportgreaterthan5dayscontainercount
,   a.dwelltimeattransshipmentportdayssum
,   a.positivedwelltimeattransshipmentportdayssum
,   a.negativedwelltimeattransshipmentportdayssum
,   a.minpositivedwelltimeattransshipmentportdayssum
,   a.maxpositivedwelltimeattransshipmentportdayssum
,   a.btapodcontainercount
,   a.podlatenesscontainercount
,   a.otaperformancecontainercount
,   a.positivepodlatenesscontainercount
,   a.negativepodlatenesscontainercount
,   a.otaperformancegreaterthan8daysearly
,   a.otaperformance7lessthanorequalto8daysearly
,   a.otaperformance6lessthanorequalto7daysearly
,   a.otaperformance5lessthanorequalto6daysearly
,   a.otaperformance4lessthanorequalto5daysearly
,   a.otaperformance3lessthanorequalto4daysearly
,   a.otaperformance2lessthanorequalto3daysearly
,   a.otaperformance1lessthanorequalto2daysearly
,   a.otaperformance0lessthanorequalto1daysearly
,   a.otaperformance0lessthanorequalto1dayslate
,   a.otaperformance1lessthanorequalto2dayslate
,   a.otaperformance2lessthanorequalto3dayslate
,   a.otaperformance3lessthanorequalto4dayslate
,   a.otaperformance4lessthanorequalto5dayslate
,   a.otaperformancegreaterthan5dayslate
,   a.otaperformance5lessthanorequalto6dayslate
,   a.otaperformance6lessthanorequalto7dayslate
,   a.otaperformance7lessthanorequalto8dayslate
,   a.otaperformancegreaterthan8dayslate
,   a.podlatenessdayssum
,   a.positivepodlatenessdayssum
,   a.negativepodlatenessdayssum
,   a.arrivedatpodcompletenesscontainercount
,   a.bookedetapodcompletenesscontainercount
,   a.customsreleasecompletenesscontainercount
,   a.emptyreturncompletenesscontainercount
,   a.etapodcompletenesscontainercount
,   a.fullcontainerdischargedatpodcompletenesscontainercount
,   a.yardoutcompletenesscontainercount
,   a.teuatpodsum
,   a.bookedetapodcontainercount
,   a.arrivedatpodcontainercount
,   a.vesseltransittimecontainercount
,   a.vesseltransittimeperformancecontainercount
,   a.positivevesseltransittimecontainercount
,   a.negativevesseltransittimecontainercount
,   a.vesseltransittime0lessthanorequalto5dayscontainercount
,   a.vesseltransittime5lessthanorequalto10dayscontainercount
,   a.vesseltransittime10lessthanorequalto15dayscontainercount
,   a.vesseltransittime15lessthanorequalto20dayscontainercount
,   a.vesseltransittime20lessthanorequalto25dayscontainercount
,   a.vesseltransittime25lessthanorequalto30dayscontainercount
,   a.vesseltransittime30lessthanorequalto35dayscontainercount
,   a.vesseltransittime35lessthanorequalto40dayscontainercount
,   a.vesseltransittimegreaterthan40dayscontainercount
,   a.positivevesseltransittimedayssum
,   a.negativevesseltransittimedayssum
,   a.minpositivevesseltransittimedayssum
,   a.maxpositivevesseltransittimedayssum
,   a.vesselperformancegreaterthan10daysearlycontainercount
,   a.vesselperformance9lessthanorequalto10daysearlycontainercount
,   a.vesselperformance8lessthanorequalto9daysearlycontainercount
,   a.vesselperformance7lessthanorequalto8daysearlycontainercount
,   a.vesselperformance6lessthanorequalto7daysearlycontainercount
,   a.vesselperformance5lessthanorequalto6daysearlycontainercount
,   a.vesselperformance4lessthanorequalto5daysearlycontainercount
,   a.vesselperformance3lessthanorequalto4daysearlycontainercount
,   a.vesselperformance2lessthanorequalto3daysearlycontainercount
,   a.vesselperformance1lessthanorequalto2daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1daysearlycontainercount
,   a.vesselperformance0lessthanorequalto1dayslatecontainercount
,   a.vesselperformance1lessthanorequalto2dayslatecontainercount
,   a.vesselperformance2lessthanorequalto3dayslatecontainercount
,   a.vesselperformance3lessthanorequalto4dayslatecontainercount
,   a.vesselperformance4lessthanorequalto5dayslatecontainercount
,   a.vesselperformance5lessthanorequalto6dayslatecontainercount
,   a.vesselperformance6lessthanorequalto7dayslatecontainercount
,   a.vesselperformance7lessthanorequalto8dayslatecontainercount
,   a.vesselperformance8lessthanorequalto9dayslatecontainercount
,   a.vesselperformance9lessthanorequalto10dayslatecontainercount
,   a.vesselperformancegreaterthan10dayslatecontainercount
,   a.customsreleasecontainercount
,   a.customsreleasetimecontainercount
,   a.positivecustomsreleasetimecontainercount
,   a.negativecustomsreleasetimecontainercount
,   a.customsreleasetime0lessthanorequalto1dayscontainercount
,   a.customsreleasetime1lessthanorequalto2dayscontainercount
,   a.customsreleasetime2lessthanorequalto3dayscontainercount
,   a.customsreleasetime3lessthanorequalto4dayscontainercount
,   a.customsreleasetime4lessthanorequalto5dayscontainercount
,   a.customsreleasetime5lessthanorequalto6dayscontainercount
,   a.customsreleasetime6lessthanorequalto7dayscontainercount
,   a.customsreleasetime7lessthanorequalto8dayscontainercount
,   a.customsreleasetimegreaterthan8dayscontainercount
,   a.customsreleasetimedayssum
,   a.positivecustomsreleasetimedayssum
,   a.negativecustomsreleasetimedayssum
,   a.minpositivecustomsreleasetimedayssum
,   a.maxpositivecustomsreleasetimedayssum
,   a.fullcontainerdischargedatpodcontainercount
,   a.yardoutcontainercount
,   a.containertransittimecontainercount
,   a.positivecontainertransittimecontainercount
,   a.negativecontainertransittimecontainercount
,   a.containertransittime0lessthanorequalto5dayscontainercount
,   a.containertransittime5lessthanorequalto10dayscontainercount
,   a.containertransittime10lessthanorequalto15dayscontainercount
,   a.containertransittime15lessthanorequalto20dayscontainercount
,   a.containertransittime20lessthanorequalto25dayscontainercount
,   a.containertransittime25lessthanorequalto30dayscontainercount
,   a.containertransittime30lessthanorequalto35dayscontainercount
,   a.containertransittime35lessthanorequalto40dayscontainercount
,   a.containertransittimegreaterthan40dayscontainercount
,   a.containertransittimedayssum
,   a.positivecontainertransittimedayssum
,   a.negativecontainertransittimedayssum
,   a.minpositivecontainertransittimedayssum
,   a.maxpositivecontainertransittimedayssum
,   a.dwelltimeatpodcontainercount
,   a.positivedwelltimeatpodcontainercount
,   a.negativedwelltimeatpodcontainercount
,   a.dwelltimeatpod0lessthanorequalto1dayscontainercount
,   a.dwelltimeatpod1lessthanorequalto2dayscontainercount
,   a.dwelltimeatpod2lessthanorequalto3dayscontainercount
,   a.dwelltimeatpod3lessthanorequalto4dayscontainercount
,   a.dwelltimeatpod4lessthanorequalto5dayscontainercount
,   a.dwelltimeatpod5lessthanorequalto6dayscontainercount
,   a.dwelltimeatpod6lessthanorequalto7dayscontainercount
,   a.dwelltimeatpod7lessthanorequalto8dayscontainercount
,   a.dwelltimeatpodgreaterthan8dayscontainercount
,   a.dwelltimeatpoddayssum
,   a.positivedwelltimeatpoddayssum
,   a.negativedwelltimeatpoddayssum
,   a.minpositivedwelltimeatpoddayssum
,   a.maxpositivedwelltimeatpoddayssum
,   a.totaltimeatpodcontainercount
,   a.positivetotaltimeatpodcontainercount
,   a.negativetotaltimeatpodcontainercount
,   a.totaltimeatpod0lessthanorequalto1dayscontainercount
,   a.totaltimeatpod1lessthanorequalto2dayscontainercount
,   a.totaltimeatpod2lessthanorequalto3dayscontainercount
,   a.totaltimeatpod3lessthanorequalto4dayscontainercount
,   a.totaltimeatpod4lessthanorequalto5dayscontainercount
,   a.totaltimeatpod5lessthanorequalto6dayscontainercount
,   a.totaltimeatpod6lessthanorequalto7dayscontainercount
,   a.totaltimeatpod7lessthanorequalto8dayscontainercount
,   a.totaltimeatpodgreaterthan8dayscontainercount
,   a.totaltimeatpoddayssum
,   a.positivetotaltimeatpoddayssum
,   a.negativetotaltimeatpoddayssum
,   a.minpositivetotaltimeatpoddayssum
,   a.maxpositivetotaltimeatpoddayssum
,   a.fulloutgatefrompolcontainercount
,   a.fullcontainerdeliveryattrcontainercount
,   a.destinationinlandtransitcontainercount
,   a.positivedestinationinlandtransitcontainercount
,   a.negativedestinationinlandtransitcontainercount
,   a.destinationinlandtransit0lessthanorequalto1dayscontainercount
,   a.destinationinlandtransit1lessthanorequalto2dayscontainercount
,   a.destinationinlandtransit2lessthanorequalto3dayscontainercount
,   a.destinationinlandtransit3lessthanorequalto4dayscontainercount
,   a.destinationinlandtransit4lessthanorequalto5dayscontainercount
,   a.destinationinlandtransitgreaterthan5dayscontainercount
,   a.destinationinlandtransitdayssum
,   a.positivedestinationinlandtransitdayssum
,   a.negativedestinationinlandtransitdayssum
,   a.minpositivedestinationinlandtransitdayssum
,   a.maxpositivedestinationinlandtransitdayssum
,   a.emptyreturncontainercount
,   a.dwelltimeawayfrompodcontainercount
,   a.positivedwelltimeawayfrompodcontainercount
,   a.negativedwelltimeawayfrompodcontainercount
,   a.dwelltimeawayfrompod0lessthanorequalto1dayscontainercount
,   a.dwelltimeawayfrompod1lessthanorequalto2dayscontainercount
,   a.dwelltimeawayfrompod2lessthanorequalto3dayscontainercount
,   a.dwelltimeawayfrompod3lessthanorequalto4dayscontainercount
,   a.dwelltimeawayfrompod4lessthanorequalto5dayscontainercount
,   a.dwelltimeawayfrompod5lessthanorequalto6dayscontainercount
,   a.dwelltimeawayfrompod6lessthanorequalto7dayscontainercount
,   a.dwelltimeawayfrompod7lessthanorequalto8dayscontainercount
,   a.dwelltimeawayfrompodgreaterthan8dayscontainercount
,   a.dwelltimeawayfrompoddayssum
,   a.positivedwelltimeawayfrompoddayssum
,   a.negativedwelltimeawayfrompoddayssum
,   a.minpositivedwelltimeawayfrompoddayssum
,   a.maxpositivedwelltimeawayfrompoddayssum
,   a.outforstrippingcontainercount
,   a.positiveoutforstrippingcontainercount
,   a.negativeoutforstrippingcontainercount
,   a.outforstripping0lessthanorequalto1dayscontainercount
,   a.outforstripping1lessthanorequalto2dayscontainercount
,   a.outforstripping2lessthanorequalto3dayscontainercount
,   a.outforstripping3lessthanorequalto4dayscontainercount
,   a.outforstripping4lessthanorequalto5dayscontainercount
,   a.outforstripping5lessthanorequalto6dayscontainercount
,   a.outforstripping6lessthanorequalto7dayscontainercount
,   a.outforstripping7lessthanorequalto8dayscontainercount
,   a.outforstrippinggreaterthan8dayscontainercount
,   a.outforstrippingdayssum
,   a.positiveoutforstrippingdayssum
,   a.negativeoutforstrippingdayssum
,   a.minpositiveoutforstrippingdayssum
,   a.maxpositiveoutforstrippingdayssum
,   b.createdatecontainercount
,   b.firstetdpolcompletenesscontainercount
,   b.firstetapodcompletenesscontainercount
FROM hv_orc_ocm_benchmark_emptycontainerreturn_no_owner_agg_temp AS a
    FULL OUTER JOIN hv_orc_create_no_owner_agg AS b
    ON a.carrierorgid = b.carrierorgid
    AND a.podcityid = b.podcityid
    AND a.polcityid = b.polcityid
    AND a.transshipcityid = b.transshipcityid
    AND a.containertypecluster = b.containertypecluster
    AND a.movetypecluster = b.movetypecluster
    AND a.originmovetypecluster = b.originmovetypecluster
    AND a.destinationmovetypecluster = b.destinationmovetypecluster
    AND a.datedimid = b.datedimid
;

DROP TABLE IF EXISTS hv_orc_ocm_benchmark_no_owner_agg;

ALTER TABLE hv_orc_ocm_benchmark_create_no_owner_agg_temp RENAME TO hv_orc_ocm_benchmark_no_owner_agg;

DROP TABLE IF EXISTS hv_orc_ocm_benchmark_arrivepod_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_arrivetransship_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_booketapod_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_booketdpol_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_btapod_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_btdpol_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_create_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_customsreleased_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_departpol_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_departtransship_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_emptycontainerreturn_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_fullcontainerdelivery_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_fullcontainerdischarge_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_fulloutgateocean_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_onboard_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_outforstuffing_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_yardin_no_owner_agg_temp;
DROP TABLE IF EXISTS hv_orc_ocm_benchmark_yardout_no_owner_agg_temp;