SET tez.queue.name=Hive;

USE community_common;

DROP TABLE IF EXISTS hv_orc_tt_vmcontainermove_owner_agg_temp;

CREATE TABLE IF NOT EXISTS hv_orc_tt_vmcontainermove_owner_agg_temp
STORED AS ORC TBLPROPERTIES("orc.compress" = "SNAPPY")
AS
SELECT
    a.carrierorg_id
,   a.carriername
,   a.carriergroup AS carriergroupname
,   a.ownerorg_id
,   a.ownerorgname
,   a.customerorg_id
,   a.customerorgname
,   a.lanecityname
,   a.lanecityiata AS laneiata
,   a.lanecityunlocode AS laneunlocode
,   a.lanecountryname
,   a.laneregion1
,   a.laneregion2
,   a.polcity_id
,   a.polcityname
,   a.polcityiata
,   a.polcityunlocode
,   a.polcitylongitude
,   a.polcitylatitude
,   a.polcountry_id
,   a.polcountryname
,   a.polcountrycode
,   a.polregion1
,   a.polregion2
,   a.podcity_id
,   a.podcityname
,   a.podcityiata
,   a.podcityunlocode
,   a.podcitylongitude
,   a.podcitylatitude
,   a.podcountry_id
,   a.podcountryname
,   a.podcountrycode
,   a.podregion1
,   a.podregion2
,   a.origcity_id
,   a.origcityname
,   a.origcityiata
,   a.origcityunlocode
,   a.origcitylongitude
,   a.origcitylatitude
,   a.origcountry_id
,   a.origcountryname
,   a.origcountrycode
,   a.origregion1
,   a.origregion2
,   a.destcity_id
,   a.destcityname
,   a.destcityiata
,   a.destcityunlocode
,   a.destcitylongitude
,   a.destcitylatitude
,   a.destcountry_id
,   a.destcountryname
,   a.destcountrycode
,   a.destregion1
,   a.destregion2
,   a.containertype_id AS containertype
,   a.containertype AS containertypename
,   a.containertypecluster AS equipmenttype
,   a.originmovetype_id AS originmove_id
,   a.destinationmovetype_id AS destinationmove_id
,   a.movetype_id
,   a.movetype AS movetypename
,   a.movetypecode
,   a.movetypecluster AS servicetype
,   a.the_year
,   a.the_year_quarter
,   a.the_year_month
,   a.the_year_week
,   a.the_quarter
,   a.the_quarter_name
,   a.the_month
,   a.the_month_name
,   a.the_week
,   a.the_week_name
,   'Ocean' as mode
,   COUNT(vmcontainermove_id) AS container_count
,   SUM(weight_kg) AS weight_kg_sum
,   COUNT(weight_kg) AS weight_kg_rowcount
,   SUM(volume_cr) AS volume_cr_sum
,   COUNT(volume_cr) AS volume_cr_rowcount
,   SUM(capacityweight_kg) AS capacityweight_kg_sum
,   COUNT(capacityweight_kg) AS capacityweight_kg_rowcount
,   SUM(capacityvolume_cr) AS capacityvolume_cr_sum
,   COUNT(capacityvolume_cr) AS capacityvolume_cr_rowcount
,   SUM(teu) AS teu_sum
,   COUNT(teu) AS teu_rowcount
,   SUM(distance_km) AS distance_km_sum
,   COUNT(distance_km) AS distance_km_rowcount
,   SUM(speed) AS speed_sum
,   COUNT(speed) AS speed_rowcount
,   SUM(polteu) AS polteu_sum
,   COUNT(polteu) AS polteu_rowcount
,   SUM(podteu) AS podteu_sum
,   COUNT(podteu) AS podteu_rowcount
,   SUM(polcontainer) AS polcontainer_sum
,   COUNT(polcontainer) AS polcontainer_rowcount
,   SUM(podcontainer) AS podcontainer_sum
,   COUNT(podcontainer) AS pod_container_rowcount
,   SUM(oceantransittime) AS oceantransittime_sum
,   COUNT(oceantransittime) AS oceantransittime_rowcount
,   SUM(containertransittime) AS containertransittime_sum
,   COUNT(containertransittime) AS containertransittime_rowcount
,   SUM(dwellatpol) AS poldwell_sum
,   COUNT(dwellatpol) AS poldwell_rowcount
,   SUM(dwellatpod) AS poddwell_sum
,   COUNT(dwellatpod) AS poddwell_rowcount
,   SUM(CASE WHEN poltimeliness >= -11.60 THEN poltimeliness else 0 end) AS polontimetime_sum
,   COUNT(vmcontainermove_id) AS polontimetime_rowcount
,   SUM(CASE WHEN poltimeliness >= -11.60 THEN 1 else 0 end) AS polontime_sum
,   COUNT(vmcontainermove_id) AS polontime_rowcount
,   SUM(CASE WHEN poltimeliness < -11.60 THEN poltimeliness else 0 end) AS pollatetime_sum
,   COUNT(vmcontainermove_id) AS pollatetime_rowcount
,   SUM(CASE WHEN poltimeliness < -11.60 THEN 1 else 0 end) AS pollate_sum
,   COUNT(vmcontainermove_id) AS pollate_rowcount
,   SUM(poltimeliness) AS poltimeliness_sum
,   COUNT(poltimeliness) AS poltimeliness_rowcount
,   SUM(CASE WHEN podtimeliness >= -11.60 THEN podtimeliness else 0 end) AS podontimetime_sum
,   COUNT(vmcontainermove_id) AS podontimetime_rowcount
,   SUM(CASE WHEN podtimeliness >= -11.60 THEN 1 else 0 end) AS podontime_sum
,   COUNT(vmcontainermove_id) AS podontime_rowcount
,   SUM(CASE WHEN podtimeliness < -11.60 THEN podtimeliness else 0 end) AS podlatetime_sum
,   COUNT(vmcontainermove_id) AS podlatetime_rowcount
,   SUM(CASE WHEN podtimeliness < -11.60 THEN 1 else 0 end) AS podlate_sum
,   COUNT(vmcontainermove_id) AS podlate_rowcount
,   SUM(podtimeliness) AS podtimeliness_sum
,   COUNT(podtimeliness) AS podtimeliness_rowcount
,   SUM(fullcontainerrec_etdrec) AS fullcontainerrec_etdrec_sum
,   COUNT(fullcontainerrec_etdrec) AS fullcontainerrec_etdrec_count
,   SUM(fullcontainerrec_etdrec_processed) AS fullcontainerrec_etdrec_processed_sum
,   COUNT(fullcontainerrec_etdrec_processed) AS fullcontainerrec_etdrec_processed_count
,   SUM(fullcontainerrec_etdorig_processed) AS fullcontainerrec_etdorig_processed_sum
,   COUNT(fullcontainerrec_etdorig_processed) AS fullcontainerrec_etdorig_processed_count
,   SUM(fullcontainerrec_etdorig) AS fullcontainerrec_etdorig_sum
,   COUNT(fullcontainerrec_etdorig) AS fullcontainerrec_etdorig_count
,   SUM(atd_etdorigin) AS atd_etdorigin_sum
,   COUNT(atd_etdorigin) AS atd_etdorigin_count
,   SUM(first_booketdpol) AS first_booketdpol_sum
,   COUNT(first_booketdpol) AS first_booketdpol_count
,   SUM(prev_booketdpol) AS prev_booketdpol_sum
,   COUNT(prev_booketdpol) AS prev_booketdpol_count
,   SUM(etd_booketdpol) AS etd_booketdpol_sum
,   COUNT(etd_booketdpol) AS etd_booketdpol_count
,   SUM(atd_booketdpol) AS atd_booketdpol_sum
,   COUNT(atd_booketdpol) AS atd_booketdpol_count
,   SUM(prev_firstetdpol) AS prev_firstetdpol_sum
,   COUNT(prev_firstetdpol) AS prev_firstetdpol_count
,   SUM(atd_firstetdpol) AS atd_firstetdpol_sum
,   COUNT(atd_firstetdpol) AS atd_firstetdpol_count
,   SUM(etd_prev_etdpol) AS etd_prev_etdpol_sum
,   COUNT(etd_prev_etdpol) AS etd_prev_etdpol_count
,   SUM(depart_prev_etdpol) AS depart_prev_etdpol_sum
,   COUNT(depart_prev_etdpol) AS depart_prev_etdpol_count
,   SUM(atd_prev_etdpol) AS atd_prev_etdpol_sum
,   COUNT(atd_prev_etdpol) AS atd_prev_etdpol_count
,   SUM(atd_departpoltime) AS atd_departpoltime_sum
,   COUNT(atd_departpoltime) AS atd_departpoltime_count
,   SUM(first_booketapod) AS first_booketapod_sum
,   COUNT(first_booketapod) AS first_booketapod_count
,   SUM(prev_booketapod) AS prev_booketapod_sum
,   COUNT(prev_booketapod) AS prev_booketapod_count
,   SUM(ata_booketapod) AS ata_booketapod_sum
,   COUNT(ata_booketapod) AS ata_booketapod_count
,   SUM(prev_firsteta) AS prev_firsteta_sum
,   COUNT(prev_firsteta) AS prev_firsteta_count
,   SUM(ata_firsteta) AS ata_firsteta_sum
,   COUNT(ata_firsteta) AS ata_firsteta_count
,   SUM(eta_prev_etapod) AS eta_prev_etapod_sum
,   COUNT(eta_prev_etapod) AS eta_prev_etapod_count
,   SUM(arrived_prev_etapod) AS arrived_prev_etapod_sum
,   COUNT(arrived_prev_etapod) AS arrived_prev_etapod_count
,   SUM(ata_prev_etapod) AS ata_prev_etapod_sum
,   COUNT(ata_prev_etapod) AS ata_prev_etapod_count
,   SUM(ata_arrivepodtime) AS ata_arrivepodtime_sum
,   COUNT(ata_arrivepodtime) AS ata_arrivepodtime_count
,   SUM(etadest_booketadel) AS etadest_booketadel_sum
,   COUNT(etadest_booketadel) AS etadest_booketadel_count
,   SUM(containerdelivery_booketadel) AS containerdelivery_booketadel_sum
,   COUNT(containerdelivery_booketadel) AS containerdelivery_booketadel_count
,   SUM(atadest_booketadel) AS atadest_booketadel_sum
,   COUNT(atadest_booketadel) AS atadest_booketadel_count
,   SUM(dwellawayfrompod) AS total_time_away_from_pod_sum
,   COUNT(dwellawayfrompod) AS total_time_away_from_pod_count
,   SUM(dwellawayfrompol) AS total_time_away_from_pol_sum
,   COUNT(dwellawayfrompol) AS total_time_away_from_pol_count
,   SUM(booking_etd_pol_accuracy) AS booking_etd_pol_accuracy_sum
,   COUNT(booking_etd_pol_accuracy) AS booking_etd_pol_accuracy_count
,   SUM(first_etd_pol_variance) AS first_etd_pol_variance_sum
,   COUNT(first_etd_pol_variance) AS first_etd_pol_variance_count
,   SUM(first_etd_pol_accuracy) AS first_etd_pol_accuracy_sum
,   COUNT(first_etd_pol_accuracy) AS first_etd_pol_accuracy_count
,   SUM(etd_pol_accuracy) AS etd_pol_accuracy_sum
,   COUNT(etd_pol_accuracy) AS etd_pol_accuracy_count
,   SUM(departure_time_diff) AS departure_time_diff_sum
,   COUNT(departure_time_diff) AS departure_time_diff_count
,   SUM(book_eta_pod_variance) AS book_eta_pod_variance_sum
,   COUNT(book_eta_pod_variance) AS book_eta_pod_variance_count
,   SUM(book_eta_pod_accuracy) AS book_eta_pod_accuracy_sum
,   COUNT(book_eta_pod_accuracy) AS book_eta_pod_accuracy_count
,   SUM(first_eta_pod_accuracy) AS first_eta_pod_accuracy_sum
,   COUNT(first_eta_pod_accuracy) AS first_eta_pod_accuracy_count
,   SUM(first_eta_pod_variance) AS first_eta_pod_variance_sum
,   COUNT(first_eta_pod_variance) AS first_eta_pod_variance_count
,   SUM(eta_pod_accuracy) AS eta_pod_accuracy_sum
,   COUNT(eta_pod_accuracy) AS eta_pod_accuracy_count
,   SUM(arrival_time_differene) AS arrival_time_differene_sum
,   COUNT(arrival_time_differene) AS arrival_time_differene_count
,   SUM(outforstripping) AS out_for_stripping_sum
,   COUNT(outforstripping) AS out_for_stripping_count
,   SUM(outforstuffing) AS out_for_stuffing_sum
,   COUNT(outforstuffing) AS out_for_stuffing_count
FROM hv_orc_tt_vmcontainermove_owner_base a
WHERE carrierorg_id > 0
AND ownerorg_id > 0
AND polcity_id > 0
AND podcity_id > 0
AND movetype_id > 0
AND btdpol IS NOT NULL
AND a.destcity_id >= 0
AND a.origcity_id >= 0
GROUP BY a.carrierorg_id
,   a.carriername
,   a.ownerorg_id
,   a.ownerorgname
,   a.customerorg_id
,   a.customerorgname
,   a.lanecityname
,   a.lanecityiata
,   a.lanecityunlocode
,   a.lanecountryname
,   a.laneregion1
,   a.laneregion2
,   a.polcity_id
,   a.polcityname
,   a.polcityiata
,   a.polcityunlocode
,   a.polcitylongitude
,   a.polcitylatitude
,   a.polcountry_id
,   a.polcountryname
,   a.polcountrycode
,   a.polregion1
,   a.polregion2
,   a.podcity_id
,   a.podcityname
,   a.podcityiata
,   a.podcityunlocode
,   a.podcitylongitude
,   a.podcitylatitude
,   a.podcountry_id
,   a.podcountryname
,   a.podcountrycode
,   a.podregion1
,   a.podregion2
,   a.origcity_id
,   a.origcityname
,   a.origcityiata
,   a.origcityunlocode
,   a.origcitylongitude
,   a.origcitylatitude
,   a.origcountry_id
,   a.origcountryname
,   a.origcountrycode
,   a.origregion1
,   a.origregion2
,   a.destcity_id
,   a.destcityname
,   a.destcityiata
,   a.destcityunlocode
,   a.destcitylongitude
,   a.destcitylatitude
,   a.destcountry_id
,   a.destcountryname
,   a.destcountrycode
,   a.destregion1
,   a.destregion2
,   a.containertype_id
,   a.containertype
,   a.containertypecluster
,   a.originmovetype_id
,   a.destinationmovetype_id
,   a.movetype_id
,   a.movetype
,   a.movetypecode
,   a.movetypecluster
,   a.the_year
,   a.the_year_quarter
,   a.the_year_month
,   a.the_year_week
,   a.the_quarter
,   a.the_quarter_name
,   a.the_month
,   a.the_month_name
,   a.the_week
,   a.the_week_name
,   a.carriergroup;

DROP TABLE IF EXISTS hv_orc_tt_vmcontainermove_owner_agg;

ALTER TABLE hv_orc_tt_vmcontainermove_owner_agg_temp RENAME TO hv_orc_tt_vmcontainermove_owner_agg;