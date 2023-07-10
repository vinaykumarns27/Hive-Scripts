import os
import sys

try:
    from pyspark import SparkConf, SparkContext, StorageLevel
    from pyspark.sql.functions import broadcast, col, greatest, isnull, least, lit, max, round, unix_timestamp, when
    from pyspark.sql import Row, SparkSession
    from pyspark.sql.types import DateType, NullType

    numOfPartitions = 80
    sparkSess = SparkSession.builder \
        .appName("ocm_base.py") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.sql.shuffle.partition", 80) \
        .config("spark.default.parallelism", 80) \
        .getOrCreate()
    sparkCtx = sparkSess.sparkContext

    sparkCtx.addPyFile("/opt/gtnexus/spark/gtnexus_utils.py")
    # MUST import; otherwise, it UDFs cannot be found
    import gtnexus_utils
    startdate = gtnexus_utils.getThreeYearStartDateFromTable(sparkSess)
    enddate = gtnexus_utils.getThreeYearEndDateFromTable(sparkSess)
    #List of existing ocean events in tz_vmeventtype table. If more events need to be tracked simply add the vmeventtype_id to this
    #list. Container move events are filtered on this list.
    ocean_events = ["40", "49", "52", "91"]
    ###########################################################################
    ## RDD
    ###########################################################################
    orgRDD = sparkCtx.textFile("/apps/hive/warehouse/dimension_common.db/hv_ext_extended_org", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(org_id = x[0], orgname = x[1], orgtype = x[3], carriergroup = x[4], carriercluster = x[5]))
    cityCountryRegionRDD = sparkCtx.textFile("/apps/hive/warehouse/dimension_common.db/hv_tt_city_country_region") \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(city_id = x[0], cityname = x[1], country_id = x[2],iata = x[3], longitude = x[4], latitude = x[5], unlocode = x[6], countryname = x[7], countrycode = x[8], region1 = x[9], region2 = x[10], subdivision=x[11], citycluster1 = x[12], citycluster2 = x[13]))
    threeplCustomerRDD = sparkCtx.textFile("/apps/hive/warehouse/dimension_common.db/hv_tt_threepl_customer", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(ownerorg_id = x[0], customerorg_id = x[2]))
    dateRDD = sparkCtx.textFile("/dimension/common/tt_dw_date_dim", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(the_date_2 = x[2], the_year = x[4], the_month = x[5], the_month_name = x[6], the_quarter = x[7], the_quarter_name = x[8], the_week = x[9], the_week_name = x[10], the_year_month = x[11], the_year_quarter = x[13], the_year_week = x[15]))
    containerTypeRDD = sparkCtx.textFile("/apps/hive/warehouse/dimension_common.db/hv_ext_extended_containertype", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(containertype_id = x[0], containertype = x[1], teu = x[2], maxweightkg = x[3], maxvolume = x[4], containertypecluster = x[5]))
    moveTypeRDD = sparkCtx.textFile("/apps/hive/warehouse/dimension_common.db/hv_ext_extended_movetype", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(movetype_id = x[0], movetype = x[1], movetypecode = x[2], movetypecluster = x[3]))
    unitConvRDD = sparkCtx.textFile("/dimension/common/tz_unitconv", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(fromunit_id = x[1], tounit_id = x[2], rate = x[3]))
    distanceRDD = sparkCtx.textFile("/apps/hive/warehouse/ocean_container_common.db/hv_tt_port_distance_distinct", use_unicode = True) \
        .map(lambda x: x.split("\001"))
    ocmRDD = sparkCtx.textFile("/ocm/raw/tt_vmcontainermove", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .filter(lambda x: x[2] == "1") \
        .filter(lambda x: x[16] >= startdate and x[16] < enddate)
    rawPartyRDD = sparkCtx.textFile("/ocm/raw/tt_vmcontainermoveparty", use_unicode = True) \
        .map(lambda x: x.split("\001")) \
        .map(lambda x: Row(vmcontainermove_id = x[2], org_id = x[4]))
    #Note: Currently filtering out all events which are not included in ocean_events above. GTN-33231 only requirement for ocean events from tt_vmcontinaermoveevent are the 'processed' dates thus filtering out all NULL createtimes.
    ocmEventRDD = sparkCtx.textFile("/ocm/raw/tt_vmcontainermoveevent", use_unicode = True) \
        .map(lambda x : x.split("\001")) \
        .filter(lambda x : x[8] >= startdate and x[8] <= enddate) \
        .filter(lambda x : x[5] != "null") \
        .filter(lambda x : x[2] in ocean_events)
    # schema
    distanceSchema = gtnexus_utils.getSchemaFromString("""origcity_id,destcity_id,distance_in_km""",",")
    ocmSchema = gtnexus_utils.getSchemaFromString("""vmcontainermove_id,etl_modtime,status,carrierorg_id,polcity_id,podcity_id,origcity_id,destcity_id,containertype,origmovetype,destmovetype,containernum,etdpol,etapod,atdpol,atapod,btdpol,btapod,departpoltime,arrivepodtime,fullingateoceantime,fulloutgateoceantime,onboardpoltime,fullcontainerdischargetime,booketdpol,booketapod,onraildesttime,offrailorigtime,teu,weight,weightunit_id,volume,volumeunit_id,capacityweight,capacityweightunit_id,capacityvolume,capacityvolumeunit_id,booketdrec,booketadel,etdorig,etadest,atdorig,atadest,firsteta,prev_etapod,firstetd,prev_etdpol,fullcontainerreceivedrectime,fullcontainerdeliverytime,emptycontainerreturntime,emptyoutgateoceantime,emptyoutgateinlandtime,transshipportcity_id,createtime,appttime,arrivetransshiptime,blisubmitted,blprocessed,bookacceptedtime,bookcancelledtime,bookcutofftime,carrierreleasedtime,constrplacetime,containeravailabletime,customsreleasedtime,departrectime,departtransshiptime,departuretime,destarrivaltime,fullcontainerdischargedtransshiptime,fullingatedesttime,fullingateinlandtime,fullingateorigtime,fulloutgatedesttime,fulloutgateinlandtime,fulloutgateorigtime,intermodalinterchangetime,intransitarrivaltime,junctiondeliverytime,junctionreceivedtime,motorcarrierarrivaltime,motorcarrierdeparturetime,offraildesttime,onrailorigtime,railarrived,railarriveddesttime,railarrivedorigtime,raildepartdesttime,raildepartorigtime,railderampedtime,railrampedtime,releasedtime,shipmentdelaytime""", ",")
    ocmEventSchema = gtnexus_utils.getSchemaFromString("""vmcontainermoveevent_id,vmcontainermove_id,vmeventtype_id,vmevent_id,eventcode,createtime,eventtimeutc,org_id,etl_modtime,isobsolete""", ",")
    ###########################################################################
    ## DataFrame
    ###########################################################################
    orgDF = sparkSess.createDataFrame(orgRDD).cache()
    cityCountryRegionDF = sparkSess.createDataFrame(cityCountryRegionRDD).cache()
    threeplCustomerDF = sparkSess.createDataFrame(threeplCustomerRDD).cache()
    dateDF = sparkSess.createDataFrame(dateRDD).cache()
    containerTypeDF = sparkSess.createDataFrame(containerTypeRDD).cache()
    moveTypeDF = sparkSess.createDataFrame(moveTypeRDD).cache()
    unitConvDF = sparkSess.createDataFrame(unitConvRDD).cache()
    distanceDF = sparkSess.createDataFrame(distanceRDD, distanceSchema).cache()
    ocmDF = sparkSess.createDataFrame(ocmRDD, ocmSchema).coalesce(numOfPartitions)
    latestPartyDF = sparkSess.createDataFrame(rawPartyRDD).coalesce(numOfPartitions).persist(StorageLevel.MEMORY_AND_DISK)
    ocmEventDF = sparkSess.createDataFrame(ocmEventRDD, ocmEventSchema).coalesce(numOfPartitions)
    ###########################################################################
    ## Normalize NULL like values
    ###########################################################################
    ocmDF = gtnexus_utils.nullifyDFNullValue(ocmDF).coalesce(numOfPartitions)
    ocmEventDF = gtnexus_utils.nullifyDFNullValue(ocmEventDF).coalesce(numOfPartitions)
    ###########################################################################
    ## Code
    ###########################################################################

    shipperPartyDF = latestPartyDF \
        .join(broadcast(orgDF.filter("orgtype = 1")), orgDF.org_id == latestPartyDF.org_id) \
        .select(
            latestPartyDF.vmcontainermove_id,
            (orgDF.org_id).alias("ownerorg_id"),
            (orgDF.orgname).alias("ownerorgname"))

    threeplPartyDF = latestPartyDF \
        .join(broadcast(orgDF.filter("orgtype = 4")), orgDF.org_id == latestPartyDF.org_id) \
        .select(
            latestPartyDF.vmcontainermove_id,
            (orgDF.org_id).alias("ownerorg_id"),
            (orgDF.orgname).alias("ownerorgname"))

    partyDF = threeplPartyDF \
        .join(shipperPartyDF, shipperPartyDF.vmcontainermove_id == threeplPartyDF.vmcontainermove_id, "outer") \
        .join(broadcast(threeplCustomerDF), (threeplCustomerDF.ownerorg_id == threeplPartyDF.ownerorg_id) & (threeplCustomerDF.customerorg_id == shipperPartyDF.ownerorg_id)) \
        .select(
            threeplPartyDF.vmcontainermove_id,
            threeplPartyDF.ownerorg_id,
            threeplPartyDF.ownerorgname,
            (shipperPartyDF.ownerorg_id).alias("customerorg_id"),
            (shipperPartyDF.ownerorgname).alias("customerorgname")) \
        .union(shipperPartyDF.select(
            shipperPartyDF.vmcontainermove_id,
            shipperPartyDF.ownerorg_id,
            shipperPartyDF.ownerorgname,
            lit(0).alias("customerorg_id"),
            lit(None).cast(NullType()).alias("customerorgname")))

    origMoveTypeDF = moveTypeDF.alias("origMoveTypeDF") \
        .withColumnRenamed("movetype_id", "originmovetype_id") \
        .withColumnRenamed("movetype", "originmovetype") \
        .withColumnRenamed("movetypecode", "originmovetypecode") \
        .withColumnRenamed("movetypecluster", "originmovetypecluster")

    destMoveTypeDF = moveTypeDF.alias("destMoveTypeDF") \
        .withColumnRenamed("movetype_id", "destinationmovetype_id") \
        .withColumnRenamed("movetype", "destinationmovetype") \
        .withColumnRenamed("movetypecode", "destinationmovetypecode") \
        .withColumnRenamed("movetypecluster", "destinationmovetypecluster")

    origLocDF = cityCountryRegionDF.alias("origLocDF") \
        .withColumnRenamed("city_id", "origcity_id") \
        .withColumnRenamed("cityname", "origcityname") \
        .withColumnRenamed("iata", "origcityiata") \
        .withColumnRenamed("unlocode", "origcityunlocode") \
        .withColumnRenamed("longitude", "origcitylongitude") \
        .withColumnRenamed("latitude", "origcitylatitude") \
        .withColumnRenamed("country_id", "origcountry_id") \
        .withColumnRenamed("countryname", "origcountryname") \
        .withColumnRenamed("countrycode", "origcountrycode") \
        .withColumnRenamed("region1", "origregion1") \
        .withColumnRenamed("region2", "origregion2")

    destLocDF = cityCountryRegionDF.alias("destLocDF") \
        .withColumnRenamed("city_id", "destcity_id") \
        .withColumnRenamed("cityname", "destcityname") \
        .withColumnRenamed("iata", "destcityiata") \
        .withColumnRenamed("unlocode", "destcityunlocode") \
        .withColumnRenamed("longitude", "destcitylongitude") \
        .withColumnRenamed("latitude", "destcitylatitude") \
        .withColumnRenamed("country_id", "destcountry_id") \
        .withColumnRenamed("countryname", "destcountryname") \
        .withColumnRenamed("countrycode", "destcountrycode") \
        .withColumnRenamed("region1", "destregion1") \
        .withColumnRenamed("region2", "destregion2")

    polLocDF = cityCountryRegionDF.alias("polLocDF") \
        .withColumnRenamed("city_id", "polcity_id") \
        .withColumnRenamed("cityname", "polcityname") \
        .withColumnRenamed("iata", "polcityiata") \
        .withColumnRenamed("unlocode", "polcityunlocode") \
        .withColumnRenamed("longitude", "polcitylongitude") \
        .withColumnRenamed("latitude", "polcitylatitude") \
        .withColumnRenamed("subdivision", "polcitysubdivision") \
        .withColumnRenamed("country_id", "polcountry_id") \
        .withColumnRenamed("countryname", "polcountryname") \
        .withColumnRenamed("countrycode", "polcountrycode") \
        .withColumnRenamed("region1", "polregion1") \
        .withColumnRenamed("region2", "polregion2") \
        .withColumnRenamed("citycluster1", "polcitycluster1") \
        .withColumnRenamed("citycluster2", "polcitycluster2")

    podLocDF = cityCountryRegionDF.alias("podLocDF") \
        .withColumnRenamed("city_id", "podcity_id") \
        .withColumnRenamed("cityname", "podcityname") \
        .withColumnRenamed("iata", "podcityiata") \
        .withColumnRenamed("unlocode", "podcityunlocode") \
        .withColumnRenamed("longitude", "podcitylongitude") \
        .withColumnRenamed("latitude", "podcitylatitude") \
        .withColumnRenamed("subdivision", "podcitysubdivision") \
        .withColumnRenamed("country_id", "podcountry_id") \
        .withColumnRenamed("countryname", "podcountryname") \
        .withColumnRenamed("countrycode", "podcountrycode") \
        .withColumnRenamed("region1", "podregion1") \
        .withColumnRenamed("region2", "podregion2") \
        .withColumnRenamed("citycluster1", "podcitycluster1") \
        .withColumnRenamed("citycluster2", "podcitycluster2")

    transshipLocDF = cityCountryRegionDF.alias("transshipLocDF") \
        .withColumnRenamed("city_id", "transshipcity_id") \
        .withColumnRenamed("cityname", "transshipcityname") \
        .withColumnRenamed("iata", "transshipcityiata") \
        .withColumnRenamed("unlocode", "transshipcityunlocode") \
        .withColumnRenamed("longitude", "transshipcitylongitude") \
        .withColumnRenamed("latitude", "transshipcitylatitude") \
        .withColumnRenamed("subdivision", "transshipcitysubdivision") \
        .withColumnRenamed("country_id", "transshipcountry_id") \
        .withColumnRenamed("countryname", "transshipcountryname") \
        .withColumnRenamed("countrycode", "transshipcountrycode") \
        .withColumnRenamed("region1", "transshipregion1") \
        .withColumnRenamed("region2", "transshipregion2") \
        .withColumnRenamed("citycluster1", "transshipcitycluster1") \
        .withColumnRenamed("citycluster2", "transshipcitycluster2")

    weightConvDF = unitConvDF.alias("weightConvDF").filter("tounit_id = 33") \
        .drop("tounit_id") \
        .withColumnRenamed("fromunit_id", "weightunit_id") \
        .withColumnRenamed("rate", "weightrate")

    volumeConvDF = unitConvDF.alias("volumeConvDF").filter("tounit_id = 81") \
        .drop("tounit_id") \
        .withColumnRenamed("fromunit_id", "volumeunit_id") \
        .withColumnRenamed("rate", "volumerate")

    capacityWeightConvDF = unitConvDF.alias("capacityWeightConvDF").filter("tounit_id = 33") \
        .drop("tounit_id") \
        .withColumnRenamed("fromunit_id", "capacityweightunit_id") \
        .withColumnRenamed("rate", "capacityweightrate")

    capacityVolumeConvDF = unitConvDF.alias("capacityVolumeConvDF").filter("tounit_id = 81") \
        .drop("tounit_id") \
        .withColumnRenamed("fromunit_id", "capacityvolumeunit_id") \
        .withColumnRenamed("rate", "capacityvolumerate")

    ocmEventPivotNoAliasDF = ocmEventDF.groupBy("vmcontainermove_id").pivot("vmeventtype_id", ocean_events).agg(max("createtime")).alias("ocmEventPivotNoAliasDF").coalesce(numOfPartitions)

    ocmEventPivotDF = ocmEventPivotNoAliasDF.withColumnRenamed("vmcontainermove_id", "vmcm_id") \
                      .withColumnRenamed("40", "etdpol_processed") \
                      .withColumnRenamed("49", "booketdrec_processed") \
                      .withColumnRenamed("52", "fullcontainerrec_processed") \
                      .withColumnRenamed("91", "etdorigin_processed") \
                      .coalesce(numOfPartitions) \
                      .persist(StorageLevel.MEMORY_AND_DISK)

    ocmEventPivotDF.write.csv("/ocm/raw/hv_ext_tt_ocmevent_pivot", mode = "overwrite", sep = "\001", nullValue = '')

    ocmDF = ocmDF.withColumn("transshipportcity_id", when(isnull(ocmDF.transshipportcity_id), 0).otherwise(ocmDF.transshipportcity_id)).coalesce(numOfPartitions)

    ocmNetworkDF = ocmDF \
        .join(ocmEventPivotDF, ocmDF.vmcontainermove_id == ocmEventPivotDF.vmcm_id, 'left_outer') \
        .join(broadcast(orgDF), orgDF.org_id == ocmDF.carrierorg_id, "left_outer") \
        .join(broadcast(containerTypeDF), containerTypeDF.containertype_id == ocmDF.containertype, "left_outer") \
        .join(broadcast(origMoveTypeDF), origMoveTypeDF.originmovetype_id == ocmDF.origmovetype, "left_outer") \
        .join(broadcast(destMoveTypeDF), destMoveTypeDF.destinationmovetype_id == ocmDF.destmovetype, "left_outer") \
        .join(broadcast(moveTypeDF), moveTypeDF.movetype_id == (ocmDF.origmovetype * 256 + ocmDF.destmovetype), "left_outer") \
        .join(broadcast(distanceDF), (distanceDF.origcity_id == ocmDF.polcity_id) & (distanceDF.destcity_id == ocmDF.podcity_id), "left_outer") \
        .join(broadcast(weightConvDF), weightConvDF.weightunit_id == ocmDF.weightunit_id, "left_outer") \
        .join(broadcast(volumeConvDF), volumeConvDF.volumeunit_id == ocmDF.volumeunit_id, "left_outer") \
        .join(broadcast(capacityWeightConvDF), capacityWeightConvDF.capacityweightunit_id == ocmDF.capacityweightunit_id, "left_outer") \
        .join(broadcast(capacityVolumeConvDF), capacityVolumeConvDF.capacityvolumeunit_id == ocmDF.capacityvolumeunit_id, "left_outer") \
        .join(broadcast(dateDF), dateDF.the_date_2 == ocmDF.btdpol.cast(DateType()), "left_outer") \
        .join(broadcast(polLocDF), polLocDF.polcity_id == ocmDF.polcity_id, "left_outer") \
        .join(broadcast(podLocDF), podLocDF.podcity_id == ocmDF.podcity_id, "left_outer") \
        .join(broadcast(origLocDF), origLocDF.origcity_id == ocmDF.origcity_id, "left_outer") \
        .join(broadcast(destLocDF), destLocDF.destcity_id == ocmDF.destcity_id, "left_outer") \
        .join(broadcast(transshipLocDF), transshipLocDF.transshipcity_id == ocmDF.transshipportcity_id, "left_outer") \
        .select(
            ocmDF.vmcontainermove_id,
            ocmDF.carrierorg_id,
            (orgDF.orgname).alias("carrierorgname"),
            orgDF.carriergroup,
            orgDF.carriercluster,
            ocmDF.polcity_id,
            polLocDF.polcityname,
            polLocDF.polcityiata,
            polLocDF.polcityunlocode,
            polLocDF.polcitylongitude,
            polLocDF.polcitylatitude,
            polLocDF.polcitysubdivision,
            polLocDF.polcitycluster1,
            polLocDF.polcitycluster2,
            polLocDF.polcountry_id,
            polLocDF.polcountryname,
            polLocDF.polcountrycode,
            polLocDF.polregion1,
            polLocDF.polregion2,
            ocmDF.podcity_id,
            podLocDF.podcityname,
            podLocDF.podcityiata,
            podLocDF.podcityunlocode,
            podLocDF.podcitylongitude,
            podLocDF.podcitylatitude,
            podLocDF.podcitysubdivision,
            podLocDF.podcitycluster1,
            podLocDF.podcitycluster2,
            podLocDF.podcountry_id,
            podLocDF.podcountryname,
            podLocDF.podcountrycode,
            podLocDF.podregion1,
            podLocDF.podregion2,
            ocmDF.origcity_id,
            origLocDF.origcityname,
            origLocDF.origcityiata,
            origLocDF.origcityunlocode,
            origLocDF.origcitylongitude,
            origLocDF.origcitylatitude,
            origLocDF.origcountry_id,
            origLocDF.origcountryname,
            origLocDF.origcountrycode,
            origLocDF.origregion1,
            origLocDF.origregion2,
            ocmDF.destcity_id,
            destLocDF.destcityname,
            destLocDF.destcityiata,
            destLocDF.destcityunlocode,
            destLocDF.destcitylongitude,
            destLocDF.destcitylatitude,
            destLocDF.destcountry_id,
            destLocDF.destcountryname,
            destLocDF.destcountrycode,
            destLocDF.destregion1,
            destLocDF.destregion2,
            ocmDF.transshipportcity_id,
            transshipLocDF.transshipcityname,
            transshipLocDF.transshipcityiata,
            transshipLocDF.transshipcityunlocode,
            transshipLocDF.transshipcitylongitude,
            transshipLocDF.transshipcitylatitude,
            transshipLocDF.transshipcitysubdivision,
            transshipLocDF.transshipcitycluster1,
            transshipLocDF.transshipcitycluster2,
            transshipLocDF.transshipcountry_id,
            transshipLocDF.transshipcountryname,
            transshipLocDF.transshipcountrycode,
            transshipLocDF.transshipregion1,
            transshipLocDF.transshipregion2,
            ocmDF.containertype.alias("containertype_id"),
            containerTypeDF.containertype,
            containerTypeDF.containertypecluster,
            origMoveTypeDF.originmovetype_id,
            origMoveTypeDF.originmovetype,
            origMoveTypeDF.originmovetypecode,
            origMoveTypeDF.originmovetypecluster,
            destMoveTypeDF.destinationmovetype_id,
            destMoveTypeDF.destinationmovetype,
            destMoveTypeDF.destinationmovetypecode,
            destMoveTypeDF.destinationmovetypecluster,
            moveTypeDF.movetype_id,
            moveTypeDF.movetype,
            moveTypeDF.movetypecode,
            moveTypeDF.movetypecluster,
            ocmDF.containernum,
            ocmDF.createtime,
            ocmDF.etdpol,
            ocmDF.etapod,
            ocmDF.atdpol,
            ocmDF.atapod,
            ocmDF.btdpol,
            ocmDF.btapod,
            ocmDF.booketdpol,
            ocmDF.booketapod,
            ocmDF.firstetd,
            ocmDF.firsteta,
            ocmDF.emptyoutgateoceantime,
            ocmDF.emptyoutgateinlandtime,
            ocmDF.offrailorigtime,
            ocmDF.fullingateoceantime,
            ocmDF.onboardpoltime,
            ocmDF.departpoltime,
            ocmDF.arrivetransshiptime,
            ocmDF.departtransshiptime,
            ocmDF.arrivepodtime,
            ocmDF.fullcontainerdischargetime,
            ocmDF.customsreleasedtime,
            ocmDF.fulloutgateoceantime,
            ocmDF.onraildesttime,
            ocmDF.emptycontainerreturntime,
            greatest(ocmDF.emptyoutgateoceantime, ocmDF.emptyoutgateinlandtime).alias("emptyoutgateattr"),
            greatest(ocmDF.fullcontainerdeliverytime, ocmDF.fullingatedesttime, ocmDF.railarriveddesttime, ocmDF.fulloutgatedesttime).alias("fullcontainerdeliveryattr"),
            least(ocmDF.fulloutgateorigtime, ocmDF.onrailorigtime, ocmDF.raildepartorigtime, greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime)).alias("outforstuffingattr"),
            greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime).alias("yardinattr"),
            least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime).alias("yardoutattr"),
            dateDF.the_year,
            dateDF.the_year_quarter,
            dateDF.the_year_month,
            dateDF.the_year_week,
            dateDF.the_quarter,
            dateDF.the_quarter_name,
            dateDF.the_month,
            dateDF.the_month_name,
            dateDF.the_week,
            dateDF.the_week_name,
            distanceDF.distance_in_km,
            ocmDF.teu,
            ocmDF.weight,
            ocmDF.weightunit_id,
            (ocmDF.weight * weightConvDF.weightrate).alias("weight_kg"),
            ocmDF.volume,
            ocmDF.volumeunit_id,
            (ocmDF.volume * volumeConvDF.volumerate).alias("volume_cr"),
            ocmDF.capacityweight,
            ocmDF.capacityweightunit_id,
            (ocmDF.capacityweight * capacityWeightConvDF.capacityweightrate).alias("capacityweight_kg"),
            ocmDF.capacityvolume,
            ocmDF.capacityvolumeunit_id,
            (ocmDF.capacityvolume * capacityVolumeConvDF.capacityvolumerate).alias("capacityvolume_cr"),
            ocmEventPivotDF.etdpol_processed,
            ocmEventPivotDF.booketdrec_processed,
            ocmEventPivotDF.fullcontainerrec_processed,
            ocmEventPivotDF.etdorigin_processed,
            when(ocmDF.departpoltime == "null", 0).otherwise(ocmDF.teu).alias("polteu"),
            when(ocmDF.arrivepodtime == "null", 0).otherwise(ocmDF.teu).alias("podteu"),
            when(ocmDF.departpoltime == "null", 0).otherwise(1).alias("polcontainer"),
            when(ocmDF.arrivepodtime == "null", 0).otherwise(1).alias("podcontainer"),
            gtnexus_utils.getCycleTime(ocmDF.departpoltime, greatest(ocmDF.booketdpol, ocmDF.firstetd), 20).alias("poltimeliness"),
            gtnexus_utils.getCycleTime(ocmDF.arrivepodtime, least(ocmDF.booketapod, ocmDF.firsteta), 20).alias("podtimeliness"),
            gtnexus_utils.getCycleTime(ocmDF.booketdrec, ocmDF.fullcontainerreceivedrectime, 20).alias("fullcontainerrec_etdrec"),
            gtnexus_utils.getCycleTime(ocmEventPivotDF.booketdrec_processed, ocmEventPivotDF.fullcontainerrec_processed, 20).alias("fullcontainerrec_etdrec_processed"),
            gtnexus_utils.getCycleTime(ocmEventPivotDF.etdorigin_processed, ocmEventPivotDF.fullcontainerrec_processed, 20).alias("fullcontainerrec_etdorig_processed"),
            gtnexus_utils.getCycleTime(ocmDF.etdorig, ocmDF.fullcontainerreceivedrectime, 20).alias("fullcontainerrec_etdorig"),
            gtnexus_utils.getCycleTime(ocmDF.etdorig, ocmDF.atdorig, 20).alias("atd_etdorigin"),
            gtnexus_utils.getCycleTime(ocmDF.booketdpol, ocmDF.firstetd, 20).alias("first_booketdpol"),
            gtnexus_utils.getCycleTime(ocmDF.booketdpol, ocmDF.prev_etdpol, 20).alias("prev_booketdpol"),
            gtnexus_utils.getCycleTime(ocmDF.booketdpol, ocmDF.etdpol, 20).alias("etd_booketdpol"),
            gtnexus_utils.getCycleTime(ocmDF.booketdpol, ocmDF.atdpol, 20).alias("atd_booketdpol"),
            gtnexus_utils.getCycleTime(ocmDF.firstetd, ocmDF.prev_etdpol, 20).alias("prev_firstetdpol"),
            gtnexus_utils.getCycleTime(ocmDF.firstetd, ocmDF.atdpol, 20).alias("atd_firstetdpol"),
            gtnexus_utils.getCycleTime(ocmDF.prev_etdpol, ocmDF.etdpol, 20).alias("etd_prev_etdpol"),
            gtnexus_utils.getCycleTime(ocmDF.prev_etdpol, ocmDF.departpoltime, 20).alias("depart_prev_etdpol"),
            gtnexus_utils.getCycleTime(ocmDF.prev_etdpol, ocmDF.atdpol, 20).alias("atd_prev_etdpol"),
            gtnexus_utils.getCycleTime(ocmDF.departpoltime, ocmDF.atdpol, 20).alias("atd_departpoltime"),
            gtnexus_utils.getCycleTime(ocmDF.booketapod, ocmDF.firsteta, 20).alias("first_booketapod"),
            gtnexus_utils.getCycleTime(ocmDF.booketapod, ocmDF.prev_etapod, 20).alias("prev_booketapod"),
            gtnexus_utils.getCycleTime(ocmDF.booketapod, ocmDF.atapod, 20).alias("ata_booketapod"),
            gtnexus_utils.getCycleTime(ocmDF.firsteta, ocmDF.prev_etapod, 20).alias("prev_firsteta"),
            gtnexus_utils.getCycleTime(ocmDF.firsteta, ocmDF.atapod, 20).alias("ata_firsteta"),
            gtnexus_utils.getCycleTime(ocmDF.prev_etapod, ocmDF.etapod, 20).alias("eta_prev_etapod"),
            gtnexus_utils.getCycleTime(ocmDF.prev_etapod, ocmDF.arrivepodtime, 20).alias("arrived_prev_etapod"),
            gtnexus_utils.getCycleTime(ocmDF.prev_etapod, ocmDF.atapod, 20).alias("ata_prev_etapod"),
            gtnexus_utils.getCycleTime(ocmDF.arrivepodtime, ocmDF.atapod, 20).alias("ata_arrivepodtime"),
            gtnexus_utils.getCycleTime(ocmDF.booketadel, ocmDF.etadest, 20).alias("etadest_booketadel"),
            gtnexus_utils.getCycleTime(ocmDF.booketadel, ocmDF.fullcontainerdeliverytime, 20).alias("containerdelivery_booketadel"),
            gtnexus_utils.getCycleTime(ocmDF.booketadel, ocmDF.atadest, 20).alias("atadest_booketadel"),
            gtnexus_utils.getCycleTime(ocmDF.booketdpol, ocmDF.departpoltime, 20).alias("booking_etd_pol_accuracy"),
            gtnexus_utils.getCycleTime(ocmDF.firstetd, ocmDF.etdpol, 20).alias("first_etd_pol_variance"),
            gtnexus_utils.getCycleTime(ocmDF.firstetd, ocmDF.departpoltime, 20).alias("first_etd_pol_accuracy"),
            gtnexus_utils.getCycleTime(ocmDF.etdpol, ocmDF.departpoltime, 20).alias("etd_pol_accuracy"),
            gtnexus_utils.getCycleTime(ocmDF.etdpol, ocmDF.atdpol, 20).alias("departure_time_diff"),
            gtnexus_utils.getCycleTime(ocmDF.booketapod, ocmDF.etapod, 20).alias("book_eta_pod_variance"),
            gtnexus_utils.getCycleTime(ocmDF.booketapod, ocmDF.arrivepodtime, 20).alias("book_eta_pod_accuracy"),
            gtnexus_utils.getCycleTime(ocmDF.firsteta, ocmDF.arrivepodtime, 20).alias("first_eta_pod_accuracy"),
            gtnexus_utils.getCycleTime(ocmDF.firsteta, ocmDF.etapod, 20).alias("first_eta_pod_variance"),
            gtnexus_utils.getCycleTime(ocmDF.etapod, ocmDF.arrivepodtime, 20).alias("eta_pod_accuracy"),
            gtnexus_utils.getCycleTime(ocmDF.etapod, ocmDF.atapod, 20).alias("arrival_time_differene"),
            gtnexus_utils.getCycleTime(least(ocmDF.booketapod, ocmDF.firsteta), greatest(ocmDF.booketdpol, ocmDF.firstetd), 20).alias("vesseltransitestimate"),
            gtnexus_utils.getCycleTime(ocmDF.arrivepodtime, ocmDF.departpoltime, 60).alias("oceantransittime"),
            gtnexus_utils.getCycleTime(least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime), greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime), 60).alias("containertransittime"),
            gtnexus_utils.getCycleTime(ocmDF.emptycontainerreturntime, greatest(ocmDF.fullcontainerdeliverytime, ocmDF.fullingatedesttime, ocmDF.railarriveddesttime, ocmDF.fulloutgatedesttime, least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime)), 20).alias("outforstripping"),
            gtnexus_utils.getCycleTime(least(ocmDF.fulloutgateorigtime, ocmDF.onrailorigtime, ocmDF.raildepartorigtime, greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime)), greatest(ocmDF.emptyoutgateoceantime, ocmDF.emptyoutgateinlandtime), 20).alias("outforstuffing"),
            gtnexus_utils.getCycleTime(ocmDF.customsreleasedtime, greatest(ocmDF.fullcontainerdischargetime, ocmDF.arrivepodtime), 20).alias("customsreleased"),
            gtnexus_utils.getCycleTime(greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime), least(ocmDF.fulloutgateorigtime, ocmDF.onrailorigtime, ocmDF.raildepartorigtime), 20).alias("origininlandtransit"),
            gtnexus_utils.getCycleTime(greatest(ocmDF.fullcontainerdeliverytime, ocmDF.fullingatedesttime, ocmDF.railarriveddesttime, ocmDF.fulloutgatedesttime), least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime), 20).alias("destinationinlandtransit"),
            gtnexus_utils.getCycleTime(greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime), greatest(ocmDF.emptyoutgateoceantime, ocmDF.emptyoutgateinlandtime), 20).alias("dwellawayfrompol"),
            gtnexus_utils.getCycleTime(ocmDF.emptycontainerreturntime, least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime), 20).alias("dwellawayfrompod"),
            gtnexus_utils.getCycleTime(ocmDF.onboardpoltime, greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime), 20).alias("dwellatpol"),
            gtnexus_utils.getCycleTime(least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime), ocmDF.fullcontainerdischargetime, 20).alias("dwellatpod"),
            gtnexus_utils.getCycleTime(ocmDF.departpoltime, greatest(ocmDF.fullingateoceantime, ocmDF.offrailorigtime, ocmDF.railarrivedorigtime), 20).alias("totalpoldwell"),
            gtnexus_utils.getCycleTime(least(ocmDF.fulloutgateoceantime, ocmDF.onraildesttime, ocmDF.raildepartdesttime), ocmDF.arrivepodtime, 20).alias("totalpoddwell"),
            gtnexus_utils.getCycleTime(ocmDF.departtransshiptime, ocmDF.arrivetransshiptime, 20).alias("dwellattransship")
        ) \
        .coalesce(numOfPartitions) \
        .persist(StorageLevel.MEMORY_AND_DISK)

    ocmNetworkDF.write.csv("/community/ocm/hv_ext_tt_vmcontainermove_no_owner_base", mode = "overwrite", sep = "\001", nullValue = '')

    ocmOrgDF = ocmNetworkDF \
        .join(partyDF, partyDF.vmcontainermove_id == ocmNetworkDF.vmcontainermove_id) \
        .drop(partyDF.vmcontainermove_id) \
        .select(
            [partyDF.ownerorg_id,partyDF.ownerorgname,partyDF.customerorg_id,partyDF.customerorgname] + [col(x) for x in ocmNetworkDF.columns]
        ) \
        .coalesce(numOfPartitions)

    ocmOrgDF.write.csv("/community/ocm/hv_ext_tt_vmcontainermove_owner_base", mode = "overwrite", sep = "\001", nullValue = '')

    sparkSess.stop()
except ImportError as e:
    print("error importing spark modules", e)
    sys.exit(1)
