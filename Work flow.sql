

1) sqoop_tt_vmcontainermove.sh
2) tt_vmcontainermove_dedup.ddl
3) ocm_base.py

exec_ocm.sh -- Sequence of the script execution : 

exec_sql /opt/gtnexus/hive/community/ocm/table/

hv_ext_tt_vmcontainermove_no_owner_base.ddl
hv_ext_tt_vmcontainermove_owner_base.ddl
hv_orc_tt_vmcontainermove_no_owner_base.ddl
hv_orc_tt_vmcontainermove_owner_base.ddl -- 1st old data, 

-- count of data for jan 2021, dec 2021 vmcontainermove_id's 
-- one record, column to column match. 

hql_ocm_base_to_orc.hql

hv_orc_tt_vmcontainermove_no_owner_agg.ddl
hv_orc_tt_vmcontainermove_owner_agg.ddl

hv_orc_arrivepod_no_owner_agg.ddl
hv_orc_arrivetransship_no_owner_agg.ddl
hv_orc_booketapod_no_owner_agg.ddl
hv_orc_booketdpol_no_owner_agg.ddl
hv_orc_btapod_no_owner_agg.ddl
hv_orc_btdpol_no_owner_agg.ddl
hv_orc_create_no_owner_agg.ddl
hv_orc_customsreleased_no_owner_agg.ddl
hv_orc_departpol_no_owner_agg.ddl
hv_orc_departtransship_no_owner_agg.ddl
hv_orc_emptycontainerreturn_no_owner_agg.ddl
hv_orc_emptyoutgate_no_owner_agg.ddl
hv_orc_fullcontainerdelivery_no_owner_agg.ddl
hv_orc_fullcontainerdischarge_no_owner_agg.ddl
hv_orc_fulloutgateocean_no_owner_agg.ddl
hv_orc_onboard_no_owner_agg.ddl
hv_orc_outforstuffing_no_owner_agg.ddl
hv_orc_yardin_no_owner_agg.ddl
hv_orc_yardout_no_owner_agg.ddl

hv_orc_arrivepod_owner_agg.ddl
hv_orc_arrivetransship_owner_agg.ddl
hv_orc_booketapod_owner_agg.ddl
hv_orc_booketdpol_owner_agg.ddl
hv_orc_btapod_owner_agg.ddl
hv_orc_btdpol_owner_agg.ddl
hv_orc_create_owner_agg.ddl
hv_orc_customsreleased_owner_agg.ddl
hv_orc_departpol_owner_agg.ddl
hv_orc_departtransship_owner_agg.ddl
hv_orc_emptycontainerreturn_owner_agg.ddl
hv_orc_emptyoutgate_owner_agg.ddl
hv_orc_fullcontainerdelivery_owner_agg.ddl
hv_orc_fullcontainerdischarge_owner_agg.ddl
hv_orc_fulloutgateocean_owner_agg.ddl
hv_orc_onboard_owner_agg.ddl
hv_orc_outforstuffing_owner_agg.ddl
hv_orc_yardin_owner_agg.ddl
hv_orc_yardout_owner_agg.ddl

hv_orc_ocm_benchmark_no_owner_agg.ddl
hv_orc_ocm_benchmark_owner_agg.ddl -- All the 
hv_orc_ocm_benchmark_combined_agg.ddl
	

