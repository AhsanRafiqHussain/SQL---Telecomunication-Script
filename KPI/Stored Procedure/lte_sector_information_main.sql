CREATE OR REPLACE FUNCTION lte_sector_information_main(_date timestamp without time zone)
  RETURNS Text As 
	$BODY$
DECLARE 
message1 TEXT;

count_pm INTEGER;
v_qry9 text;
v_qry10 text;
v_qry11 text;
v_qry12 text;
v_qry13 text;
v_qry14 text;
v_qry15 text;
v_qry16 text;
   
v_closed smallint default 0;
   
BEGIN

	count_pm:= (SELECT count(*)  FROM huawei_pm_4g_algo_cell where begintime = _date);		

IF count_pm = 0 THEN 
message1 := 'lte KPI not calculated. PM records for selected date does not exist.';
ELSE IF count_pm > 0 THEN

--truncate lte_temp_information;
--select lte_sector_information(_date) into message1;

    
v_qry9 := 'Select lte_kpi_abnorm_rel_erab_init_enb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_cs_rlc_pdu_srb_maxt_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_cs_trig_mme_rel_cause_e_utran_gen_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_due_to_faults_at_the_radio_nw_layer_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_due_to_faults_at_the_transp_nw_layer_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_due_to_handover_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_due_to_nw_congestion_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_due_to_ul_resynchronization_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_faults_at_the_transp_nw_layer_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_faults_radio_nw_layer_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_handover_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_no_responses_from_the_ue_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_radio_nw_conges_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_radio_resource_overload_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_radio_resource_preemption_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_transp_resource_overload_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_transp_resource_preemption_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_ul_resynchronization_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_cs_of_ul_weak_coverage_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_for_services_with_the_qci_of_1_in_a_cell_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_init_by_the_enodeb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_init_enodeb_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 9 SPs %',v_qry9;

v_qry10 := 'Select lte_kpi_abnorm_rel_e_rabs_of_no_responses_from_the_ue_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_of_radio_resource_overload_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_of_radio_resource_preemption_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_of_transp_resource_overload_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_of_transp_resource_preemption_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_rlc_pdu_drb_maxt_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_rlc_pdu_srb_maxt_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_abnorm_rel_e_rabs_trig_mme_rel_cause_e_utran_gen_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rate_ps_service_accessbility_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rrc_service_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rrc_signaling_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rrc_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_s1_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_alarm_count_nb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_alarm_count_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_average_number_of_cell_edge_users_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_average_ta_distance_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_call_drop_rate_with_mme_release_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cell_availability_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cell_downlink_average_throughput_kbps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cell_max_throughput_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cell_max_throughput_ul_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cell_unavailtime_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 10 SPs %', v_qry10;

v_qry11 := 'Select lte_kpi_cell_uplink_average_throughput_kbps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_0_to_6_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_10_to_15_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_4g_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_7_to_9_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_accessibility_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_average_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_drops_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_retainability_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_csfb_preparation_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dcr_service_drop_call_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_den_calldrop_abnormalreleases_plus_normalreleases_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_discarded_paging_messages_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dl_prb_usage_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dl_thp_cqi_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dl_traffic_volume_gb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_down_time_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erabs_attemps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_drops_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_due_to_faults_at_the_transport_network_layer_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_faults_at_the_radio_network_layer_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_faults_in_the_epc_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 11 SPs %', v_qry11;

v_qry12 := 'Select lte_kpi_erab_failures_insufficient_radio_resources_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_no_responses_from_ues_in_a_cell_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_release_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_setup_sr_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_setup_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_succ_established_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_inter_rat_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_intrafreq_ho_att_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_intra_freq_ho_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_irat_4g_to_3g_events_sector_hourly('||QUOTE_LITERAL(_date) ||');
Select lte_kpi_irat_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_l2w_ho_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_l_cell_unavail_dur_sys_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_l_sfb_prepatt_none_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_maximum_number_of_cell_edge_users_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_average_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_average_ul_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_dl_0_to_9_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_dl_10_to_14_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_dl_10_to_16_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_dl_15_to_19_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_dl_25_to_28_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 12 SPs %', v_qry12;

v_qry13 := 'Select lte_kpi_mcs_dl_5_to_9_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_ul_10_to_14_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_ul_10_to_16_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_ul_15_to_19_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_ul_20_to_24_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_ul_25_to_28_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_ul_5_to_9_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mme_calldrop_abnormalreleasesmme_plus_abnormalreleases_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mme_trig_abnormal_e_rab_rels_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mme_trig_abnorm_rel_e_rabs_for_cs_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mme_trig_abnorm_rel_of_e_rabs_for_cs_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_paging_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_retainability_ps_service_retainability_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_attemps_service_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_attemps_signaling_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_attempts_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_connected_users_average_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_connected_users_max_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_connreq_att_total_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_setup_success_rate_service_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_setup_success_rate_signaling_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_success_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 13 SPs %', v_qry13;

v_qry14 := 'Select lte_kpi_rrc_success_service_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_success_signaling_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_s1_attemps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_s1_setup_success_rate_sector_hourly('||QUOTE_LITERAL(_date) ||');
Select lte_kpi_s1_success_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ta_distance_at_100_Percent_samples_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ta_distance_at_50_Percent_samples_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ta_distance_at_70_Percent_samples_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ta_distance_at_85_Percent_samples_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ta_distance_at_90_Percent_samples_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_thrp_time_dl_rmvlasttti_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_traffic_dl_no_padding_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_traffic_user_avg_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ul_interference_average_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ul_interference_maximum_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ul_prb_usage_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ul_thp_cqi_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ul_traffic_volume_gb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_unavailability_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_user_dl_avrg_throughput_kbps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_user_ul_avrg_throughput_kbps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rate_ps_service_accessbility_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 14 SPs %', v_qry14;

v_qry15 := 'Select lte_kpi_acc_rrc_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_call_drop_rate_with_mme_release_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_average_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dcr_service_drop_call_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_den_calldrop_abnormalreleases_plus_normalreleases_secto('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dl_traffic_volume_gb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erabs_attemps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_due_to_faults_at_the_transport_network_la('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_faults_at_the_radio_network_layer_sector_('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_insufficient_radio_resources_sector_hourl('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_no_responses_from_ues_in_a_cell_sector_ho('|| QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_release_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_succ_established_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_irat_4g_to_3g_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_average_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_paging_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_connreq_att_total_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_thrp_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rate_ps_service_accessbility_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_acc_rrc_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');';
--RAISE NOTICE 'Qry 15 SPs %',v_qry15;

v_qry16 := 'Select lte_kpi_call_drop_rate_with_mme_release_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_cqi_average_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dcr_service_drop_call_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_den_calldrop_abnormalreleases_plus_normalreleases_secto('||QUOTE_LITERAL(_date)||');
Select lte_kpi_dl_traffic_volume_gb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erabs_attemps_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_due_to_faults_at_the_transport_network_la('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_faults_at_the_radio_network_layer_sector_('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_insufficient_radio_resources_sector_hourl('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_no_responses_from_ues_in_a_cell_sector_ho('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_release_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_erab_succ_established_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_irat_4g_to_3g_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_mcs_average_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_paging_success_rate_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_connreq_att_total_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_rrc_failures_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_thrp_dl_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_ul_traffic_volume_gb_sector_hourly('||QUOTE_LITERAL(_date)||');
Select lte_kpi_user_dl_avrg_throughput_kbps_sector_hourly('||QUOTE_LITERAL(_date)||');'; 
--RAISE NOTICE 'Qry 16 SPs %', v_qry16;

   PERFORM dblink_connect('conn9','dbname=conure');
   PERFORM dblink_send_query('conn9',v_qry9);
   PERFORM dblink_connect('conn10','dbname=conure');
   PERFORM dblink_send_query('conn10',v_qry10);
   PERFORM dblink_connect('conn11','dbname=conure');
   PERFORM dblink_send_query('conn11',v_qry11);
   PERFORM dblink_connect('conn12','dbname=conure');
   PERFORM dblink_send_query('conn12',v_qry12);
   PERFORM dblink_connect('conn13','dbname=conure');
   PERFORM dblink_send_query('conn13',v_qry13);
   PERFORM dblink_connect('conn14','dbname=conure');
   PERFORM dblink_send_query('conn14',v_qry14);
   PERFORM dblink_connect('conn15','dbname=conure');
   PERFORM dblink_send_query('conn15',v_qry15);
   PERFORM dblink_connect('conn16','dbname=conure');
   PERFORM dblink_send_query('conn16',v_qry16);
    RAISE NOTICE 'Connection Creataed';
	RAISE NOTICE 'Query Start run on server';
    
     
     WHILE v_closed < 8 loop 
		v_closed := 0;
	--	RAISE NOTICE 'v_closed %', v_closed;
	   if check_conn_is_busy('conn9') = 0 then
          v_closed := v_closed + 1;
       end if;
       if check_conn_is_busy('conn10') = 0 then
          v_closed := v_closed + 1;
       end if;
	   if check_conn_is_busy('conn11') = 0 then
          v_closed := v_closed + 1;
       end if;
       if check_conn_is_busy('conn12') = 0 then
          v_closed := v_closed + 1;
       end if;
	   if check_conn_is_busy('conn13') = 0 then
          v_closed := v_closed + 1;
       end if;
	   if check_conn_is_busy('conn14') = 0 then
          v_closed := v_closed + 1;
       end if;
	   if check_conn_is_busy('conn15') = 0 then
          v_closed := v_closed + 1;
       end if;
	   if check_conn_is_busy('conn16') = 0 then
          v_closed := v_closed + 1;
       end if;
	  -- RAISE NOTICE 'v_closed %', v_closed;
     END LOOP;
	RAISE NOTICE 'All Query completed';
   
     PERFORM dblink_disconnect('conn9');
     PERFORM dblink_disconnect('conn10');
	 PERFORM dblink_disconnect('conn11');
     PERFORM dblink_disconnect('conn12');
	 PERFORM dblink_disconnect('conn13');
     PERFORM dblink_disconnect('conn14');
	 PERFORM dblink_disconnect('conn15');
     PERFORM dblink_disconnect('conn16');
	 RAISE NOTICE 'Connection Diconnected';
   
--truncate lte_temp_information;

END IF;
END IF;

RETURN  'Inserted in information: '|| message1||'.';
		   
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION lte_sector_information_main(timestamp without time zone)
  OWNER TO postgres;

/*CREATE OR REPLACE FUNCTION check_conn_is_busy(conn text) RETURNS INT AS $$
DECLARE
  v int;
BEGIN
   SELECT dblink_is_busy(conn) INTO v;
   RETURN v;
END;
$$
language 'plpgsql';*/