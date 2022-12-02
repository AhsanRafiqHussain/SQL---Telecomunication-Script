CREATE OR REPLACE FUNCTION lte_kpi_ul_traffic_volume_gb_sector_hourly(_date timestamp without time zone)
  RETURNS text AS
$BODY$

DECLARE 
int_kpi INT;
message TEXT;

count_kpi INTEGER;


BEGIN

	count_kpi := (SELECT count(*) 
		FROM lte_kpi_ul_traffic_volume_gb_sector_hourly
		where begin_time = _date) t;
				

IF count_kpi > 0 THEN 
message := 'lte KPIs Tables contain the latest records for Datetime ' ||_date || '.';	
ELSE IF count_kpi = 0  THEN
	---------------------------------------------------------------------
 --1- Sector 
	---------------------------------------------------------------------
	
	insert into lte_kpi_ul_traffic_volume_gb_sector_hourly("lte_sector_information_id","begin_time","end_time","years","months","weeks","days","hours","minutes","ul_traffic_volume_gb")
	Select 
		( select id from lte_sector_information b 
			WHERE b.region_db_id  = (case when (lte_pm.region is null) 
									then (select region_db_id from lte_region where region is null) 
									else (select region_db_id from lte_region where region = lte_pm.region) end)
			and b.market_db_id =  	(case when (lte_pm.market is null) 
									then (select market_db_id from lte_market where market is null) 
									else (select market_db_id from lte_market where market = lte_pm.market)end )
			and b.zone_db_id = 		(case when(lte_pm.zone is null) 
									then (select zone_db_id from lte_zone where zone is null) 
									else (select zone_db_id from lte_zone where zone = lte_pm.zone) end)
			and b.tac_db_id = 		(case when(lte_pm.tac_id is null) 
									then (select tac_db_id from lte_tac where tac_id is null) 
									else (select tac_db_id from lte_tac where tac_id = lte_pm.tac_id ) end)
			and b.enodeb_db_id = 	(case when(lte_pm.enodeb is null) 
									then (select enodeb_db_id from lte_enodeb where enodeb is null) 
									else (select enodeb_db_id from lte_enodeb where enodeb_id = lte_pm.enodeb_id and enodeb = lte_pm.enodeb) end)
			and b.sector_db_id = 	(case when(lte_pm.cell_name is null) 
									then (select sector_db_id from lte_sector where sector is null) 
									else (select sector_db_id from lte_sector where sector_id = lte_pm.local_cell_id and sector = lte_pm.cell_name) end))
	,lte_pm.begin_time
	,lte_pm.end_time
	,lte_pm.years
	,lte_pm.months
	,lte_pm.weeks
	,lte_pm.days
	,lte_pm.hours
	,lte_pm.minutes
	,roundf(((l_thrp_bits_ul_qci_1 + l_thrp_bits_ul_qci_2 + l_thrp_bits_ul_qci_3 + l_thrp_bits_ul_qci_4 + l_thrp_bits_ul_qci_5 + l_thrp_bits_ul_qci_6 + l_thrp_bits_ul_qci_7 + l_thrp_bits_ul_qci_8 + l_thrp_bits_ul_qci_9) / 8388608000::BIGINT), 2) AS ul_traffic_volume_gb 

	FROM lte_temp_information lte_pm
	inner join huawei_pm_4g_traffic_thruput_cell a ON lte_pm.local_cell_id = a.local_cell_id
	AND lte_pm.cell_name = a.cell_name
	AND lte_pm.enodeb = a.enodeb_function_name
	AND a.begintime = _date;
	GET DIAGNOSTICS int_kpi = ROW_COUNT;	

message := 'lte KPI ul_traffic_volume_gb calculated. '||int_kpi || ' rows are updated for Datetime ' || _date ||'.';
END IF;
END IF;

RETURN message;
		
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION lte_kpi_ul_traffic_volume_gb_sector_hourly(timestamp without time zone)
  OWNER TO postgres;
