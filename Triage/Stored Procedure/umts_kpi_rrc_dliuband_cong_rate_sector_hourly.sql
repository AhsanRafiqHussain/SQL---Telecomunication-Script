CREATE OR REPLACE FUNCTION umts_kpi_rrc_dliuband_cong_rate_sector_hourly(_date timestamp without time zone)
  RETURNS text AS
$BODY$

DECLARE 
int_kpi INT;
message TEXT;

count_kpi INTEGER;


BEGIN

	count_kpi := (SELECT count(*) 
		FROM umts_kpi_rrc_dliuband_cong_rate_sector_hourly
		where begin_time = _date) t;
				

IF count_kpi > 0 THEN 
message := 'UMTS KPIs rrc_dliuband_cong_rate  Tables contain the latest records for Datetime ' ||_date || '.';	
ELSE IF count_kpi = 0  THEN
	---------------------------------------------------------------------
 --1- Sector 
	---------------------------------------------------------------------
	
	insert into umts_kpi_rrc_dliuband_cong_rate_sector_hourly("umts_sector_information_id","begin_time","end_time","years","months","weeks","days","hours","minutes","rrc_dliuband_cong_rate")
	Select 
		( select id from umts_sector_information b 
			WHERE b.region_db_id  = (case when (umts_pm.region is null) 
									then (select region_db_id from umts_region where region is null) 
									else (select region_db_id from umts_region where region = umts_pm.region) end)
			and b.market_db_id =  	(case when (umts_pm.market is null) 
									then (select market_db_id from umts_market where market is null) 
									else (select market_db_id from umts_market where market = umts_pm.market)end )
			and b.zone_db_id = 		(case when(umts_pm.zone is null) 
									then (select zone_db_id from umts_zone where zone is null) 
									else (select zone_db_id from umts_zone where zone = umts_pm.zone) end)
			and b.rnc_db_id = 		(case when(umts_pm.rnc is null) 
									then (select rnc_db_id from umts_rnc2 where rnc is null) 
									else (select rnc_db_id from umts_rnc2 where rnc_id = umts_pm.rnc_id  and rnc = umts_pm.rnc) end)
			and b.nodeb_db_id = 	(case when(umts_pm.nodeb is null) 
									then (select nodeb_db_id from umts_nodeb where nodeb is null) 
									else (select nodeb_db_id from umts_nodeb where nodeb_id = umts_pm.nodeb_id and nodeb = umts_pm.nodeb) end)
			and b.sector_db_id = 	(case when(umts_pm.cell is null) 
									then (select sector_db_id from umts_sector where sector is null) 
									else (select sector_db_id from umts_sector where sector_id = umts_pm.cell_id and sector = umts_pm.cell) end))
	,umts_pm.begin_time
	,umts_pm.end_time
	,umts_pm.years
	,umts_pm.months
	,umts_pm.weeks
	,umts_pm.days
	,umts_pm.hours
	,umts_pm.minutes
	,CASE 
		WHEN vs_rrc_attconnestab_sum = 0
			THEN 0
		ELSE (vs_rrc_rej_dliubband_cong / vs_rrc_attconnestab_sum) * 100
		END AS rrc_dliuband_cong_rate
	FROM umts_temp_information umts_pm
	inner join huawei_pm_3g_rrc_setupfail_cell a ON umts_pm.cell_id = a.cellid
	AND umts_pm.rnc = a.netype
	AND a.begintime = _date
	inner join huawei_pm_3g_rrc_setup_cell b ON umts_pm.cell_id = b.cellid
	AND umts_pm.rnc = b.netype
	AND b.begintime = _date;
	GET DIAGNOSTICS int_kpi = ROW_COUNT;
	

message := 'UMTS KPI calculated. '||int_kpi || ' rows are updated for Datetime ' || _date ||'.';
END IF;
END IF;

RETURN message;
		
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION umts_kpi_rrc_dliuband_cong_rate_sector_hourly(timestamp without time zone)
  OWNER TO postgres;
