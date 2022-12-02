CREATE OR REPLACE FUNCTION umts_kpi_rrc_attempts_sector_hourly(_date timestamp without time zone)
  RETURNS text AS
$BODY$

DECLARE 
int_kpi INT;
message TEXT;

count_kpi INTEGER;


BEGIN

	count_kpi := (SELECT count(*) 
		FROM umts_kpi_rrc_attempts_sector_hourly
		where begin_time = _date) t;
				

IF count_kpi > 0 THEN 
message := 'UMTS KPIs Tables contain the latest records for Datetime ' ||_date || '.';	
ELSE IF count_kpi = 0  THEN
	---------------------------------------------------------------------
 --1- Sector 
	---------------------------------------------------------------------
	
		
	insert into umts_kpi_rrc_attempts_sector_hourly("umts_sector_information_id","begin_time","end_time","years","months","weeks","days","hours","minutes","rrc_attempts" )
	Select 
		( select id from umts_sector_information b 
			WHERE b.region_db_id  = (case when (a.region is null) 
									then (select region_db_id from umts_region where region is null) 
									else (select region_db_id from umts_region where region = a.region) end)
			and b.market_db_id =  	(case when (a.market is null) 
									then (select market_db_id from umts_market where market is null) 
									else (select market_db_id from umts_market where market = a.market)end )
			and b.zone_db_id = 		(case when(a.zone is null) 
									then (select zone_db_id from umts_zone where zone is null) 
									else (select zone_db_id from umts_zone where zone = a.zone) end)
			and b.rnc_db_id = 		(case when( a.rnc is null) 
									then (select rnc_db_id from umts_rnc2 where rnc is null) 
									else (select rnc_db_id from umts_rnc2 where rnc_id = a.rnc_id  and rnc = a.rnc) end)
			and b.nodeb_db_id = 	(case when( a.nodeb is null) 
									then (select nodeb_db_id from umts_nodeb where nodeb is null) 
									else (select nodeb_db_id from umts_nodeb where nodeb_id = a.nodeb_id and nodeb = a.nodeb) end)
			and b.sector_db_id = 	(case when( a.cell is null) 
									then (select sector_db_id from umts_sector where sector is null) 
									else (select sector_db_id from umts_sector where sector_id = a.cell_id and sector = a.cell) end))
	,a.begin_time
	,a.end_time
	,a.years
	,a.months
	,a.weeks
	,a.days
	,a.hours
	,a.minutes
	,umts_t.vs_rrc_attconnestab_sum AS rrc_attempts
	FROM umts_temp_information a
	LEFT JOIN huawei_pm_3g_rrc_setup_cell umts_t ON a.cell_id = umts_t.cellid
	AND a.rnc = umts_t.netype
	AND umts_t.begintime = _date;
	GET DIAGNOSTICS int_kpi = ROW_COUNT;
	
message := 'UMTS KPI calculated. '||int_kpi || ' rows are updated for Datetime ' || _date ||'.';
END IF;
END IF;

RETURN message;
		
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION umts_kpi_rrc_attempts_sector_hourly(timestamp without time zone)
  OWNER TO postgres;
