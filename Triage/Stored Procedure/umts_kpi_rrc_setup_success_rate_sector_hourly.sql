CREATE OR REPLACE FUNCTION umts_kpi_rrc_setup_success_rate_sector_hourly(_date timestamp without time zone)
  RETURNS text AS
$BODY$

DECLARE 
int_kpi INT;
_endtime time;
_begintime time;
message TEXT;

count_kpi INTEGER;


BEGIN

	count_kpi := (SELECT count(*) 
		FROM umts_kpi_rrc_setup_success_rate_sector_hourly
		where begin_time = _date) t;
				

IF count_kpi > 0 THEN 
message := 'UMTS KPIs Tables contain the latest records for Datetime ' ||_date || '.';	
ELSE IF count_kpi = 0  THEN
	---------------------------------------------------------------------
 --1- Sector 
	---------------------------------------------------------------------
	 
--huawei_pm_3g_rrc_setup_cell
drop table if exists df_huawei_pm_3g_rrc_setup_cell;
CREATE TEMP TABLE df_huawei_pm_3g_rrc_setup_cell ON
COMMIT PRESERVE ROWS AS
SELECT  
	cellid 
	,label
	,begintime
	,endtime
	,netype
	,rrc_attconnestab_orgconvcall 
	,rrc_attconnestab_tmconvcall 
	,rrc_attconnestab_emgcall 
	,rrc_succconnestab_orgconvcall 
	,rrc_succconnestab_tmconvcall 
	,rrc_succconnestab_emgcall 
FROM huawei_pm_3g_rrc_setup_cell
WHERE begintime = _date;

drop table if exists df_huawei_pm_3g_cellupdate_cell;
CREATE TEMP TABLE df_huawei_pm_3g_cellupdate_cell ON
COMMIT PRESERVE ROWS AS
SELECT DISTINCT cellid
	,label
	,begintime
	,endtime
	,netype
	,vs_attcellupdt_orgconvcall_pch
	,vs_attcellupdt_emgcall_pch
	,vs_attcellupdt_tmconvcall_pch
	,vs_succcellupdt_orgconvcall_pch
	,vs_succcellupdt_emgcall_pch
	,vs_succcellupdt_tmconvcall_pch
	,vs_attcellupdt_pagersp
	,vs_attcellupdt_uldatatrans
	,vs_succcellupdt_pagersp
	,vs_succcellupdt_uldatatrans
FROM huawei_pm_3g_cellupdate_cell
WHERE begintime = _date;

DROP TABLE IF EXISTS umts_z;
CREATE TEMP TABLE umts_z ON
COMMIT PRESERVE ROWS AS
SELECT 
	 umts_pm.cell_id
	,umts_pm.cell
	,umts_pm.nodeb_id
	,umts_pm.nodeb
	,umts_pm.rnc_id
	,umts_pm.rnc
	,umts_pm.zone
	,umts_pm.market
	,umts_pm.region
	,umts_pm.begin_time
	,umts_pm.end_time
	,umts_pm.years
	,umts_pm.months
	,umts_pm.weeks
	,umts_pm.days
	,umts_pm.hours
	,umts_pm.minutes
	,(rrc_attconnestab_orgconvcall + rrc_attconnestab_tmconvcall + rrc_attconnestab_emgcall + vs_attcellupdt_orgconvcall_pch + vs_attcellupdt_tmconvcall_pch + vs_attcellupdt_emgcall_pch) AS Acc_CS_ATT_RRC_Den
	,(rrc_succconnestab_orgconvcall + rrc_succconnestab_tmconvcall + rrc_succconnestab_emgcall + vs_succcellupdt_orgconvcall_pch + vs_succcellupdt_tmconvcall_pch + vs_succcellupdt_emgcall_pch) AS Acc_CS_SUCC_RRC_Num
	FROM umts_temp_information umts_pm
	LEFT JOIN df_huawei_pm_3g_rrc_setup_cell umts_t ON umts_pm.cell_id = umts_t.cellid
	AND umts_pm.rnc = umts_t.netype
	AND umts_t.begintime = _date
	LEFT JOIN df_huawei_pm_3g_cellupdate_cell umts_u ON umts_pm.cell_id = umts_u.cellid
	AND umts_pm.rnc = umts_u.netype
	AND umts_u.begintime = _date;
	
	drop table if exists df_huawei_pm_3g_rrc_setup_cell;
	drop table if exists df_huawei_pm_3g_cellupdate_cell;
		
	insert into umts_kpi_rrc_setup_success_rate_sector_hourly ("umts_sector_information_id","begin_time","end_time","years","months","weeks","days","hours","minutes","rrc_setup_success_rate" )
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
	,CASE 
		WHEN Acc_CS_ATT_RRC_Den = 0
			THEN 0
			ELSE roundf(((Acc_CS_SUCC_RRC_Num / Acc_CS_ATT_RRC_Den) * 100), 2)
	END AS rrc_setup_success_rate
	from umts_z a;
	GET DIAGNOSTICS int_kpi = ROW_COUNT;
	
	drop table if exists umts_z;

message := 'UMTS KPI calculated. '||int_kpi || ' rows are updated for Datetime ' || _date ||'.';
END IF;
END IF;

RETURN message;
		
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION umts_kpi_rrc_setup_success_rate_sector_hourly(timestamp without time zone)
  OWNER TO postgres;
