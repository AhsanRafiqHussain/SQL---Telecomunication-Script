CREATE OR REPLACE FUNCTION umts_sector_information_main(_date timestamp without time zone)
  RETURNS Text As 
	$BODY$
DECLARE 
 db text := 'conure';
  sql  TEXT;
  subquery text;
  sp text ;
  i integer default 0;
  s integer default 0;
  conn text;
  n integer default 0;
  num_done integer := 0;
  status integer default 0;
  dispatch_result integer default 0;
  dispatch_error text;
message text;
message1 text;
count_pm INTEGER;

BEGIN
			
	count_pm:= (SELECT count(*) 
	FROM huawei_pm_3g_algo_cell
		where begintime = _date);		

IF count_pm = 0 THEN 
message := 'UMTS KPI not calculated. PM records for selected date does not exist.';
ELSE IF count_pm > 0 THEN

truncate umts_temp_information;
select umts_sector_information(_date) into message1;

  n := (Select Count(name) FROM kpi_and_cqi_references where technology = 'umts' and is_active = true);
	
  -- loop through chunks
  BEGIN
		for sp in  Select name FROM kpi_and_cqi_references where technology = 'umts' and is_active = true
		LOOP
			i := i + 1;
			--make a new db connection
			conn := 'conn_' || i;  
			sql := 'SELECT dblink_connect(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL('dbname=' || db) ||');';
			execute sql;
			
			subquery := 'SELECT '||sp||'('|| QUOTE_LITERAL(_date) ||');';
			raise NOTICE 'SP SelectQuerry %', subquery;
		 
			--send the query asynchronously using the dblink connection
			sql := 'SELECT dblink_send_query(' || QUOTE_LITERAL(conn) || ',' || QUOTE_LITERAL(subquery) || ');';----
			execute sql into dispatch_result;

			--check for errors dispatching the query
			if dispatch_result = 0 then
			sql := 'SELECT dblink_error_message(' || QUOTE_LITERAL(conn)  || ');';
			execute sql into dispatch_error;
			RAISE NOTICE'dispatch errro msg for run %', dispatch_error;
			end if;
		end loop;
  END;

  BEGIN 
	  -- wait until all queries are finished
	WHILE num_done < n  Loop
	  num_done := 0;
		  for s in 1..n Loop
		  conn := 'conn_'||s;
			sql := 'SELECT dblink_is_busy(' || QUOTE_LITERAL(conn) || ');';
			execute sql into status;
			if status = 0 THEN	
				-- check for error messages
				sql := 'SELECT dblink_error_message('||QUOTE_LITERAL(conn)||');';
				execute sql into dispatch_error;
				if dispatch_error <> 'OK' THEN
					RAISE 'dispatch errro msg for wait %', dispatch_error;
				end if;
				num_done := num_done + 1;
				sql := 'SELECT dblink_disconnect('||QUOTE_LITERAL(conn)||');';
				execute sql;
			END if;
		  end loop;
	END loop;
	END;
  
    
   -- disconnect the dblinks
  i := 0;
  FOR i in 1..n
  LOOP
	conn := 'conn_' || i;
	sql := 'SELECT dblink_disconnect('|| QUOTE_LITERAL(conn)||');';
	execute sql;
  end loop;

  truncate umts_temp_information;
  message := 'UMTS KPI Calculated sucessfully.Inserted in information' || message1 ||'.';

  exception when others then
  BEGIN
	s := 0;
	  RAISE NOTICE '% %', SQLERRM, SQLSTATE;
	  for s in 
		SELECT generate_series(1,n)
	  LOOP
		conn := 'conn_'||s;
		sql := 'SELECT dblink_disconnect(' || QUOTE_LITERAL(conn) || ');';
		execute sql;
	  END LOOP;
	  exception when others then
		RAISE NOTICE 'Exception % %', SQLERRM, SQLSTATE;
  end;
END IF;
END IF;

RETURN message;
		
END;$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION umts_sector_information_main(timestamp without time zone)
  OWNER TO postgres;