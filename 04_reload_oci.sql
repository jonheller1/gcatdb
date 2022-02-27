
--------------------------------------------------------------------------------
-- Import data to cloud. Run these steps from the CLOUD database.
--------------------------------------------------------------------------------


--Copy a (pre-authenticated) file from object storage to the database directory.
--Add the new pre-authenticated URL below. Takes 5 seconds.
begin
	dbms_cloud.get_object
	(
		object_uri     => '&URL',
		directory_name => 'DATA_PUMP_DIR'
	);
end;
/

--Ensure that new file exists in database directory
select * from dbms_cloud.list_files('DATA_PUMP_DIR') order by created desc;


--Import the dump file. Takes 40 seconds.
declare
  l_dp_handle       number;
  v_date_string  varchar2(100) := to_char(sysdate, 'YYYYMMDDHH24MISS');
  v_job_status   varchar2(128);
begin
  -- Open a schema import job.
  l_dp_handle := dbms_datapump.open(
    operation   => 'IMPORT',
    job_mode    => 'TABLE',
    job_name    => 'GCAT_IMPORT_'||v_date_string,
    version     => 'LATEST');

  -- Specify the dump file name and directory object name.
  dbms_datapump.add_file(
    handle    => l_dp_handle,
    filename  => 'GCAT_20220223004933.DMP',
    directory => 'DATA_PUMP_DIR');

  -- Specify the log file name and directory object name.
  dbms_datapump.add_file(
    handle    => l_dp_handle,
    filename  => 'GCAT_IMPORT_'||v_date_string||'.log',
    directory => 'DATA_PUMP_DIR',
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);

  -- Perform a REMAP_TABLE.
  dbms_datapump.metadata_remap(
    handle     => l_dp_handle,
    name       => 'REMAP_SCHEMA',
    old_value  => 'JHELLER',
    value      => 'GCAT');

  dbms_datapump.start_job(l_dp_handle);

  dbms_datapump.wait_for_job(
    handle    => l_dp_handle,
    job_state => v_job_status);

  dbms_output.put_line ('DataPump Import - '||to_char(sysdate,'DD/MM/YYYY HH24:MI:SS')||' Status '||v_job_status);

  dbms_datapump.detach(l_dp_handle);
end;
/


--Find the newest file.
select * from dbms_cloud.list_files('DATA_PUMP_DIR') order by created desc;


--Read the logfile for any errors.
select *
from external
(
	(
		line varchar2(4000)
	)
	default directory data_pump_dir
	access parameters
	(
		records delimited by newline
		fields
		missing field values are null
	)
	location ('GCAT_IMPORT_20220225061609.log')
);



--Create grants and public synonyms.
begin
	execute immediate
	q'[
		create or replace procedure create_public_synonyms_and_grants as
		begin
			for tables in
			(
				select
					'create or replace public synonym ' || object_name || ' for ' || owner || '.' || object_name v_synonym_sql,
					'grant select on '||owner||'.'||object_name||' to GCAT_PUBLIC' v_grant_sql
				from all_objects
				where owner = 'GCAT'
					and object_type not in ('INDEX')
				order by object_name
			) loop
				execute immediate tables.v_synonym_sql;
				execute immediate tables.v_grant_sql;
			end loop;
		end;
	]';

	execute immediate 'begin create_public_synonyms_and_grants; end;';

	execute immediate
	q'[
		drop procedure create_public_synonyms_and_grants
	]';
end;
/

