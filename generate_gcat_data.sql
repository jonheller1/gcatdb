--------------------------------------------------------------------------------
-- Prerequisites
--------------------------------------------------------------------------------
/*
-- # Prequisites

1. Oracle 19c+ on Windows
2. Apex 21c+
Download the latest from https://www.oracle.com/tools/downloads/apex-downloads.html
Cd into the directory and run this command as PDB SYS:
@apexins.sql SYSAUX SYSAUX TEMP /i/

*/



--------------------------------------------------------------------------------
-- Create OCI database
--------------------------------------------------------------------------------

Create Oracle Cloud account.

Open this link to see the login screen: https://console.us-ashburn-1.oraclecloud.com/db/adb

Choose oracleidentitycloudservice and click Continue to enter password.

Enter Username and Password to continue to Autonomous Database.

Click "Create Autonomous Database"

Change "Display name" and "Database name" to "gcat"

Keep the free options

Create and write down new password for "Create administrator credentials"

Keep the default connection options

Enter Contact Email

Click "Create Autonomous Database"

Wait about a minute for the database to provision and status to change to "Available"

Click on "DB Connection" to setup local connections

Change Wallet type to "Regional Wallet", so you will get metadata for all relevant instances.

Create and write down new password for the wallet and save the file.

Find the directory of your tnsnames.ora file by running this on your local command line:
	tnsping anything

Create a subdirectory named "gcatwallet" and copy the files from the downloaded ZIP file into that directory.

In that new subdirectory, open the tnsnames.ora file and copy the new gcat entries and paste them into your normal tnsnames.ora file.

Open your sqlnet.ora file and add a WALLET_LOCATION like this that refers to your new wallet subdirectory.
	WALLET_LOCATION = (SOURCE = (METHOD = file) (METHOD_DATA = (DIRECTORY="D:\19c\WINDOWS.X64_193000_db_home\network\admin\gcatwallet")))

You should be able to ping the new database from your local command line like this:
	tnsping gcat_low

You should be able to login to the new database like this:
	sqlplus ADMIN/(admin password from above)@gcat_low

(TODO: Create separate user for link, not ADMIN)
Create database link from local machine to GCAT admin:
	create database link gcat connect to admin identified by "<password from above>" using 'gcat_low';

Test database link:
	select * from dual@gcat;



--------------------------------------------------------------------------------
-- Design
--------------------------------------------------------------------------------

Packages:
	todo:
	gcat_helper
	gcat_loader

Column names:
	Are prefixed to uniquely identify their table, with the rest of the name based on the GCAT name.
	

Tables:
	* means done.


LAUNCH
	LAUNCH_PAYLOAD_ORG
	LAUNCH_AGENCY

SATELLITE
	SATELLITE_ORG

ORGANIZATION
	ORGANIZATION_ORG_TYPE

PLATFORM

SITE
	SITE_ORG

LAUNCH_VEHICLE
	LAUNCH_VEHICLE_MANUFACTURER
	LAUNCH_VEHICLE_FAMILY

STAGE
	STAGE_MANUFACTURER

* PROPELLANT

ENGINE
	ENGINE_MANUFACTURER
	ENGINE_PROPELLANT

LAUNCH_VEHICLE_STAGE





--------------------------------------------------------------------------------
-- Create helper objects - one time step.
--------------------------------------------------------------------------------



-- From: https://oracle-base.com/articles/misc/apex_data_parser
CREATE OR REPLACE FUNCTION file_to_blob (p_dir       IN  VARCHAR2,
                                         p_filename  IN  VARCHAR2)
  RETURN BLOB
  -- JH 2021-11-06 Added this line:
  authid current_user
AS
  l_bfile  BFILE;
  l_blob   BLOB;

  l_dest_offset INTEGER := 1;
  l_src_offset  INTEGER := 1;
BEGIN
  l_bfile := BFILENAME(p_dir, p_filename);
  DBMS_LOB.fileopen(l_bfile, DBMS_LOB.file_readonly);
  DBMS_LOB.createtemporary(l_blob, FALSE);
  IF DBMS_LOB.getlength(l_bfile) > 0 THEN
    DBMS_LOB.loadblobfromfile (
      dest_lob    => l_blob,
      src_bfile   => l_bfile,
      amount      => DBMS_LOB.lobmaxsize,
      dest_offset => l_dest_offset,
      src_offset  => l_src_offset);
  END IF;
  DBMS_LOB.fileclose(l_bfile);
  RETURN l_blob;
END file_to_blob;
/

create or replace function get_nt_from_list
(
	p_list in varchar2,
	p_delimiter in varchar2
) return sys.odcivarchar2list is
/*
	Purpose: Split a list of strings into a nested table of string.
*/
	v_index number := 0;
	v_item varchar2(32767);
	v_results sys.odcivarchar2list := sys.odcivarchar2list();
begin
	--Split.
	loop
		v_index := v_index + 1;
		v_item := regexp_substr(p_list, '[^' || p_delimiter || ']+', 1, v_index);
		exit when v_item is null;
		v_results.extend;
		v_results(v_results.count) := v_item;		
	end loop;

	return v_results;
end;
/







--------------------------------------------------------------------------------
-- Create configuration view based on expected headers and how to handle them.
--------------------------------------------------------------------------------

create or replace view gcat_config_vw as
select 'launch.tsv'  file_name, 'LAUNCH_STAGING'  staging_table_name, 73426 min_expected_rows, '#Launch_Tag	Launch_JD	Launch_Date	LV_Type	Variant	Flight_ID	Flight	Mission	FlightCode	Platform	Launch_Site	Launch_Pad	Ascent_Site	Ascent_Pad	Apogee	Apoflag	Range	RangeFlag	Dest	Agency	Launch_Code	Group	Category	LTCite	Cite	Notes' first_line from dual union all
select 'engines.tsv' file_name, 'ENGINES_STAGING' staging_table_name, 1347  min_expected_rows, '#Name	Manufacturer	Family	Alt_Name	Oxidizer	Fuel	Mass	MFlag	Impulse	ImpFlag	Thrust	TFlag	Isp	IspFlag	Duration	DurFlag	Chambers	Date	Usage	Group' from dual
order by file_name;







--------------------------------------------------------------------------------
-- Create job to download files
-- MUST RUN THIS AS SYS.
--------------------------------------------------------------------------------

--Must run this as SYS.
--Based on https://asktom.oracle.com/pls/apex/asktom.search?tag=dbms-scheduler-execute-bat-file
declare
	v_job_name varchar(20) := 'SYS.GCAT_CURL_JOB';
	v_unknown_job exception;
	pragma exception_init(v_unknown_job, -27475);
begin
	--Ensure user is correct.
	if user <> 'SYS' then
		raise_application_error(-20000, 'You must run this PL/SQL block as sys, because only SYS ' ||
			'has the right OS privileges to run the OS commands.');
	end if;

	--Drop old job, if any.
	begin
		dbms_scheduler.drop_job(v_job_name);
	exception when v_unknown_job then null;
	end;

	--Create new job.
	dbms_scheduler.create_job
	(
		job_name            => v_job_name,
		job_type            => 'EXECUTABLE',
		job_action          => 'C:\Windows\System32\curl.exe',
		number_of_arguments => 4,
		enabled             => false,
		auto_drop           => false
	);
end;
/

grant alter on sys.gcat_curl_job to jheller;




--------------------------------------------------------------------------------
-- Download the files into the directory.
-- (Must run this, and everything below, as your normal user.)
--------------------------------------------------------------------------------

declare
	v_name varchar(20) := 'SYS.GCAT_CURL_JOB';
	v_directory_path varchar2(128);
begin
	--Find the path.
	select directory_path
	into v_directory_path
	from all_directories
	where directory_name = 'DATA_PUMP_DIR';

	--Run the job for each file.
	for files in
	(
		select file_name
		from gcat_config_vw
		--TEMP for TESTING - only use one file.
		where file_name = 'engines.tsv'
		order by file_name
	) loop
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 1, argument_value => '--output');
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 2, argument_value => v_directory_path || '/' || files.file_name);
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 3, argument_value => '--url');
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 4, argument_value => 'https://planet4589.org/space/gcat/tsv/tables/' || files.file_name);
		dbms_scheduler.run_job(v_name);
	end loop;
end;
/




--------------------------------------------------------------------------------
-- Check file headers for unexpected changes.
--   File header changes will probably require manual intervention, like changing columns.
--------------------------------------------------------------------------------

-- Raise an exception if any of the file headers are unexpected.
declare
	v_template varchar2(4000) :=
	q'[
		select line
		from external
		(
			(line clob)
			type oracle_loader
			default directory data_pump_dir
			access parameters (records delimited by "\n")
			location ('#FILE_NAME#')
		)
		where rownum = 1
	]';
	v_sql clob;
	v_first_line clob;
begin
	for expected_headers in
	(
		select file_name, first_line
		from gcat_config_vw
		order by file_name
	) loop
		v_sql := replace(v_template, '#FILE_NAME#', expected_headers.file_name);
		execute immediate v_sql into v_first_line;

		if v_first_line = expected_headers.first_line then
			null;
		else
			raise_application_error(-20000, 'For file ' || expected_headers.file_name || chr(10) ||
				'Expected: ' || expected_headers.first_line || chr(10) ||
				'Actual: ' || v_first_line);
		end if;
	end loop;
end;
/




--------------------------------------------------------------------------------
-- Create staging tables.
-- May take a few minutes to run.
--------------------------------------------------------------------------------

--Recreate staging tables for each file, based on the first header row.
--The staging tables are small wrappers around the files.
declare
	v_sql_template varchar2(32767) :=
	q'[
		create table #STAGING_TABLE_NAME# as
		select
			line_number, #COLUMN_LIST#
		from table
		(
			apex_data_parser.parse
			(
				p_content   => file_to_blob('DATA_PUMP_DIR', '#FILE_NAME#'),
				p_file_name => '#FILE_NAME#'
			)
		)
		where col001 not like '#%'
	]';
	v_sql varchar2(32767);
begin
	--Drop existing staging tables.
	declare
		v_table_does_not_exist exception;
		pragma exception_init(v_table_does_not_exist, -942);
	begin
		for tables in
		(
			select staging_table_name
			from gcat_config_vw
			order by staging_table_name
		) loop
			begin
				execute immediate 'drop table ' || tables.staging_table_name;
			exception when v_table_does_not_exist then null;
			end;
		end loop;
	end;

	--Create staging tables.
	for files in
	(
		--Column list for each file.
		select
			file_name,
			staging_table_name,
			listagg(column_number || ' "' || column_name || '"', ',') within group (order by rownumber) column_list
		from
		(
			--Format column names for the CREATE VIEW statements.
			select
				file_name,
				staging_table_name,
				replace(column_value, '#') column_name,
				'col' || lpad(row_number() over (partition by file_name order by rownumber), 3, '0') column_number,
				rownumber
			from
			(
				--Raw column names from the configuration file.
				select
					file_name,
					staging_table_name,
					column_value,
					row_number() over (partition by file_name order by rownum) rownumber
				from gcat_config_vw
				cross join table(get_nt_from_list(p_delimiter => '	', p_list => first_line))
			)
		)
		group by file_name, staging_table_name
		order by file_name
	) loop
		v_sql := replace(replace(replace(v_sql_template,
			'#STAGING_TABLE_NAME#'  , files.staging_table_name),
			'#COLUMN_LIST#', files.column_list),
			'#FILE_NAME#'  , files.file_name);
		begin
			execute immediate v_sql;
		exception when others then
			raise_application_error(-20000, 'Error ' || sqlerrm || ' with this SQL: ' || chr(10) || v_sql);
		end;
	end loop;
end;
/




--------------------------------------------------------------------------------
-- Quick quality check of staging tables.
--------------------------------------------------------------------------------

--Ensure that the number of rows is higher than a previous value, on the assumption
--that the row counts will never decrease with future releases.
declare
	v_count number;
begin
	for tables in
	(
		select 'select count(*) from ' || staging_table_name v_sql, staging_table_name, min_expected_rows
		from gcat_config_vw
		order by staging_table_name
	) loop
		execute immediate tables.v_sql into v_count;

		if v_count < tables.min_expected_rows then
			raise_application_error(-20000, 'Expected at least ' || tables.min_expected_rows || ' rows from ' ||
				tables.staging_table_name || ', but there are only ' || v_count || '.');
		end if;
	end loop;
end;
/




--------------------------------------------------------------------------------
-- Drop the presentation tables.
--------------------------------------------------------------------------------


declare
	v_table_does_not_exist exception;
	pragma exception_init(v_table_does_not_exist, -942);
begin
	for tables in
	(
		select 'drop table ' || column_value v_sql
		from table(sys.odcivarchar2list('ENGINE_PROPELLANT', 'PROPELLANT'))
	) loop
		begin
			execute immediate tables.v_sql;
		exception when v_table_does_not_exist then null;
		end;
	end loop;
end;
/



--------------------------------------------------------------------------------
-- Load the real tables
--------------------------------------------------------------------------------


--PROPELLANT
create table propellant compress as
select propellant_name p_name
from
(
	select distinct
		--Fix: It looks like some of the values got their last letter cut off.
		replace(replace(replace(replace(column_value,
			'Al TP-H-334', 'Al TP-H-3340'),
			'AlTP-H-3062', 'AlTP-H-3062M'),
			'RFNA AK-20', 'RFNA AK-20F'),
			'Vinyl Isobutyl ethe', 'Vinyl Isobutyl ether')
		propellant_name
	from
	(
		--All chemicals
		--
		--Oxidizers.
		select replace("Oxidizer", '?') propellant_list
		from engines_staging
		where "Oxidizer" <> '-'
		union
		--Fuels.
		--(Fix: one weird fuel that looks like there's a missing second value)
		select case when propellant_list = 'MMH/' then 'MMH' else propellant_list end
		from
		(
			select replace("Fuel", '?') propellant_list
			from engines_staging
			where "Fuel" <> '-'
		)
	)
	cross join get_nt_from_list(propellant_list, '/')
)
order by p_name;

alter table propellant add constraint propellant_pk primary key (p_name);








select * from engines_staging;


/*

*/

select * from space.engine_propellant;

drop table engine_propellant;
drop table propellant;


select * from propellant order by p_name;
select propellant_name from space.propellant order by propellant_name;













--------------------------------------------------------------------------------
-- Old ideas
--------------------------------------------------------------------------------



-- Engine types and subtypes with their rownumber ranges.
select rownumber, nvl(lead(rownumber) over (order by rownumber), 99999999) next_rownumber, line, engine_type, engine_subtype
from
(
	--Engine types and subtypes.
	select rownumber, line
		,regexp_substr(line, '(# )([^:]*)(: *)?(.*)?', 1, 1, 'i', 2) engine_type
		,regexp_substr(line, '(# )([^:]*)(: *)?(.*)?', 1, 1, 'i', 4) engine_subtype
	from
	(
		--Raw data.
		select rownum rownumber, to_char(line) line
		from external
		(
			(line clob)
			type oracle_loader
			default directory data_pump_dir
			access parameters (records delimited by "\n")
			location ('engines.tsv')
		)
	) raw_data
	where rownumber > 2
		and line like '# %'
) engine_types_and_subtypes

;


