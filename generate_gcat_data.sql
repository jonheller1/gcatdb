--------------------------------------------------------------------------------
-- Prerequisites - one time step
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
-- Create Oracle Cloud Infrastructure (OCI) database - one time step
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
-- Design - one time step
--------------------------------------------------------------------------------

Packages:
	todo:
	gcat_helper
	gcat_loader

Column names:
	Column names have a unique prefix plus the rest of the name based on the GCAT name. The prefix helps in complicated SQL statements where multiple tables have similar column names.
	Names that are normal words are separated by an underscore. For example, "ShortName" becomes short_name, but "EName" stays as ename.
	Names that reference other columns have that column in the name. For example, the "Parent" column in ORGANIZATION is named "O_PARENT_O_CODE", to make it obvious which column it references.

Tables:
	* means done.


LAUNCH
	LAUNCH_PAYLOAD_ORG
	LAUNCH_AGENCY

SATELLITE
	SATELLITE_ORG

ORGANIZATION
	ORGANIZATION_CLASS
	ORGANIZATION_ORG_TYPE
		ORGANIZATION_TYPE

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
;

select * from space.organization_org_type;
select * from organization;


--------------------------------------------------------------------------------
-- Create helper objects - one time step.
--------------------------------------------------------------------------------

create or replace package gcat_helper authid current_user is
	c_version constant varchar2(10) := '0.0.1';

	--This list is in an order that could be used to create objets.
	--The order matters because of foreign key constraints.
	--Use the reverse order to drop them.
	c_ordered_objects constant sys.odcivarchar2list := sys.odcivarchar2list
	(
		'ORGANIZATION_CLASS',
		'ORGANIZATION_TYPE',
		'ORGANIZATION',
		'PLATFORM',
		'ORGANIZATION_ORG_TYPE',
		'SITE',
		'SITE_ORG',
		'LAUNCH_VEHICLE_FAMILY',
		'LAUNCH_VEHICLE',
		'LAUNCH_VEHICLE_MANUFACTURER',
		'LAUNCH',
		'LAUNCH_PAYLOAD_ORG',
		'LAUNCH_AGENCY',
		'SATELLITE',
		'SATELLITE_ORG',
		'ENGINE',
		'STAGE',
		'LAUNCH_VEHICLE_STAGE',
		'STAGE_MANUFACTURER',
		'PROPELLANT',
		'ENGINE_PROPELLANT',
		'ENGINE_MANUFACTURER'
	);

	function file_to_blob (p_dir in varchar2, p_filename in varchar2) return blob;
	function get_nt_from_list(p_list in varchar2, p_delimiter in varchar2) return sys.odcivarchar2list;
	procedure vague_date_and_precision(p_date_string in varchar2, p_date out date, p_precision out varchar2);
	function vague_to_date(p_date_string in varchar2) return date;
	function vague_to_precision(p_date_string in varchar2) return varchar2;
	function gcat_to_number(p_number_string varchar2) return number;
	function convert_null(p_string in varchar2) return varchar2;
end gcat_helper;
/

create or replace package body gcat_helper is

	---------------------------------------
	-- Purpose: Safely convert the vague date format into an Oracle date and precision.
	-- (TODO: Should I use a timestamp? Do any vague dates have millisecond precision?)
	procedure vague_date_and_precision
	(
		p_date_string in  varchar2,
		p_date        out date,
		p_precision   out varchar2
	) is
		v_date_string varchar2(4000);
		v_has_question_mark boolean := false;
	begin

		--Find and remove question mark, if it exists.
		if p_date_string like '%?' then
			v_date_string := substr(p_date_string, 1, length(p_date_string)-1);
			v_has_question_mark := true;
		else
			v_date_string := p_date_string;
			v_has_question_mark := false;
		end if;

		--If the date only has 3 digits for a year, add a digit to the front.
		if regexp_like(v_date_string, '^[0-9][0-9][0-9]$') or regexp_like(v_date_string, '^[0-9][0-9][0-9] %') then
			v_date_string := '0' || v_date_string;
		end if;

		--Find the correct formt, looking from largest to smallest value.
		if v_date_string is null or v_date_string = '-' or v_date_string = '*' then
			null;
		elsif v_date_string like '%M' then
			v_date_string := replace(v_date_string, 'M');
			p_date := to_date(to_char((to_number(v_date_string) - 1) * 1000) || '-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Millenia';
			else
				p_precision := 'Millenium';
			end if;
		elsif v_date_string like '%C' then
			v_date_string := replace(v_date_string, 'C');
			p_date := to_date(to_char((to_number(v_date_string) * 100) - 100) || '-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Centuries';
			else
				p_precision := 'Century';
			end if;
		elsif v_date_string like '%s' then
			v_date_string := replace(v_date_string, 's');
			p_date := to_date(to_char(to_number(v_date_string)) || '-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Decades';
			else
				p_precision := 'Decade';
			end if;
/*
		elsif regexp_like(v_date_string, '^[0-9][0-9][0-9]$') then
			p_date := to_date(to_char(to_number(v_date_string)) || '-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Years';
			else
				p_precision := 'Year';
			end if;
*/
		elsif length(v_date_string) = 4 then
			v_date_string := replace(v_date_string, 's');
			p_date := to_date(to_char(to_number(v_date_string)) || '-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Years';
			else
				p_precision := 'Year';
			end if;
		elsif v_date_string like '%Q%' then
			if v_date_string like '%Q1' then
				p_date := to_date(to_char(to_number(substr(v_date_string, 1, 4))) || '-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			elsif v_date_string like '%Q2' then
				p_date := to_date(to_char(to_number(substr(v_date_string, 1, 4))) || '-04-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			elsif v_date_string like '%Q3' then
				p_date := to_date(to_char(to_number(substr(v_date_string, 1, 4))) || '-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			elsif v_date_string like '%Q4' then
				p_date := to_date(to_char(to_number(substr(v_date_string, 1, 4))) || '-10-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
			end if;
			if v_has_question_mark then
				p_precision := 'Quarters';
			else
				p_precision := 'Quarter';
			end if;
		elsif length(v_date_string) = 8 then
			p_date := to_date(v_date_string || ' 01 00:00:00', 'YYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Months';
			else
				p_precision := 'Month';
			end if;
		elsif length(v_date_string) in (10, 11) and v_date_string not like '%.%' then
			p_date := to_date(v_date_string || ' 00:00:00', 'YYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Days';
			else
				p_precision := 'Day';
			end if;
		elsif v_date_string like '%h%' then
			v_date_string := replace(v_date_string, 'h');
			p_date := to_date(v_date_string || ':00:00', 'YYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Hours';
			else
				p_precision := 'Hour';
			end if;
		elsif length(v_date_string) = 13 then
			p_date :=
				to_date(substr(v_date_string, 1, length(v_date_string)-2) || ' 00:00:00', 'YYYY Mon DD HH24:MI:SS')
				+ numToDSInterval(1440 * to_number(substr(v_date_string, -2, 2)), 'MINUTE');
			if v_has_question_mark then
				p_precision := 'Hours';
			else
				p_precision := 'Hour';
			end if;
		elsif length(v_date_string) = 14 then
			p_date :=
				to_date(substr(v_date_string, 1, length(v_date_string)-3) || ' 00:00:00', 'YYYY Mon DD HH24:MI:SS')
				+ numToDSInterval(24*60*60 * to_number(substr(v_date_string, -3, 3)), 'SECOND');
			if v_has_question_mark then
				p_precision := 'Centidays';
			else
				p_precision := 'Centiday';
			end if;
		elsif length(v_date_string) = 16 then
			p_date := to_date(v_date_string || ':00', 'YYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Minutes';
			else
				p_precision := 'Minute';
			end if;
		elsif length(v_date_string) = 19 then
			v_date_string := replace(v_date_string, 'h');
			p_date := to_date(v_date_string, 'YYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Seconds';
			else
				p_precision := 'Second';
			end if;
		--Note: Millisecond not yet implemented - may require conversion to timestamp.
		else
			raise_application_error(-20000, 'Unexpected vague date format: ' || p_date_string);
		end if;
	exception when others then
		raise_application_error(-20000, 'Vague date format error with this date string: ' || p_date_string || chr(10) || sqlerrm);
	end vague_date_and_precision;


	---------------------------------------
	-- Purpose: Safely convert the vague date format into an Oracle date.
	function vague_to_date(p_date_string in varchar2) return date is
		v_date      date;
		v_precision varchar2(4000);
	begin
		vague_date_and_precision(p_date_string, v_date, v_precision);
		return v_date;
	end vague_to_date;


	---------------------------------------
	-- Purpose: Safely get the precision from a vague date.
	function vague_to_precision(p_date_string in varchar2) return varchar2 is
		v_date      date;
		v_precision varchar2(4000);
	begin
		vague_date_and_precision(p_date_string, v_date, v_precision);
		return v_precision;
	end vague_to_precision;


	---------------------------------------
	-- Purpose: Convert a file to a BLOB so it can be used by APEX_DATA_PARSER.
	-- Based on https://oracle-base.com/articles/misc/apex_data_parser
	function file_to_blob (p_dir in varchar2, p_filename in varchar2) return blob
	as
		l_bfile  bfile;
		l_blob   blob;
		l_dest_offset integer := 1;
		l_src_offset  integer := 1;
	begin
		l_bfile := bfilename(p_dir, p_filename);
		dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
		dbms_lob.createtemporary(l_blob, false);
		if dbms_lob.getlength(l_bfile) > 0 then
		dbms_lob.loadblobfromfile (
			dest_lob    => l_blob,
			src_bfile   => l_bfile,
			amount      => dbms_lob.lobmaxsize,
			dest_offset => l_dest_offset,
			src_offset  => l_src_offset);
		end if;
		dbms_lob.fileclose(l_bfile);
		return l_blob;
	end file_to_blob;


	-- Purpose: Split a list of strings into a nested table of string.
	function get_nt_from_list
	(
		p_list in varchar2,
		p_delimiter in varchar2
	) return sys.odcivarchar2list is
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
	end get_nt_from_list;


	---------------------------------------
	-- Purpose: Convert numbers.
	function gcat_to_number(p_number_string varchar2) return number is
	begin
		if p_number_string = '-' then
			return null;
		end if;

		return to_char(p_number_string);
	exception when others then
		raise_application_error(-20000, 'Error converting this string to a number: ' || p_number_string || chr(10) || sqlerrm);
	end gcat_to_number;


	---------------------------------------
	-- Purpose: Convert a GCAT NULL, a dash, to a database NULL.
	function convert_null(p_string in varchar2) return varchar2 is
	begin
		if p_string = '-' then
			return null;
		else
			return p_string;
		end if;
	end convert_null;

end gcat_helper;
/




--------------------------------------------------------------------------------
-- Test helper functions - one time step
--------------------------------------------------------------------------------


declare
	v_date_string varchar2(100);
	v_date date;
	v_precision varchar2(100);
begin
	--Values straight from the GCAT doc, for reference:
	/*
	Precision  Vague Date              Range implied (semiopen interval)    Width
	Millisec   2016 Jun  8 2355:57.345  2016 Jun 8 2355:57.345 to 57.346     1ms
	Second     2016 Jun  8 2355:57      2016 Jun 8 2355:57.0 to 2355:58.0    1s
	Seconds    2016 Jun  8 2355:57?     2016 Jun 8 2355:56.0 to 2355:59.0    3s
	Minute     2016 Jun  8 2355         2016 Jun 8 2355:00 to 2356:00        1m
	Minutes    2016 Jun  8 2355?        2016 Jun 8 2354:00 to 2357:00        3m
	Centiday   2016 Jun  8.98           2016 Jun 8 2331:48 to Jun 8 2345:54  0.01d
	Centidays  2016 Jun  8.98?          2016 Jun 8 2316:48 to Jun 9 0000:00  0.03d
	Hour       2016 Jun  8 23h          2016 Jun 8 2300:00 to Jun 9 0000:00  1h
	Hours      2016 Jun  8.9            2016 Jun 8 2136:00 to Jun 9 0000:00  2.4h
	Day        2016 Jun  8              2016 Jun 8 0000 to 2016 Jun 9 0000   1d
	Days       2016 Jun  8?             2016 Jun 7 0000 to 2016 Jun 10 0000  3d
	Month      2016 Jun                 2016 Jun 1 0000 to 2016 Jul 1 0000   1mo
	Months     2016 Jun?                2016 May 1 0000 to 2016 Aug 1 0000   3mo
	Quarter    2016 Q2                  2016 Apr 1 0000 to 2016 Jul 1 0000   3mo
	Quarters   2016 Q2?                 2016 Jan 1 0000 to 2016 Oct 1 0000   9mo
	Year       2016                     2016 Jan 1 0000 to 2017 Jan 1 0000   1y
	Years      2016?                    2015 Jan 1 0000 to 2018 Jan 1 0000   3y
	Decade     2010s                    2010 Jan 1 0000 to 2020 Jan 1 0000   10y
	Decades    2010s?                   2000 Jan 1 0000 to 2030 Jan 1 0000   30y
	Century    21C                      2001 Jan 1 0000 to 2101 Jan 1 0000   100y
	Centuries  21C?                     1901 Jan 1 0000 to 2201 Jan 1 0000   300y
	Millenium  3M                       2001 Jan 1 0000 to 3001 Jan 1 0000   1000y
	Millenia   3M?                      1001 Jan 1 0000 to 4001 Jan 1 0000   3000y
	*/

	--These dates are mostly from the PDF document, with a few extra values for my own testing.
	--Millisecond not yet implemented.
	--v_date_string := trim('2016 Jun  8 2355:57.345'); vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8 2355:57    '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8 2355:57?   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8 2355       '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8 2355?      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8.98         '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8.98?        '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8 23h        '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8 23h?       '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8.9          '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8.9?         '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun 10.5          '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun 11.5?         '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8            '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun  8?           '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun 30            '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun 30?           '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun               '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Jun?              '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Q2                '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016 Q2?               '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016                   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2016?                  '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2010s                  '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('2010s?                 '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('21C                    '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('21C?                   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('3M                     '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('3M?                    '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	--Some weird dates from the ORGS file.
	v_date_string := trim('700?                   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('927 Jul 12             '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('?                      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('                       '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('-                      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);
	v_date_string := trim('*                      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); dbms_output.put_line('Date in: ' || v_date_string || ', Date out: ' || to_char(v_date, 'YYYY-MM-DD HH24:MI:SS') || ', Precision out: ' || v_precision);

end;
/




--------------------------------------------------------------------------------
-- Create configuration view based on expected headers and how to handle them - one time step.
--------------------------------------------------------------------------------

create or replace view gcat_config_vw as
select 'launch.tsv'  file_name, 'LAUNCH_STAGING'  staging_table_name, 73426 min_expected_rows, '#Launch_Tag	Launch_JD	Launch_Date	LV_Type	Variant	Flight_ID	Flight	Mission	FlightCode	Platform	Launch_Site	Launch_Pad	Ascent_Site	Ascent_Pad	Apogee	Apoflag	Range	RangeFlag	Dest	Agency	Launch_Code	Group	Category	LTCite	Cite	Notes' first_line from dual union all
select 'engines.tsv' file_name, 'ENGINES_STAGING' staging_table_name, 1347  min_expected_rows, '#Name	Manufacturer	Family	Alt_Name	Oxidizer	Fuel	Mass	MFlag	Impulse	ImpFlag	Thrust	TFlag	Isp	IspFlag	Duration	DurFlag	Chambers	Date	Usage	Group' from dual union all
select 'orgs.tsv'    file_name, 'ORGS_STAGING'    staging_table_name, 3270  min_expected_rows, '#Code	UCode	StateCode	Type	Class	TStart	TStop	ShortName	Name	Location	Longitude	Latitude	Error	Parent	ShortEName	EName	UName' from dual
order by file_name;



--------------------------------------------------------------------------------
-- Create job to download files - one time step.
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
		where file_name = 'orgs.tsv'
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
			(line varchar2(4000))
			type oracle_loader
			default directory data_pump_dir
			access parameters (records delimited by "\n" characterset al32utf8)
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
				p_content   => gcat_helper.file_to_blob('DATA_PUMP_DIR', '#FILE_NAME#'),
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
				cross join table(gcat_helper.get_nt_from_list(p_delimiter => '	', p_list => first_line))
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

	procedure drop_if_exists(p_object_name varchar2, p_object_type varchar2) is
		v_table_view_does_not_exist exception;
		pragma exception_init(v_table_view_does_not_exist, -942);
	begin
		execute immediate 'drop '||p_object_type||' '||p_object_name;
	exception when v_table_view_does_not_exist then
		null;
	when others then
		raise_application_error(-20000, 'Error with this object: '||p_object_name||chr(10)||
			sys.dbms_utility.format_error_stack||sys.dbms_utility.format_error_backtrace);
	end drop_if_exists;

begin
	for i in reverse 1 .. gcat_helper.c_ordered_objects.count loop
		drop_if_exists(gcat_helper.c_ordered_objects(i), 'table');
	end loop;
end;
/




--------------------------------------------------------------------------------
-- Load the real tables
--------------------------------------------------------------------------------

--ORGANIZATION_CLASS:
create table organization_class compress as
select cast(oc_code as varchar2(1)) oc_code, oc_description
from
(
	select 'A' oc_code, 'Academic, amateur and non-profit' oc_description from dual union all
	select 'B' oc_code, 'Business/commercial'              oc_description from dual union all
	select 'C' oc_code, 'Civil government'                 oc_description from dual union all
	select 'D' oc_code, 'Defense/military/intelligence'    oc_description from dual union all
	--Fix: "E" and "O" are not in https://planet4589.org/space/gcat/web/orgs/index.html
	select 'E' oc_code, 'Engine/motor manufacturer'        oc_description from dual union all
	select 'O' oc_code, 'Other'                            oc_description from dual
);

alter table organization_class add constraint pk_organization_class primary key(oc_code);


--ORGANIZATION_TYPE:
create table organization_type compress as
select 'CY'  ot_code, 'Country (i.e. nation-state or autonomous region)'                                                                                              ot_description, 'States and similar entities' ot_group from dual union all
select 'IGO' ot_code, 'Intergovernmental organization. Treated as equivalent to a country for the purposes of tabulations of launches by country etc.'                ot_description, 'States and similar entities' ot_group from dual union all 
select 'AP'  ot_code, 'Astronomical Polity: e.g. Luna, Mars. Used for the ''country'' field for locations that are not on Earth and therefore don''t have a country.' ot_description, 'States and similar entities' ot_group from dual union all
select 'E'   ot_code, 'Engine manufacturer'                                                                                                                           ot_description, 'Manufacturers'               ot_group from dual union all
select 'LV'  ot_code, 'Launch vehicle manufacturer'                                                                                                                   ot_description, 'Manufacturers'               ot_group from dual union all
select 'W'   ot_code, 'Meteorological rocket launch agency or manufacturer'                                                                                           ot_description, 'Manufacturers'               ot_group from dual union all
select 'PL'  ot_code, 'Payload manufacturer'                                                                                                                          ot_description, 'Manufacturers'               ot_group from dual union all
select 'LA'  ot_code, 'Launch Agency'                                                                                                                                 ot_description, 'Operators'                    ot_group from dual union all
select 'S'   ot_code, 'Suborbital payload operator'                                                                                                                   ot_description, 'Operators'                    ot_group from dual union all
select 'O'   ot_code, 'Payload owner'                                                                                                                                 ot_description, 'Operators'                    ot_group from dual union all
select 'P'   ot_code, 'Parent organization of another entry'                                                                                                          ot_description, 'Operators'                    ot_group from dual union all
select 'LS'  ot_code, 'Launch site'                                                                                                                                   ot_description, 'Launch origin or destination' ot_group from dual union all
select 'LP'  ot_code, 'Launch position'                                                                                                                               ot_description, 'Launch origin or destination' ot_group from dual union all
select 'LC'  ot_code, 'Launch cruise'                                                                                                                                 ot_description, 'Launch origin or destination' ot_group from dual union all
select 'LZ'  ot_code, 'Launch zone'                                                                                                                                   ot_description, 'Launch origin or destination' ot_group from dual union all
select 'TGT' ot_code, 'Suborbital target area'                                                                                                                        ot_description, 'Launch origin or destination' ot_group from dual;

alter table organization_type add constraint pk_organization_type primary key (ot_code);


--ORGANIZATION:
create table organization compress as
select o_code, o_ucode, o_state_code, o_type, o_oc_code,
	gcat_helper.vague_to_date(o_tstart) o_tstart,
	gcat_helper.vague_to_precision(o_tstart) o_tstart_precision,
	gcat_helper.vague_to_date(o_tstop) o_tstop,
	gcat_helper.vague_to_precision(o_tstop) o_tstop_precision,
	o_short_name,
	o_name,
	o_location,
	gcat_helper.gcat_to_number(o_longitude) o_longitude,
	gcat_helper.gcat_to_number(o_latitude) o_latitude,
	gcat_helper.gcat_to_number(o_error) o_error,
	o_parent_o_code,
	o_short_ename,
	o_ename,
	o_uname
from
(
	--Fix data issues.
	select
		o_code,
		o_ucode,
		o_state_code,
		o_type,
		o_oc_code,
		o_tstart,
		replace(o_tstop, '2015 Feb ?', '2015 Feb?') o_tstop,
		o_short_name,
		o_name,
		o_location,
		o_longitude,
		o_latitude,
		o_error,
		--Fix: Convert multi-parent values into single-parent (the biggest parent).
		case
			when o_parent = 'MOTI/TMI' then 'MOTI'
			when o_parent = 'HISD/LOR' then 'LOR'
			else o_parent
		end o_parent_o_code,
		o_short_ename,
		o_ename,
		o_uname
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null("Code"      ) o_code,
			gcat_helper.convert_null("UCode"     ) o_ucode,
			gcat_helper.convert_null("StateCode" ) o_state_code,
			gcat_helper.convert_null("Type"      ) o_type,
			gcat_helper.convert_null("Class"     ) o_oc_code,
			gcat_helper.convert_null("TStart"    ) o_tstart,
			gcat_helper.convert_null("TStop"     ) o_tstop,
			gcat_helper.convert_null("ShortName" ) o_short_name,
			gcat_helper.convert_null("Name"      ) o_name,
			gcat_helper.convert_null("Location"  ) o_location,
			gcat_helper.convert_null("Longitude" ) o_longitude,
			gcat_helper.convert_null("Latitude"  ) o_latitude,
			gcat_helper.convert_null("Error"     ) o_error,
			gcat_helper.convert_null("Parent"    ) o_parent,
			gcat_helper.convert_null("ShortEName") o_short_ename,
			gcat_helper.convert_null("EName"     ) o_ename,
			gcat_helper.convert_null("UName"     ) o_uname
		from orgs_staging
	) rename_columns
) fix_data;

alter table organization add constraint pk_organization primary key(o_code);
alter table organization add constraint fk_organization_organization foreign key (o_parent_o_code) references organization(o_code);
alter table organization add constraint fk_organization_organization_class foreign key (o_oc_code) references organization_class(oc_code);


--ORGANIZATION_ORG_TYPE

select * from organization;
select * from space.organization_org_type;

o_code, ot_code;


select "Code", "Type"
from orgs_staging;



--Check date functions:
select "TStart", gcat_helper.vague_to_date(replace("TStart", '-'))
from orgs_staging;

select distinct "Launch_Date", gcat_helper.vague_to_date(replace(replace(replace(replace(replace(replace("Launch_Date", '-'), '1963 Jun   5', '1963 Jun  5'), '1963 Jun  25', '1963 Jun 25'), '1963 Jun  26', '1963 Jun 26'), '1971 Mar 24 1832:0', '1971 Mar 24 1832:00'), '1971 Jul 31 2334:0', '1971 Jul 31 2334:00asdf'))
from launch_staging;


--Code is the primary key.
select "Code", count(*) from orgs_staging group by "Code" having count(*) > 1;




select distinct "Parent" from orgs_staging where "Parent" like '%/%';

select * from space.organization;




select * from engines_staging;


select * from engines_staging;



select * from space.engine_propellant;

drop table engine_propellant;
drop table propellant;


select * from propellant order by p_name;
select propellant_name from space.propellant order by propellant_name;





--PROPELLANT:
create table propellant
(
	p_name,
	constraint pk_propellant primary key (p_name)
)
compress as
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
	cross join gcat_helper.get_nt_from_list(propellant_list, '/')
)
order by p_name;























--------------------------------------------------------------------------------
-- Shrink columns.
--------------------------------------------------------------------------------


--Automatically shrink column size as much as possible.
--(This doesn't save space, but can help with applications that use the max data size for presentation.)
declare
	p_table_name varchar2(128) := upper('ORGANIZATION');
	v_max_size number;
begin
	--Alter each table.
	for i in 1 .. gcat_helper.c_ordered_objects.count loop
		--Alter each column in the table.
		for varchar2_columns in
		(
			select
				'select max(lengthb('||column_name||')) from '||table_name v_select,
				'alter table '||table_name||' modify '||column_name||' varchar2(?)' v_alter
			from user_tab_columns
			where table_name = gcat_helper.c_ordered_objects(i)
				and data_type = 'VARCHAR2'
			order by 1
		) loop
			execute immediate varchar2_columns.v_select into v_max_size;
			execute immediate replace(varchar2_columns.v_alter, '?', v_max_size);
		end loop;
	end loop;
end;
/



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


