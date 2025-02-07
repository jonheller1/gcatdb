create or replace package gcat_helper authid current_user is
	c_version constant varchar2(10) := '0.0.1';

	--This list is in an order that could be used to create objets.
	--The order matters because of foreign key constraints.
	--Use the reverse order to drop them.
	c_ordered_objects constant sys.odcivarchar2list := sys.odcivarchar2list
	(
		'ORGANIZATION_TYPE',
		'ORGANIZATION',
		'PLATFORM',
		'PLATFORM_ORG',
		'ORGANIZATION_ORG_TYPE',
		'SITE',
		'SITE_ORG',
		'LAUNCH_POINT',
		'LAUNCH_POINT_ORG',
		'LAUNCH_VEHICLE_FAMILY',
		'LAUNCH_VEHICLE',
		'LAUNCH_VEHICLE_ORG',
		'REFERENCE',
		'LAUNCH',
		'LAUNCH_AGENCY_ORG',
		'LAUNCH_PAYLOAD_ORG',
		'LAUNCH_INVESTIGATOR',
		'WORLD',
		'SATELLITE',
		'SATELLITE_OWNER_ORG',
		'SATELLITE_MANUFACTURER_ORG',
		'PAYLOAD',
		'PAYLOAD_CATEGORY',
		'PAYLOAD_DISCIPLINE',
		'ENGINE',
		'ENGINE_PROPELLANT',
		'ENGINE_MANUFACTURER',
		'STAGE',
		'LAUNCH_VEHICLE_STAGE',
		'STAGE_MANUFACTURER'
	);

	function file_to_blob (p_dir in varchar2, p_filename in varchar2) return blob;
	function get_nt_from_list(p_list in varchar2, p_delimiter in varchar2) return sys.odcivarchar2list;
	procedure vague_date_and_precision(p_date_string in varchar2, p_date out date, p_precision out varchar2);
	function vague_to_date(p_date_string in varchar2) return date;
	function vague_to_precision(p_date_string in varchar2) return varchar2;
	function gcat_to_number(p_number_string varchar2) return number;
	function gcat_to_binary_double(p_number_string varchar2) return binary_double;
	function convert_null_and_trim(p_string in varchar2) return varchar2;
end gcat_helper;
/
create or replace package body gcat_helper is

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
		-- Caching doubles performance.
		dbms_lob.createtemporary(l_blob, cache => true);
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


	---------------------------------------
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
	-- Purpose: Safely convert the vague date format into an Oracle date and precision.
	-- (TODO: Should I use a timestamp? Do any vague dates have millisecond precision?)
	procedure vague_date_and_precision
	(
		p_date_string in  varchar2,
		p_date        out date,
		p_precision   out varchar2
	) is
		v_date_string varchar2(4000) := p_date_string;
		v_has_question_mark boolean := false;
		v_bc_minus_sign varchar2(1);
	begin

		--Find and remove question mark, if it exists, as well as any extra space at the end.
		if v_date_string like '%?' then
			v_date_string := substr(v_date_string, 1, length(v_date_string)-1);
			v_date_string := rtrim(v_date_string);
			v_has_question_mark := true;
		else
			v_has_question_mark := false;
		end if;

		--Fix a common date format issue, where there are too many spaces between the month and the day.
		--For example, "2016 Jun   8" and "2016 Dec  18" should both have one less space.
		v_date_string := regexp_replace(v_date_string, '([Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec])(   )([0-9])', '\1  \3');
		v_date_string := regexp_replace(v_date_string, '([Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec])(  )([0-9][0-9])', '\1 \3');

		--Find and remove the BC indicator, if it exists.
		if v_date_string like 'BC %' then
			v_date_string := substr(v_date_string, 4);
			v_bc_minus_sign := '-';
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
			p_date := to_date(v_bc_minus_sign || to_char((to_number(v_date_string) - 1) * 1000) || '-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Millenia';
			else
				p_precision := 'Millenium';
			end if;
		elsif v_date_string like '%C' then
			v_date_string := replace(v_date_string, 'C');
			p_date := to_date(v_bc_minus_sign || to_char((to_number(v_date_string) * 100) - 100) || '-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Centuries';
			else
				p_precision := 'Century';
			end if;
		elsif v_date_string like '%s' then
			v_date_string := replace(v_date_string, 's');
			p_date := to_date(v_bc_minus_sign || to_char(to_number(v_date_string)) || '-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Decades';
			else
				p_precision := 'Decade';
			end if;
/*
		elsif regexp_like(v_date_string, '^[0-9][0-9][0-9]$') then
			p_date := to_date(v_bc_minus_sign || to_char(to_number(v_date_string)) || '-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Years';
			else
				p_precision := 'Year';
			end if;
*/
		elsif length(v_date_string) = 4 then
			v_date_string := replace(v_date_string, 's');
			p_date := to_date(v_bc_minus_sign || to_char(to_number(v_date_string)) || '-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Years';
			else
				p_precision := 'Year';
			end if;
		elsif v_date_string like '%Q%' then
			if v_date_string like '%Q1' then
				p_date := to_date(v_bc_minus_sign || to_char(to_number(substr(v_date_string, 1, 4))) || '-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			elsif v_date_string like '%Q2' then
				p_date := to_date(v_bc_minus_sign || to_char(to_number(substr(v_date_string, 1, 4))) || '-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			elsif v_date_string like '%Q3' then
				p_date := to_date(v_bc_minus_sign || to_char(to_number(substr(v_date_string, 1, 4))) || '-07-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			elsif v_date_string like '%Q4' then
				p_date := to_date(v_bc_minus_sign || to_char(to_number(substr(v_date_string, 1, 4))) || '-10-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS');
			end if;
			if v_has_question_mark then
				p_precision := 'Quarters';
			else
				p_precision := 'Quarter';
			end if;
		elsif length(v_date_string) = 8 then
			p_date := to_date(v_bc_minus_sign || v_date_string || ' 01 00:00:00', 'SYYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Months';
			else
				p_precision := 'Month';
			end if;
		elsif length(v_date_string) in (10, 11) and v_date_string not like '%.%' then
			p_date := to_date(v_bc_minus_sign || v_date_string || ' 00:00:00', 'SYYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Days';
			else
				p_precision := 'Day';
			end if;
		elsif v_date_string like '%h%' then
			v_date_string := replace(v_date_string, 'h');
			p_date := to_date(v_bc_minus_sign || v_date_string || ':00:00', 'SYYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Hours';
			else
				p_precision := 'Hour';
			end if;
		elsif length(v_date_string) = 13 then
			p_date :=
				to_date(v_bc_minus_sign || substr(v_date_string, 1, length(v_date_string)-2) || ' 00:00:00', 'SYYYY Mon DD HH24:MI:SS')
				+ numToDSInterval(1440 * to_number(substr(v_date_string, -2, 2)), 'MINUTE');
			if v_has_question_mark then
				p_precision := 'Hours';
			else
				p_precision := 'Hour';
			end if;
		elsif length(v_date_string) = 14 then
			p_date :=
				to_date(v_bc_minus_sign || substr(v_date_string, 1, length(v_date_string)-3) || ' 00:00:00', 'SYYYY Mon DD HH24:MI:SS')
				+ numToDSInterval(24*60*60 * to_number(substr(v_date_string, -3, 3)), 'SECOND');
			if v_has_question_mark then
				p_precision := 'Centidays';
			else
				p_precision := 'Centiday';
			end if;
		elsif length(v_date_string) = 16 then
			p_date := to_date(v_bc_minus_sign || v_date_string || ':00', 'SYYYY Mon DD HH24:MI:SS');
			if v_has_question_mark then
				p_precision := 'Minutes';
			else
				p_precision := 'Minute';
			end if;
		elsif length(v_date_string) = 19 then
			v_date_string := replace(v_date_string, 'h');
			p_date := to_date(v_bc_minus_sign || v_date_string, 'SYYYY Mon DD HH24:MI:SS');
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
	-- Purpose: Convert numbers.
	function gcat_to_number(p_number_string varchar2) return number is
		v_number_string varchar2(4000) := p_number_string;
		v_has_m boolean := false;
		v_has_yr boolean := false;
	begin
		--Ignore GCAT "nulls".
		if v_number_string = '-' then
			return null;
		end if;

		--Remove uncertainty. (But this might be useful as a separate flag?)
		v_number_string := replace(v_number_string, '?');

		--If the string ends with "M", record it for later and remove the character.
		if substr(v_number_string, -1, 1) = 'M' then
			v_has_m := true;
			v_number_string := substr(v_number_string, 1, length(v_number_string)-1);
		end if;		

		--If the string ends with "yr", record it for later and remove the character.
		if substr(v_number_string, -2, 2) = 'yr' then
			v_has_yr := true;
			v_number_string := substr(v_number_string, 1, length(v_number_string)-2);
		end if;		


		--Multiple by 1 million for "M" and 365 for "yr" if necessary.
		if v_has_m then
			return to_number(v_number_string) * 1000000;
		elsif v_has_yr then
			--Why 365.26? It's complicatd: 
			--https://www.washingtonpost.com/news/speaking-of-science/wp/2017/02/24/think-you-know-how-many-days-are-in-a-year-think-again/
			return to_number(v_number_string) * 365.26;
		else
			return to_number(v_number_string);
		end if;
	exception when others then
		raise_application_error(-20000, 'Error converting this string to a number: ' || p_number_string || chr(10) || sqlerrm);
	end gcat_to_number;


	---------------------------------------
	-- Purpose: Convert numbers that may include "Inf" for infinity. Only BINARY_DOUBLE supports those values.
	function gcat_to_binary_double(p_number_string varchar2) return binary_double is
		v_number_string varchar2(4000) := p_number_string;
	begin
		--Ignore GCAT "nulls".
		if v_number_string = '-' then
			return null;
		end if;

		--Remove uncertainty. (But this might be useful as a separate flag?)
		v_number_string := replace(v_number_string, '?');

		if v_number_string = 'Inf' then
			return binary_double_infinity;
		else
			return to_number(v_number_string);
		end if;
	exception when others then
		raise_application_error(-20000, 'Error converting this string to a binary_double: ' || p_number_string || chr(10) || sqlerrm);
	end gcat_to_binary_double;


	---------------------------------------
	-- Purpose: Convert a GCAT NULL, a dash, to a database NULL, and remove leading and trailing whitespace.
	function convert_null_and_trim(p_string in varchar2) return varchar2 is
		pragma udf;
	begin
		if p_string = '-' then
			return null;
		else
			return trim(p_string);
		end if;
	end convert_null_and_trim;

end gcat_helper;
/
