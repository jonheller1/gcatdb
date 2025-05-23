create or replace package gcat_exporter authid current_user is
	-- True public functions
	procedure generate_oracle_file;
	procedure generate_postgres_file;
	procedure generate_csv_files;

	-- Helper functions
	function get_formatted_string(p_string varchar2) return varchar2;
	function get_formatted_date(p_date date) return varchar2;
	function get_formatted_number(p_number number) return varchar2;
	function get_formatted_binary_double(p_binary_double binary_double) return varchar2;
end;
/
create or replace package body gcat_exporter is

c_version constant varchar2(10) := '0.0.1';
c_export_directory constant varchar2(128) := 'GCATDB_EXPORT';

--==============================================================================
--==============================================================================
function get_formatted_string(p_string varchar2) return varchar2 is
begin
	return '''' || replace(p_string, '''', '''''') || '''';
end;



--==============================================================================
--==============================================================================
function get_formatted_date(p_date date) return varchar2 is
begin
	-- Save some space by not printing unnecessary hours, minutes, and seconds.
	if to_char(p_date, 'HH24:MI:SS') = '00:00:00' then
		return '''' || to_char(p_date, 'YYYY-MM-DD') || '''';
	else
		return '''' || to_char(p_date, 'YYYY-MM-DD hh24:mi:ss') || '''';
	end if;
end;



--==============================================================================
--==============================================================================
function get_formatted_number(p_number number) return varchar2 is
begin
	if p_number is null then
		return 'null';
	else
		return to_char(p_number);
	end if;
end;



--==============================================================================
--==============================================================================
function get_formatted_binary_double(p_binary_double binary_double) return varchar2 is
begin
	if p_binary_double is null then
		return 'null';
	elsif p_binary_double = binary_double_infinity then
		return 'binary_double_infinity';
	else
		return to_char(p_binary_double);
	end if;
end;




--==============================================================================
--==============================================================================
-- Return formatted metadata, for use in a load script.
-- This function is mostly from the DBMS_METADATA chapter of the manual:
--   https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/using-oracle-dbms_metadata-api.html
FUNCTION get_metadata
(
	p_object_type varchar2,
	p_schema varchar2,
	p_table_name varchar2
) RETURN CLOB IS
 -- Define local variables.
 h    NUMBER;   -- handle returned by 'OPEN'
 th   NUMBER;   -- handle returned by 'ADD_TRANSFORM'
 doc  CLOB;
BEGIN
 -- Specify the object type. 
 h := DBMS_METADATA.OPEN(p_object_type);

 -- Use filters to specify the particular object desired.
 DBMS_METADATA.SET_FILTER(h,'SCHEMA',p_schema);
 DBMS_METADATA.SET_FILTER(h,'NAME',p_table_name);

 -- Request that the metadata be transformed into creation DDL.
 th := dbms_metadata.add_transform(h,'DDL');

 -- Don't print schema name.
 DBMS_METADATA.SET_TRANSFORM_PARAM(th, 'EMIT_SCHEMA', false);

 -- Specify that segment attributes are not to be returned.
 -- Note that this call uses the TRANSFORM handle, not the OPEN handle.
 DBMS_METADATA.SET_TRANSFORM_PARAM(th,'SEGMENT_ATTRIBUTES',false);

 -- Fetch the object.
 doc := DBMS_METADATA.FETCH_CLOB(h);

 -- Release resources.
 DBMS_METADATA.CLOSE(h);

 RETURN doc;
END;



--==============================================================================
--==============================================================================
procedure generate_oracle_file is

	---------------------------------------
	function get_handle(p_open_mode in varchar2) return utl_file.file_type is
		v_handle utl_file.file_type;
	begin
		v_handle := utl_file.fopen(c_export_directory, 'oracle_create_gcatdb.sql', open_mode => p_open_mode);
		return v_handle;
	end get_handle;


	---------------------------------------
	procedure write_header is
		v_handle utl_file.file_type;
	begin
		v_handle := get_handle(p_open_mode => 'w');
		utl_file.put_line(v_handle, replace(replace(replace(replace(substr(
		q'[
				-- This file creates the gcatdb schema for Oracle databases.
				-- DO NOT MODIFY THIS FILE.  It is automatically generated.

				-- Session settings.
				-- Don't prompt for any ampersands:
				set define off;
				-- We don't need to see every message.
				set feedback off;
				-- But we do want to see a few specific messages indicating status.
				set serveroutput on
				-- Use the same date format to make file smaller:
				alter session set nls_date_format = 'YYYY-MM-DD HH24:MI:SS';
				-- Use CHAR semantics for UTF8 data.
				alter session set nls_length_semantics='CHAR';


				-- Print intro message.
				exec dbms_output.put_line('------------------------------------------------------------------------');
				exec dbms_output.put_line('-- Installing gcatdb. Data was was retrieved on #DATE#.');
				exec dbms_output.put_line('-- ');
				exec dbms_output.put_line('-- Data from GCAT (J. McDowell, planet4589.org/space/gcat)');
				exec dbms_output.put_line('-- Schema and scripts from Jon Heller, jon@jonheller.org');
				exec dbms_output.put_line('-- The database installs #TABLE_COUNT# tables and uses about 55MB of space.');
				exec dbms_output.put_line('--');
				exec dbms_output.put_line('-- The installation will run for about a minute and will stop on any');
				exec dbms_output.put_line('-- errors.  You should see a "done" message at the end.');
				exec dbms_output.put_line('------------------------------------------------------------------------');

				-- Stop at the first error.
				whenever sqlerror exit sql.sqlcode;
				]'
		, 2)
		, '				')
		, '#VERSION#', c_version)
		, '#DATE#', to_char(sysdate, 'YYYY-MM-DD'))
		, '#TABLE_COUNT#', gcat_helper.c_ordered_objects.count));

		utl_file.fclose(v_handle);
	end write_header;


	---------------------------------------
	procedure write_check_for_existing_data is
		v_handle utl_file.file_type;
		v_in_list varchar2(32767);
	begin
		v_handle := get_handle(p_open_mode => 'a');

		--Create an IN-LIST of all relevant tables.
		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			v_in_list := v_in_list || ',' || '''' || gcat_helper.c_ordered_objects(i) || '''';
		end loop;
		v_in_list := substr(v_in_list, 2);
		v_in_list := '(' || v_in_list || ')';

		--Print PL/SQL block checking for existing tables.
		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
		utl_file.put_line(v_handle, '-- Checking for existing tables.');
		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
		utl_file.put_line(v_handle, 'exec dbms_output.put_line(''Checking for existing tables...'');');
		utl_file.put_line(v_handle, replace(replace(q'[
			--Raise an error and stop installation if any tables exist.
			declare
				v_tables_already_exist varchar2(32767);
			begin
				select listagg(table_name, ',') within group (order by table_name)
				into v_tables_already_exist
				from all_tables
				where owner = sys_context('userenv', 'CURRENT_SCHEMA')
					and table_name in ]' || v_in_list || q'[
				;

				if v_tables_already_exist is not null then
					raise_application_error(-20000, 'These tables already exist in the schema ' ||
					sys_context('userenv', 'CURRENT_SCHEMA') || ': ' || v_tables_already_exist);
				end if;
			end;
			#SLASH#]'
			, '			'),
			'#SLASH#', '/')
		);

		utl_file.fclose(v_handle);
	end write_check_for_existing_data;


	---------------------------------------
	procedure write_metadata_and_data is
		v_handle utl_file.file_type;
		v_metadata varchar2(32767);
		v_select varchar2(32767);
		type string_table is table of varchar2(32767);
		v_rows string_table;
		-- The number of rows to be UNION ALL'd together.
		-- The number must be low enough so that this many rows put together is less than 32K bytes.
		-- The Satellite table seems to be the largest offender and cannot do 100 rows as a time.
		c_rows_per_chunk constant number := 75;
		v_union_all varchar2(32767);
	begin
		v_handle := get_handle(p_open_mode => 'a');

		--Tables:
		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			--Table header:
			utl_file.new_line(v_handle);
			utl_file.new_line(v_handle);
			utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
			utl_file.put_line(v_handle, '-- '||gcat_helper.c_ordered_objects(i));
			utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
			utl_file.new_line(v_handle);
			utl_file.put_line(v_handle, 'exec dbms_output.put_line(''Installing '||gcat_helper.c_ordered_objects(i) || '...'');');

			--Metadata for tables:
			v_metadata := get_metadata('TABLE', user, gcat_helper.c_ordered_objects(i)) || ';';
			--Special case for self-referencing key on Organization.
			if gcat_helper.c_ordered_objects(i) = 'ORGANIZATION' then
				v_metadata := replace(v_metadata,
					'REFERENCES "ORGANIZATION" ("O_CODE") ENABLE',
					'REFERENCES "ORGANIZATION" ("O_CODE") DISABLE'
				);
			end if;
			utl_file.put_line(v_handle, replace(v_metadata, chr(10)||'  ', chr(10)));
			utl_file.new_line(v_handle);

			--Metadata for indexes:
			for indexes_to_create in
			(
				select table_owner, index_name
				from user_indexes
				where table_name = gcat_helper.c_ordered_objects(i)
					--Primary key and unique keys are created as part of table definition.
					and index_name not like 'PK%'
					and index_name not like 'UQ%'
				order by index_name
			) loop
				v_metadata := get_metadata('INDEX', indexes_to_create.table_owner, indexes_to_create.index_name) || ';';
				utl_file.put_line(v_handle, replace(v_metadata, chr(10)||'  ', chr(10)));
			end loop;

			--Data:
			--Create SELECT statement that will generate another SELECT statement.
			select
				'select ''select ''||' ||
				listagg
				(
					case
						when data_type = 'VARCHAR2' then 'gcat_exporter.get_formatted_string('||column_name||')'
						when data_type = 'NUMBER' then 'gcat_exporter.get_formatted_number('||column_name||')'
						when data_type = 'BINARY_DOUBLE' then 'gcat_exporter.get_formatted_binary_double('||column_name||')'
						when data_type = 'DATE' then 'gcat_exporter.get_formatted_date('||column_name||')'
					end,
					'||'',''||'
				) within group (order by column_id) || '||'' from dual''' || chr(10) ||
					' from ' || table_name || ' order by 1'
				select_sql
			into v_select
			from user_tab_columns
			where table_name = gcat_helper.c_ordered_objects(i)
			group by table_name;

			--Run the select and get all the data.
			begin
				execute immediate v_select
				bulk collect into v_rows;
			exception when others then
				dbms_output.put_line(v_select);
				raise_application_error(-20000, 'Problem with this SQL statement:'||chr(10)||v_select);
			end;

			--Create the INSERTs.
			for j in 1 .. v_rows.count loop
				--Always start with an INSERT.
				if j = 1 then
					utl_file.put_line(v_handle, 'insert into '||gcat_helper.c_ordered_objects(i));
				end if;

				--Add rows to UNION ALL.
				v_union_all := v_union_all || v_rows(j) || ' union all' || chr(10);

				--Package the rows N rows at a time, or for the last row.
				if j = v_rows.count or remainder(j, c_rows_per_chunk) = 0 then
					--Print the N rows and reset.  (Don't print the last " union all".)
					utl_file.put_line(v_handle, substr(v_union_all, 1, length(v_union_all) - 11) || ';');
					v_union_all := null;

					--Print another INSERT, unless it's the last row.
					if j <> v_rows.count then
						utl_file.put_line(v_handle, 'insert into '||gcat_helper.c_ordered_objects(i));
					end if;
				end if;
			end loop;

			--Enable self-referencing foreign keys.
			if gcat_helper.c_ordered_objects(i) = 'ORGANIZATION' then
				utl_file.put_line(v_handle, chr(10)||chr(10)||
					'ALTER TABLE "ORGANIZATION" ENABLE CONSTRAINT "FK_ORGANIZATION_ORGANIZATION";');
			end if;

		end loop;

		utl_file.fclose(v_handle);
	end write_metadata_and_data;


	---------------------------------------
	procedure write_move_and_rebuild is
		v_handle utl_file.file_type;
	begin
		v_handle := get_handle(p_open_mode => 'a');

		utl_file.new_line(v_handle);
		utl_file.new_line(v_handle);
		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
		utl_file.put_line(v_handle, '-- Compress tables and rebuild indexes.');
		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
		utl_file.new_line(v_handle);
		utl_file.put_line(v_handle, 'exec dbms_output.put_line(''Compressing tables and rebuilding indexes...'');');
		utl_file.new_line(v_handle);

		--Create MOVE and REBUILD for each table and its indexes.
		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			utl_file.put_line(v_handle, 'alter table '||gcat_helper.c_ordered_objects(i)||' move compress;');

			for indexes_to_rebuild in
			(
				select index_name
				from all_indexes
				where owner = sys_context('userenv', 'CURRENT_SCHEMA')
					and table_name = gcat_helper.c_ordered_objects(i)
				order by 1
			) loop
				utl_file.put_line(v_handle, 'alter index '||indexes_to_rebuild.index_name||' rebuild;');
			end loop;
		end loop;

		utl_file.fclose(v_handle);
	end;


	---------------------------------------
	procedure write_footer is
		v_handle utl_file.file_type;
	begin
		v_handle := get_handle(p_open_mode => 'a');

		utl_file.new_line(v_handle);
		utl_file.new_line(v_handle);
		utl_file.new_line(v_handle);
		utl_file.put_line(v_handle, replace(substr(
		q'[
				-- Print outro message.
				exec dbms_output.put_line('------------------------------------------------------------------------');
				exec dbms_output.put_line('-- Done.  Gcatdb was successfully installed.');
				exec dbms_output.put_line('------------------------------------------------------------------------');
				]'
		, 2)
		, '				'));

		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
		utl_file.put_line(v_handle, '-- DONE');
		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');

		utl_file.fclose(v_handle);
	end write_footer;

begin
	write_header;
	write_check_for_existing_data;
	write_metadata_and_data;
	write_move_and_rebuild;
	write_footer;
end generate_oracle_file;


--==============================================================================
--==============================================================================
procedure generate_postgres_file is

	---------------------------------------
	function get_handle(p_open_mode in varchar2) return utl_file.file_type is
		v_handle utl_file.file_type;
	begin
		v_handle := utl_file.fopen(c_export_directory, 'postgres_create_gcatdb.sql', open_mode => p_open_mode);
		return v_handle;
	end get_handle;


	---------------------------------------
	procedure write_header is
		v_handle utl_file.file_type;
	begin
		v_handle := get_handle(p_open_mode => 'w');
		utl_file.put_line(v_handle, replace(replace(replace(replace(substr(
		q'[
				-- This file creates the gcatdb schema for Postgres databases.
				-- DO NOT MODIFY THIS FILE.  It is automatically generated.

				-- Print intro message.
				\echo ------------------------------------------------------------------------
				\echo -- Installing gcatdb.
				\echo --
				\echo -- Data from GCAT (J. McDowell, planet4589.org/space/gcat)
				\echo -- Schema and scripts from Jon Heller, jon@jonheller.org
				\echo -- The database installs #TABLE_COUNT# tables and uses about 55MB of space.
				\echo --
				\echo -- The installation will run for about a minute and will stop on any
				\echo -- errors.  You should see a "done" message at the end.
				\echo ------------------------------------------------------------------------

				-- Session settings.
				set client_encoding to 'utf8';

				]'
		, 2)
		, '				')
		, '#VERSION#', c_version)
		, '#DATE#', to_char(sysdate, 'YYYY-MM-DD'))
		, '#TABLE_COUNT#', gcat_helper.c_ordered_objects.count));

		utl_file.fclose(v_handle);
	end write_header;


	---------------------------------------
	procedure write_metadata_and_data is
		v_handle utl_file.file_type;
		v_metadata varchar2(32767);
	begin
		v_handle := get_handle(p_open_mode => 'a');

		--Tables:
		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			--Table header:
			utl_file.new_line(v_handle);
			utl_file.new_line(v_handle);
			utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
			utl_file.put_line(v_handle, '-- '||gcat_helper.c_ordered_objects(i));
			utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
			utl_file.new_line(v_handle);

			--Metadata for tables:
			v_metadata := get_metadata('TABLE', nvl(sys_context(user, 'CURRENT_SCHEMA'), user), gcat_helper.c_ordered_objects(i)) || ';';

			--Change it to a Postgres format.
			--Get rid of double quotes.
			v_metadata := replace(v_metadata, '"');
			--Change VARCHAR2 to VARCHAR.
			v_metadata := replace(v_metadata, 'VARCHAR2', 'VARCHAR');
			--Change NUMBER to NUMERIC.
			v_metadata := replace(v_metadata, ' NUMBER', ' NUMERIC');
			--Change BINARY_DOUBLE to DOUBLE PRECISION.
			v_metadata := replace(v_metadata, ' BINARY_DOUBLE', ' DOUBLE PRECISION');
			--Remove character length semantics
			v_metadata := replace(v_metadata, ' CHAR)', ')');
			--Remove "USING INDEX".
			v_metadata := replace(v_metadata, 'USING INDEX');
			--Remove "ENABLE".
			v_metadata := replace(v_metadata, 'ENABLE');


			utl_file.put_line(v_handle, replace(v_metadata, chr(10)||'  ', chr(10)));
			utl_file.new_line(v_handle);

			--Metadata for indexes:
			for indexes_to_create in
			(
				select table_owner, index_name
				from user_indexes
				where
					table_name = gcat_helper.c_ordered_objects(i)
					--Primary key and unique keys are created as part of table definition.
					and index_name not like 'PK%'
					and index_name not like 'UQ%'
				order by index_name
			) loop
				v_metadata := get_metadata('INDEX', indexes_to_create.table_owner, indexes_to_create.index_name) || ';';
				--Change it to a Postgres format.
				--Get rid of double quotes.
				v_metadata := replace(v_metadata, '"');
				utl_file.put_line(v_handle, replace(v_metadata, chr(10)||'  ', chr(10)));
			end loop;

			--Data:
			utl_file.put_line(v_handle, replace(
				q'[\copy $TABLE_NAME$ from 'c:\gcatdb\exports\$TABLE_NAME$.csv' delimiter ',' csv header;]'
				, '$TABLE_NAME$', gcat_helper.c_ordered_objects(i)));
		end loop;

		utl_file.fclose(v_handle);
	end write_metadata_and_data;


	---------------------------------------
	procedure write_footer is
		v_handle utl_file.file_type;
	begin
		v_handle := get_handle(p_open_mode => 'a');

		utl_file.new_line(v_handle);
		utl_file.new_line(v_handle);
		utl_file.new_line(v_handle);
		utl_file.put_line(v_handle, replace(substr(
		q'[
				-- Print outro message.
				\echo ------------------------------------------------------------------------
				\echo Done.  Gcatdb was successfully installed.
				\echo ------------------------------------------------------------------------
				]'
		, 2)
		, '				'));

		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');
		utl_file.put_line(v_handle, '-- DONE');
		utl_file.put_line(v_handle, '--------------------------------------------------------------------------------');

		utl_file.fclose(v_handle);
	end write_footer;

begin
	write_header;
	write_metadata_and_data;
	write_footer;
end generate_postgres_file;


--==============================================================================
--==============================================================================
procedure generate_csv_files is
	v_column_count number;
	v_order_by_expression varchar2(1000);
begin
	for i in 1 .. gcat_helper.c_ordered_objects.count loop

		-- Get the number of columns.
		select count(*)
		into v_column_count
		from user_tab_cols
		where table_name = gcat_helper.c_ordered_objects(i);

		-- Order by up to 3 columns, if possible.
		if v_column_count = 1 then
			v_order_by_expression := '1';
		elsif v_column_count = 2 then
			v_order_by_expression := '1,2';
		else
			v_order_by_expression := '1,2,3';
		end if;			

		-- Generate the CSV file.
		begin
			data_dump
			(
				-- Sort by "1", "1,2", or "1,2,3" depending on the column count.
				query_in        => 'select * from ' || gcat_helper.c_ordered_objects(i) || ' order by ' || v_order_by_expression,
				file_in         => gcat_helper.c_ordered_objects(i) || '.csv',
				directory_in    => c_export_directory,
				nls_date_fmt_in => 'YYYY-MM-DD HH24:MI:SS',
				delimiter_in    => ',',
				header_row_in   => true
			);
		exception when others then
			raise_application_error(-20000, 'Problems with '||gcat_helper.c_ordered_objects(i)||'.'||chr(10)||
				sys.dbms_utility.format_error_stack||sys.dbms_utility.format_error_backtrace);
		end;
	end loop;
end;

end;
/
