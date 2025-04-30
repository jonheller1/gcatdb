GCATDB
======================================

(UNSTABLE ALPHA VERSION - The data should be accurate, but table and columns names may unexpectedly change in the future.) A relational schema built out of Jonathan McDowell's [General Catalog of Artificial Space Objects.](https://planet4589.org/space/gcat/) (This project is not affiliated with Jonathan McDowell.)



How to access the data online
-----------------------------
TODO. I plan to build an online query capability.



How to import the data into your local database
-----------------------------------------------
**Oracle - Datapump import - easiest if you have access to the server file system:**
1. Find an available Oracle and filesystem directory. For example, most Oracle database have a directory named `DATA_PUMP_DIR`, and you can find the directory path with this SQL: `select directory_path from all_directories where directory_name = 'DATA_PUMP_DIR';`
2. Download the file [exports/GCATDB_ORACLE.dmp](exports/GCATDB_ORACLE.dmp) into the above directory.
3. Run this command to import the dump file: `impdp your_username/your_password/@your_database directory=data_pump_dir dumpfile=GCATDB_ORACLE.dmp REMAP_SCHEMA=jheller:your_username logfile=GCATDB_ORACLE_EXPORT.log`

**Oracle - SQL files - easiest if you do not have access to the server file system: (in progress)**

1. Download and unzip oracle_create_gcatdb.sql.zip.
2. CD into the directory with that file.
3. Set the command line to work with a UTF8 file.  In Windows this should work:

		C:\gcatdb\exports> set NLS_LANG=American_America.UTF8

	In Unix this should work:

		export NLS_LANG=American_America.UTF8

4. Start SQL\*Plus as a user who can either create another schema or can load tables and data into their own schema.
5. If you need to create a schema to hold the data, run commands like the ones below. (You may need to change "users" to "data" or some other tablespace name, depending on your configuration.)

		SQL> create user gcatdb identified by "enterPasswordHere#1" quota unlimited on users;
		SQL> alter session set current_schema = gcatdb;

6. Run this command to install the tables and data.  It should only take a minute.

		SQL> @oracle_create_gcatdb.sql

**Postgres Instructions: (in progress)**

1. Download and unzip csv_files.zip.
2. Download postgres_create_space.sql.
3. Modify postgres_create_space.sql to reference the correct directory that contains the CSV files.
4. Start psql and run this command:

		postgres=# \i postgres_create_space.sql


**Other Database (Partial) Instructions: (in progress)**

1. Download and unzip csv_files.zip.
2. Load each of the CSV files.



How to build GCATDB from scratch on your local system (only necessary if you want to rebuild export files yourself)
-----------------------------------------------------
This is much less convenient than the export files, but if you want to replace my ELT process, use these files:
* code/01_setup_local.sql
* code/02_reload_local.sql



How to recreate GCATDB on OCI
-----------------------------
TODO, but these are guides that I've used in the past.
* code/03_setup_oci.txt
* code/04_reload_oci.sql



Main architectural differences between GCAT and GCATDB
------------------------------------------------------
Database schemas work better when the columns are dumb but the schema is smart. There are three main ways that the database differs from the original text files:

1. **No concatenated values.** Concatenated values are broken into multiple columns. For example, instead of storing the Launch Date in the vague date format as the string "1943 Nov 25 0100?", the schema stores the date in a `DATE` column named `L_LAUNCH_DATE` and stores the precision value of 'Minutes' in a separate `VARCHAR2` column named `L_LAUNCH_DATE_PRECISION`.
2. **No lists of values.** Lists of values are broken into a multiple rows in a linked table. A value like 'US/EU' will be stored as two rows in a child table that references the row in the parent table.
3. **Hungarian notation.** Each column name is prefixed with a simple abbreviation for the table name. And columns with referential integrity will use that precise column name as a suffix, to make it clear what table and column is being referred to. For example, the organization code is stored as `ORGANIZATION.O_CODE`. The organization table also has a parent code, named `ORGANIZATION.O_PARENT_O_CODE.`

These changes make it harder to view the data, but they make it much easier to query and filter the data and they make it more obvious when the columns are joined incorrectly.



Tables
------

```
LAUNCH
	LAUNCH_AGENCY_ORG
	LAUNCH_PAYLOAD_ORG
	LAUNCH_INVESTIGATOR

SATELLITE
	SATELLITE_OWNER_ORG
	SATELLITE_MANUFACTURER_ORG

PAYLOAD
	PAYLOAD_CATEGORY
	PAYLOAD_DISCIPLINE

ORGANIZATION
	ORGANIZATION_ORG_TYPE
		ORGANIZATION_TYPE

SITE
	SITE_ORG

PLATFORM
	PLATFORM_ORG

LAUNCH_POINT
	LAUNCH_POINT_ORG

STAGE
	STAGE_MANUFACTURER

LAUNCH_VEHICLE
	LAUNCH_VEHICLE_ORG
	LAUNCH_VEHICLE_FAMILY
	LAUNCH_VEHICLE_STAGE

ENGINE
	ENGINE_MANUFACTURER
	ENGINE_PROPELLANT

REFERENCE

WORLD
```



Full list of instances where GCAT text data is not identically mapped to the relational model of GCATDB
-------------------------------------------------------------------------------------------------------
TODO:

* Vague dates, which contain both date and precision, were converted to two separate columns to store the date and precision.
* Question marks that are used to denote the certainty of data were removed. (TODO - Perhaps I should add a _IS_CERTAIN column?)
* spin.tsv was combined into worlds.tsv to make a single WORLD table.
* Most number string are converted to numbers. Values that might include infinity, such as the satellite apogee, are stored as BINARY_DOUBLE which supports infinity.

```
Site
	"Site" is stored as SITE_CODE. (GCATDB does not need to use the name "Site" for backwards compatibility.)
Organization
	"Parent" --> o_parent_o_code
Payload
	TLast: "*" is translated to NULL, which means that no information is available.
	TOp: "*" is translated to NULL, which means the payload is beleived to still be operating
	TDate - "*" is translated to NULL, which means the payload is believed to still be transmitting
	Payload Category is converted from a list of values into a separate table - PAYLOAD_CATEGORY
	UNState: The presence of brackets is stored in PAY_IS_REGISTERERD. (The asterisk can be inferred with other columns.)
Launch
	Group nested values are separated into the tables LAUNCH_PAYLOAD_ORG and LAUNCH_INVESTIGATOR.
	"Launch_Code" is split into L_LAUNCH_CATEGORY, L_LAUNCH_STATUS, and L_LAUNCH_SUCCESS_FRACTION.
Engine
	Since there are no decent primary keys, each row is given a unique and meaningless E_ID as the primary key.
	Oxidizer and Fuel are listed in the table ENGINE_PROPELLANT.
Launch Vehicle Stage
	The "Stage Name" is split into LVS_STAGE_NAME (values that actually join to STAGE.STAGE_NAME) and LVS_STAGE_TYPE (generic-named values that do not match any values in STAGE.)
ustacat
	Instead of a separate table, the satellite Unicode name is stored in SATELLITE.S_UNICODE_NAME.
```



License
-------

The database schema and supporting code are licensed by Jon Heller under the LGPLv3.

Data is Creative Commons CC-BY from McDowell, J, 2020: General Catalog of Artificial Space Objects, https://planet4589.org/space/gcat
