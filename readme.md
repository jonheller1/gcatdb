GCATDB
======================================

(EXPERIMENTAL - DO NOT USE YET) Oracle schema for the General Catalog of Artificial Space Objects


Main architectural differences between GCAT and GCATDB
	Database schemas work better when the columns are dumb but the schema is smart. So there are two main ways that the database differs from the text files:
	1. Concatenated value are broken into multiple columns. A value like '[US]*' will become three columns to represent the text, the presence of brackets, and the prsence of an asterisk.
	2. Lists of values are broken into a multiple rows in a linked table. A value like 'US/EU' will be stored as two rows in a child table that refers to the row in the parent table.
	3. Column names are the same as in GCAT except they are prefixed with a table abbreviation. For example, organization "Code" is named O_CODE.
	4. Column names that reference another column have that exact name at the end. For example, organization "Parent" is named O_PARENT_O_CODE.

	Although these changes make it a bit harder to view the data, they make it much easier to join and filter the data.

Tables:

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


Full list of instances where GCAT text data is not identically mapped to the relational model of GCATDB:
	Vague dates, which contain both date and precision, were converted to two separate columns to store the date and precision.
	Question marks that are used to denote the certainty of data were removed. (TODO - Perhaps I should add a _IS_CERTAIN column?)
	spin.tsv was combind into worlds.tsv to make a single WORLD table.
	Most number string are converted to numbers. Values that might include infinity, such as the satellite apogee, are stored as BINARY_DOUBLE which supports infinity.


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



TODO: how to install, etc


How to Load Data Into Your Database
-----------------------------------

TODO: Steps for local databases and OCI databases?


How to Modify or Reload GCATDB
------------------------------

Most users can simply see the previous step about loading data. These steps are only for users who want to modify or recreate the process for building the GCATDB from scratch.

See these files for instructions on building GCATDB from scratch:
01_setup_local.sql
02_setup_oci.txt
03_reload_local.sql
04_reload_oci.sql


License
-------

The database schema and supporting code are licensed by Jon Heller under the LGPLv3.

Data is Creative Commons CC-BY from GCAT (J. McDowell, planet4589.org/space/gcat)
