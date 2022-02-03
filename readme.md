# gcatdb
(EXPERIMENTAL - DO NOT USE YET) Oracle schema for the General Catalog of Artificial Space Objects


Main architectural differences between GCAT and GCATDB
	Database schemas are simpler when the columns are dumb but the schema is smart.
	Whenever GCAT combines multiple values into a single column, GCATDB will break multiple values into either separate columns or separate table.
	For example,

Column names:
	Column names have a unique prefix plus the rest of the name based on the GCAT name. The prefix helps identify the source of the column in complicated SQL statements where there would otherwise be multiple columns with the same name.
	Names that are normal words are separated by an underscore. For example, "ShortName" becomes short_name, but "EName" stays as ename.
	Names that reference other columns have that column in the name. For example, the "Parent" column in ORGANIZATION is named "O_PARENT_O_CODE", to make it obvious which column it references.

Full list of instances where GCAT text data is not identically mapped to the relational model of GCATDB:
	Vague dates, which contain both date and precision, were converted to two separate columns to store the date and precision.
	Question marks that are used to denote the certainty of data were removed. (TODO - Perhaps I should add a _IS_CERTAIN column?)
	"sites.tsv"."Site" is stored as SITE.S_CODE. (While "sites.tsv" has an empty "Code" for backwards compatibility, the database prefers name consistency over text file backwards compatibility. (TODO - Change this?)
	spin.tsv was combind into worlds.tsv to make a single WORLD table.
	Most number string are converted to numbers. Values that might include infinity, such as the satellite apogee, are stored as BINARY_DOUBLE which supports infinity.

	payload.TLast - "*" is translated to NULL, which means that no information is available.
	payload.TOp - "*" is translated to NULL, which means the payload is beleived to still be operating
	payload.TDate - "*" is translated to NULL, which means the payload is believed to still be transmitting
	Payload Category is converted from a list of values into a separate table - PAYLOAD_CATEGORY

	Payload UNState - The presence of brackets is stored in PAY_IS_REGISTERERD. (The asterisk can be inferred with other columns.)


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

ORGANIZATION
	ORGANIZATION_ORG_TYPE
		ORGANIZATION_TYPE

SITE
	SITE_ORG

PLATFORM
	PLATFORM_ORG

LAUNCH_POINT
	LAUNCH_POINT_ORG

LAUNCH_VEHICLE
	LAUNCH_VEHICLE_ORG
	LAUNCH_VEHICLE_FAMILY

STAGE
	STAGE_MANUFACTURER

PROPELLANT

ENGINE
	ENGINE_MANUFACTURER
	ENGINE_PROPELLANT

LAUNCH_VEHICLE_STAGE

REFERENCE

WORLD
