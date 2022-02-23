--------------------------------------------------------------------------------
-- Prerequisites for local development database - one time step
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
-- Design - one time step
--------------------------------------------------------------------------------

Cloud database users:
	admin - manages data
	gcat - owns objects and data
	opengcat - read-only connection open to the public

Local database packages:
	todo:
	gcat_helper
	gcat_loader
;


--------------------------------------------------------------------------------
-- Create configuration view based on expected headers and how to handle them - one time step.
--------------------------------------------------------------------------------

create or replace view gcat_config_vw as
select 'launch.tsv'    file_name, 'LAUNCH_STAGING'    staging_table_name, 73426 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/launch/launch.tsv'    url, '#Launch_Tag	Launch_JD	Launch_Date	LV_Type	Variant	Flight_ID	Flight	Mission	FlightCode	Platform	Launch_Site	Launch_Pad	Ascent_Site	Ascent_Pad	Apogee	Apoflag	Range	RangeFlag	Dest	Agency	Launch_Code	Group	Category	LTCite	Cite	Notes' first_line from dual union all
select 'engines.tsv'   file_name, 'ENGINES_STAGING'   staging_table_name,  1347 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/engines.tsv'   url, '#Name	Manufacturer	Family	Alt_Name	Oxidizer	Fuel	Mass	MFlag	Impulse	ImpFlag	Thrust	TFlag	Isp	IspFlag	Duration	DurFlag	Chambers	Date	Usage	Group' from dual union all
select 'orgs.tsv'      file_name, 'ORGS_STAGING'      staging_table_name,  3270 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/orgs.tsv'      url, '#Code	UCode	StateCode	Type	Class	TStart	TStop	ShortName	Name	Location	Longitude	Latitude	Error	Parent	ShortEName	EName	UName' from dual union all
select 'sites.tsv'     file_name, 'SITES_STAGING'     staging_table_name,   660 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/sites.tsv'     url, '#Site	Code	UCode	Type	StateCode	TStart	TStop	ShortName	Name	Location	Longitude	Latitude	Error	Parent	ShortEName	EName	Group	UName' from dual union all
select 'platforms.tsv' file_name, 'PLATFORMS_STAGING' staging_table_name,   360 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/platforms.tsv' url, '#Code	UCode	StateCode	Type	Class	TStart	TStop	ShortName	Name	Location	Longitude	Latitude	Error	Parent	ShortEName	EName	VClass	VClassID	VID	Group	UName' from dual union all
select 'lp.tsv'        file_name, 'LP_STAGING'        staging_table_name,  2700 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/lp.tsv'        url, '#Site	Code	UCode	Type	StateCode	TStart	TStop	ShortName	Name	Location	Longitude	Latitude	Error	Parent	ShortEName	EName	UName' from dual union all
select 'family.tsv'    file_name, 'FAMILY_STAGING'    staging_table_name,   615 min_expected_rows, 'https://planet4589.org/space/gcat/data/tables/family.tsv'   url, '#Family' from dual union all
select 'lv.tsv'        file_name, 'LV_STAGING'        staging_table_name,  1660 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/lv.tsv'        url, '#LV_Name	LV_Family	LV_Manufacturer	LV_Variant	LV_Alias	LV_Min_Stage	LV_Max_Stage	Length	LFlag	Diameter	DFlag	Launch_Mass	MFlag	LEO_Capacity	GTO_Capacity	TO_Thrust	Class	Apogee	Range' from dual union all
select 'refs.tsv'      file_name, 'REFS_STAGING'      staging_table_name,  3050 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/refs.tsv'      url, '#Cite	Reference' from dual union all
select 'worlds.tsv'    file_name, 'WORLDS_STAGING'    staging_table_name,   285 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/worlds/worlds.tsv'    url, '#IDT	IDName	Name	AltName	Radius	PolarRadius	Mass	SemiMajorAxis	Periapsis	Ecc	Inc	Node	Peri	M	Epoch	RotPeriod	OrbPeriod	Ephemeris	WType	Primary' from dual union all
select 'spin.tsv'      file_name, 'SPIN_STAGING'      staging_table_name,    70 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/worlds/spin.tsv'      url, '#IDT	IDName	Name	Rho	IFac	PoleRA	PoleDec	Meridian	SpinRate	J2	J4	J6	PoleRARate	PoleDecDec	PoleFunc	SpinFunc	InitFunc	JFile' from dual union all
select 'satcat.tsv'    file_name, 'SATCAT_STAGING'    staging_table_name, 50850 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/satcat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'auxcat.tsv'    file_name, 'AUXCAT_STAGING'    staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/auxcat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'ftocat.tsv'    file_name, 'FTOCAT_STAGING'    staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/ftocat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'lcat.tsv'      file_name, 'LCAT_STAGING'      staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/lcat.tsv'         url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'rcat.tsv'      file_name, 'RCAT_STAGING'      staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/rcat.tsv'         url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'tmpcat.tsv'    file_name, 'TMPCAT_STAGING'    staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/tmpcat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'csocat.tsv'    file_name, 'CSOCAT_STAGING'    staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/csocat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'ecat.tsv'      file_name, 'ECAT_STAGING'      staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/ecat.tsv'         url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'deepcat.tsv'   file_name, 'DEEPCAT_STAGING'   staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/deepcat.tsv'      url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'hcocat.tsv'    file_name, 'HCOCAT_STAGING'    staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/hcocat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'lprcat.tsv'    file_name, 'LPRCAT_STAGING'    staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/lprcat.tsv'       url, '#JCAT	Satcat	Piece	Type	Name	PLName	LDate	Parent	SDate	Primary	DDate	Status	Dest	Owner	State	Manufacturer	Bus	Motor	Mass	MassFlag	DryMass	DryFlag	TotMass	TotFlag	Length	LFlag	Diameter	DFlag	Span	SpanFlag	Shape	ODate	Perigee	PF	Apogee	AF	Inc	IF	OpOrbit	OQUAL	AltNames' from dual union all
select 'psatcat.tsv'   file_name, 'PSATCAT_STAGING'   staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/psatcat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	Result	Control	Discipline	Comment' from dual union all
select 'pauxcat.tsv'   file_name, 'PAUXCAT_STAGING'   staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/pauxcat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	Result	Control	Discipline	Comment' from dual union all
select 'pftocat.tsv'   file_name, 'PFTOCAT_STAGING'   staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/pftocat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	Result	Control	Discipline	Comment' from dual union all
select 'plcat.tsv'     file_name, 'PLCAT_STAGING'     staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/plcat.tsv'        url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	Result	Control	Discipline	Comment' from dual union all
select 'usatcat.tsv'   file_name, 'USATCAT_STAGING'   staging_table_name,  6070 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/usatcat.tsv'      url, '#JCAT	Name' from dual union all
select 'stages.tsv'    file_name, 'STAGES_STAGING'    staging_table_name,  1430 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/stages.tsv'    url, '#Stage_Name	Stage_Family	Stage_Manufacturer	Stage_Alt_Name	Length	Diameter	Launch_Mass	Dry_Mass	Thrust	Duration	Engine	NEng' from dual union all
select 'lvs.tsv'       file_name, 'LVS_STAGING'       staging_table_name,  4350 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/lvs.tsv'       url, '#LV_Name	LV_Variant	Stage_No	Stage_Name	Qualifier	Dummy	Multiplicity	Stage_Impulse	Stage_Apogee	Stage_Perigee	Perigee_Qual' from dual
order by file_name;







--------------------------------------------------------------------------------
-- Create Oracle Cloud Infrastructure (OCI) database - one time step
--------------------------------------------------------------------------------

Create Oracle Cloud account.

Open this link to see the login screen: https://cloud.oracle.com/db/adb?region=us-ashburn-1

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
-- Create helper objects - one time step.
--------------------------------------------------------------------------------
@gcat_helper.pck;




--------------------------------------------------------------------------------
-- Test helper functions - one time step.
-- You should see just "PASS" at the end.
--------------------------------------------------------------------------------


declare
	v_failure_count number := 0;
	v_date_string varchar2(100);
	v_date date;
	v_precision varchar2(100);

	procedure compare(p_test_name varchar2, p_actual varchar2, p_expected varchar2) is
	begin
		if p_actual = p_expected or (p_actual is null and p_expected is null) then
			null;
			--dbms_output.put_line('PASS for '||p_test_name||': Expected '||p_expected||' and got '||p_actual);
		else
			v_failure_count := v_failure_count + 1;
			dbms_output.put_line('FAIL for '||p_test_name||': Expected '||p_expected||' but got '||p_actual);
		end if;
	end;

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

	v_date_string := trim('2016 Jun  8 2355:57    '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:55:57' ); compare(v_date_string || ' precision', v_precision, 'Second');
	v_date_string := trim('2016 Jun  8 2355:57?   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:55:57' ); compare(v_date_string || ' precision', v_precision, 'Seconds');
	v_date_string := trim('2016 Jun  8 2355       '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:55:00' ); compare(v_date_string || ' precision', v_precision, 'Minute');
	v_date_string := trim('2016 Jun  8 2355?      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:55:00' ); compare(v_date_string || ' precision', v_precision, 'Minutes');
	v_date_string := trim('2016 Jun  8.98         '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:31:12' ); compare(v_date_string || ' precision', v_precision, 'Centiday');
	v_date_string := trim('2016 Jun  8.98?        '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:31:12' ); compare(v_date_string || ' precision', v_precision, 'Centidays');
	v_date_string := trim('2016 Jun  8 23h        '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:00:00' ); compare(v_date_string || ' precision', v_precision, 'Hour');
	v_date_string := trim('2016 Jun  8 23h?       '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 23:00:00' ); compare(v_date_string || ' precision', v_precision, 'Hours');
	v_date_string := trim('2016 Jun  8.9          '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 21:36:00' ); compare(v_date_string || ' precision', v_precision, 'Hour');
	v_date_string := trim('2016 Jun  8.9?         '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 21:36:00' ); compare(v_date_string || ' precision', v_precision, 'Hours');
	v_date_string := trim('2016 Jun 10.5          '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-10 12:00:00' ); compare(v_date_string || ' precision', v_precision, 'Hour');
	v_date_string := trim('2016 Jun 11.5?         '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-11 12:00:00' ); compare(v_date_string || ' precision', v_precision, 'Hours');
	v_date_string := trim('2016 Jun  8            '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Day');
	v_date_string := trim('2016 Jun  8?           '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Days');
	v_date_string := trim('2016 Jun 30            '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-30 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Day');
	v_date_string := trim('2016 Jun 30?           '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-30 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Days');
	v_date_string := trim('2016 Jun               '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Month');
	v_date_string := trim('2016 Jun?              '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Months');
	v_date_string := trim('2016 Q2                '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-04-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Quarter');
	v_date_string := trim('2016 Q2?               '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-04-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Quarters');
	v_date_string := trim('2016                   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Year');
	v_date_string := trim('2016?                  '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Years');
	v_date_string := trim('2010s                  '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2010-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Decade');
	v_date_string := trim('2010s?                 '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2010-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Decades');
	v_date_string := trim('21C                    '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2000-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Century');
	v_date_string := trim('21C?                   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2000-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Centuries');
	v_date_string := trim('3M                     '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2000-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Millenium');
	v_date_string := trim('3M?                    '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2000-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Millenia');
	--Some weird dates from the ORGS file.
	v_date_string := trim('700?                   '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '0700-01-01 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Years');
	v_date_string := trim('927 Jul 12             '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '0927-07-12 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Day');
	v_date_string := trim('?                      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), ''                    ); compare(v_date_string || ' precision', v_precision, '');
	v_date_string := trim('                       '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), ''                    ); compare(v_date_string || ' precision', v_precision, '');
	v_date_string := trim('-                      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), ''                    ); compare(v_date_string || ' precision', v_precision, '');
	v_date_string := trim('*                      '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), ''                    ); compare(v_date_string || ' precision', v_precision, '');
	--My guess at what BC data will look like.
	v_date_string := trim('BC 146 Jun 28          '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '-0146-06-28 00:00:00'); compare(v_date_string || ' precision', v_precision, 'Day');
	--TODO: Other BC tests?
	--Strings that don't quite match the format.
	v_date_string := trim('2016 Jun   8           '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-08 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Day');
	v_date_string := trim('2016 Jun  18           '); gcat_helper.vague_date_and_precision(v_date_string, v_date, v_precision); compare(v_date_string, trim(to_char(v_date, 'SYYYY-MM-DD HH24:MI:SS')), '2016-06-18 00:00:00' ); compare(v_date_string || ' precision', v_precision, 'Day');

	if v_failure_count = 0 then
		dbms_output.put_line('PASS - No tests failed.');
	else
		raise_application_error(-20000, 'Tests failed. See DBMS_OUTPUT for details.');
	end if;
end;
/




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
-- Download the files into the directory. Takes about 1 minute.
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
		select *
		from gcat_config_vw
		--TEMP for TESTING - only use one file.
		--where file_name = 'satcat.tsv'
		where file_name like '%usatcat.tsv%'
		order by file_name
	) loop
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 1, argument_value => '--output');
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 2, argument_value => v_directory_path || '/' || files.file_name);
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 3, argument_value => '--url');
		dbms_scheduler.set_job_argument_value( job_name => v_name, argument_position => 4, argument_value => files.url);
		dbms_scheduler.run_job(v_name);
	end loop;
end;
/

--Go here to check directory and files:
select directory_path from dba_directories where directory_name = 'DATA_PUMP_DIR';



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
-- Takes about 5 minutes for all tables.
--------------------------------------------------------------------------------

--Recreate staging tables for each file, based on the first header row.
--The staging tables are small wrappers around the files.
declare
	v_sql_template varchar2(32767) :=
	q'[
		create table #STAGING_TABLE_NAME# as
		select /*+ no_gather_optimizer_statistics */
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
				execute immediate 'drop table ' || tables.staging_table_name || ' purge';
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


--ORGANIZATION_TYPE:
create table organization_type compress as
select 'CY'  ot_type, 'Country (i.e. nation-state or autonomous region)'                                                                                              ot_meaning, 'States and similar entities'  ot_group from dual union all
select 'IGO' ot_type, 'Intergovernmental organization. Treated as equivalent to a country for the purposes of tabulations of launches by country etc.'                ot_meaning, 'States and similar entities'  ot_group from dual union all
select 'AP'  ot_type, 'Astronomical Polity: e.g. Luna, Mars. Used for the ''country'' field for locations that are not on Earth and therefore don''t have a country.' ot_meaning, 'States and similar entities'  ot_group from dual union all
select 'E'   ot_type, 'Engine manufacturer'                                                                                                                           ot_meaning, 'Manufacturers'                ot_group from dual union all
select 'LV'  ot_type, 'Launch vehicle manufacturer'                                                                                                                   ot_meaning, 'Manufacturers'                ot_group from dual union all
select 'W'   ot_type, 'Meteorological rocket launch agency or manufacturer'                                                                                           ot_meaning, 'Manufacturers'                ot_group from dual union all
select 'PL'  ot_type, 'Payload manufacturer'                                                                                                                          ot_meaning, 'Manufacturers'                ot_group from dual union all
select 'LA'  ot_type, 'Launch Agency'                                                                                                                                 ot_meaning, 'Operators'                    ot_group from dual union all
select 'S'   ot_type, 'Suborbital payload operator'                                                                                                                   ot_meaning, 'Operators'                    ot_group from dual union all
select 'O'   ot_type, 'Payload owner'                                                                                                                                 ot_meaning, 'Operators'                    ot_group from dual union all
select 'P'   ot_type, 'Parent organization of another entry'                                                                                                          ot_meaning, 'Operators'                    ot_group from dual union all
select 'LS'  ot_type, 'Launch site'                                                                                                                                   ot_meaning, 'Launch origin or destination' ot_group from dual union all
select 'LP'  ot_type, 'Launch position'                                                                                                                               ot_meaning, 'Launch origin or destination' ot_group from dual union all
select 'LC'  ot_type, 'Launch cruise'                                                                                                                                 ot_meaning, 'Launch origin or destination' ot_group from dual union all
select 'LZ'  ot_type, 'Launch zone'                                                                                                                                   ot_meaning, 'Launch origin or destination' ot_group from dual union all
select 'TGT' ot_type, 'Suborbital target area'                                                                                                                        ot_meaning, 'Launch origin or destination' ot_group from dual;

alter table organization_type add constraint pk_organization_type primary key (ot_type);


--ORGANIZATION:
create table organization compress as
select o_code, o_ucode, o_statecode, o_class,
	gcat_helper.vague_to_date(o_tstart) o_tstart,
	gcat_helper.vague_to_precision(o_tstart) o_tstart_precision,
	gcat_helper.vague_to_date(o_tstop) o_tstop,
	gcat_helper.vague_to_precision(o_tstop) o_tstop_precision,
	o_shortname,
	o_name,
	o_location,
	gcat_helper.gcat_to_number(o_longitude) o_longitude,
	gcat_helper.gcat_to_number(o_latitude) o_latitude,
	gcat_helper.gcat_to_number(o_error) o_error,
	o_parent_o_code,
	o_shortename,
	o_ename,
	o_uname
from
(
	--Fix data issues.
	select
		o_code,
		o_ucode,
		o_statecode,
		o_class,
		o_tstart,
		replace(o_tstop, '2015 Feb ?', '2015 Feb?') o_tstop,
		o_shortname,
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
		o_shortename,
		o_ename,
		o_uname
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null_and_trim("Code"      ) o_code,
			gcat_helper.convert_null_and_trim("UCode"     ) o_ucode,
			gcat_helper.convert_null_and_trim("StateCode" ) o_statecode,
			gcat_helper.convert_null_and_trim("Class"     ) o_class,
			gcat_helper.convert_null_and_trim("TStart"    ) o_tstart,
			gcat_helper.convert_null_and_trim("TStop"     ) o_tstop,
			gcat_helper.convert_null_and_trim("ShortName" ) o_shortname,
			gcat_helper.convert_null_and_trim("Name"      ) o_name,
			gcat_helper.convert_null_and_trim("Location"  ) o_location,
			gcat_helper.convert_null_and_trim("Longitude" ) o_longitude,
			gcat_helper.convert_null_and_trim("Latitude"  ) o_latitude,
			gcat_helper.convert_null_and_trim("Error"     ) o_error,
			gcat_helper.convert_null_and_trim("Parent"    ) o_parent,
			gcat_helper.convert_null_and_trim("ShortEName") o_shortename,
			gcat_helper.convert_null_and_trim("EName"     ) o_ename,
			gcat_helper.convert_null_and_trim("UName"     ) o_uname
		from orgs_staging
	) rename_columns
) fix_data;

alter table organization add constraint pk_organization primary key(o_code);
alter table organization add constraint fk_organization_organization foreign key (o_parent_o_code) references organization(o_code);


--ORGANIZATION_ORG_TYPE
create table organization_org_type compress as
-- Shrink columns a bit to avoid this error when creating a primary key later: ORA-01450: maximum key length (6398) exceeded
select cast("Code" as varchar2(100)) oot_o_code, column_value oot_ot_type
from orgs_staging
cross join gcat_helper.get_nt_from_list("Type", '/')
where "Type" <> '-'
order by oot_o_code;

alter table organization_org_type add constraint pk_organization_org_type primary key(oot_o_code, oot_ot_type);
alter table organization_org_type add constraint fk_organization_org_type_organization foreign key(oot_o_code) references organization(o_code);
alter table organization_org_type add constraint fk_organization_org_type_organization_type foreign key(oot_ot_type) references organization_type(ot_type);


--SITE
create table site compress as
select
	site_code,
	site_ucode,
	site_type,
	site_statecode,
	gcat_helper.vague_to_date(site_tstart) site_tstart,
	gcat_helper.vague_to_precision(site_tstart) site_tstart_precision,
	gcat_helper.vague_to_date(site_tstop) site_tstop,
	gcat_helper.vague_to_precision(site_tstop) site_tstop_precision,
	site_shortname,
	site_name,
	site_location,
	gcat_helper.gcat_to_number(site_longitude) site_longitude,
	gcat_helper.gcat_to_number(site_latitude) site_latitude,
	gcat_helper.gcat_to_number(site_error) site_error,
	site_shortename,
	site_ename,
	site_group,
	site_uname
from
(
	--Fix data issues.
	select
		site_code,
		site_ucode,
		site_type,
		site_statecode,
		replace(site_tstart, '1974 Nov  6:', '1974 Nov  6') site_tstart,
		site_tstop,
		site_shortname,
		site_name,
		site_location,
		site_longitude,
		site_latitude,
		site_error,
		site_shortename,
		site_ename,
		site_group,
		site_uname
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null_and_trim("Site"      ) site_code,
			gcat_helper.convert_null_and_trim("UCode"     ) site_ucode,
			gcat_helper.convert_null_and_trim("Type"      ) site_type,
			gcat_helper.convert_null_and_trim("StateCode" ) site_statecode,
			gcat_helper.convert_null_and_trim("TStart"    ) site_tstart,
			gcat_helper.convert_null_and_trim("TStop"     ) site_tstop,
			gcat_helper.convert_null_and_trim("ShortName" ) site_shortname,
			gcat_helper.convert_null_and_trim("Name"      ) site_name,
			gcat_helper.convert_null_and_trim("Location"  ) site_location,
			gcat_helper.convert_null_and_trim("Longitude" ) site_longitude,
			gcat_helper.convert_null_and_trim("Latitude"  ) site_latitude,
			gcat_helper.convert_null_and_trim("Error"     ) site_error,
			gcat_helper.convert_null_and_trim("ShortEName") site_shortename,
			gcat_helper.convert_null_and_trim("EName"     ) site_ename,
			gcat_helper.convert_null_and_trim("Group"     ) site_group,
			gcat_helper.convert_null_and_trim("UName"     ) site_uname
		from sites_staging
	) rename_columns
) fix_data;

alter table site add constraint pk_site primary key(site_code);


--SITE_ORG
create table site_org compress as
select cast("Site" as varchar2(1000)) so_site_code, replace(column_value, '?') so_o_code
from sites_staging
cross join gcat_helper.get_nt_from_list("Parent", '/')
where "Parent" <> '-'
order by so_site_code;

alter table site_org add constraint pk_site_org primary key(so_site_code, so_o_code);
alter table site_org add constraint fk_site_org_site foreign key(so_site_code) references site(site_code);
alter table site_org add constraint fk_site_org_org foreign key(so_o_code) references organization(o_code);


--PLATFORM
create table platform compress as
select p_code, p_ucode, p_statecode, p_type, p_class,
	gcat_helper.vague_to_date(p_tstart) p_tstart,
	gcat_helper.vague_to_precision(p_tstart) p_tstart_precision,
	gcat_helper.vague_to_date(p_tstop) p_tstop,
	gcat_helper.vague_to_precision(p_tstop) p_tstop_precision,
	p_shortname,
	p_name,
	p_location,
	gcat_helper.gcat_to_number(p_longitude) p_longitude,
	gcat_helper.gcat_to_number(p_latitude) p_latitude,
	gcat_helper.gcat_to_number(p_error) p_error,
	p_shortename,
	p_ename,
	p_vclass,
	p_vclassid,
	p_vid,
	p_group,
	p_uname
from
(
	--Fix data issues.
	select
		p_code,
		p_ucode,
		p_statecode,
		p_type,
		p_class,
		p_tstart,
		p_tstop,
		p_shortname,
		p_name,
		p_location,
		p_longitude,
		p_latitude,
		p_error,
		p_shortename,
		p_ename,
		p_uname,
		p_vclass,
		p_vclassid,
		p_vid,
		p_group
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null_and_trim("Code"      ) p_code,
			gcat_helper.convert_null_and_trim("UCode"     ) p_ucode,
			gcat_helper.convert_null_and_trim("StateCode" ) p_statecode,
			gcat_helper.convert_null_and_trim("Type"      ) p_type,
			gcat_helper.convert_null_and_trim("Class"     ) p_class,
			gcat_helper.convert_null_and_trim("TStart"    ) p_tstart,
			gcat_helper.convert_null_and_trim("TStop"     ) p_tstop,
			gcat_helper.convert_null_and_trim("ShortName" ) p_shortname,
			gcat_helper.convert_null_and_trim("Name"      ) p_name,
			gcat_helper.convert_null_and_trim("Location"  ) p_location,
			gcat_helper.convert_null_and_trim("Longitude" ) p_longitude,
			gcat_helper.convert_null_and_trim("Latitude"  ) p_latitude,
			gcat_helper.convert_null_and_trim("Error"     ) p_error,
			gcat_helper.convert_null_and_trim("ShortEName") p_shortename,
			gcat_helper.convert_null_and_trim("EName"     ) p_ename,
			gcat_helper.convert_null_and_trim("UName"     ) p_uname,
			gcat_helper.convert_null_and_trim("VClass"    ) p_vclass,
			gcat_helper.convert_null_and_trim("VClassID"  ) p_vclassid,
			gcat_helper.convert_null_and_trim("VID"       ) p_vid,
			gcat_helper.convert_null_and_trim("Group"     ) p_group
		from platforms_staging
	) rename_columns
) fix_data;

alter table platform add constraint pk_platform primary key(p_code);


--PLATFORM_ORG
create table platform_org compress as
select cast("Code" as varchar2(1000)) po_p_code, replace(column_value, '?') po_o_code
from platforms_staging
cross join gcat_helper.get_nt_from_list("Parent", '/')
where "Parent" <> '-'
order by po_p_code;

alter table platform_org add constraint pk_platform_org primary key(po_p_code, po_o_code);
alter table platform_org add constraint fk_platform_org_platform foreign key(po_p_code) references platform(p_code);
alter table platform_org add constraint fk_platform_org_org foreign key(po_o_code) references organization(o_code);


--LAUNCH_POINT
create table launch_point compress as
select
	cast(lp_site_code as varchar2(1000)) lp_site_code,
	cast(lp_code as varchar2(1000)) lp_code,
	lp_ucode,
	lp_type,
	lp_statecode,
	gcat_helper.vague_to_date(lp_tstart) lp_tstart,
	gcat_helper.vague_to_precision(lp_tstart) lp_tstart_precision,
	gcat_helper.vague_to_date(lp_tstop) lp_tstop,
	gcat_helper.vague_to_precision(lp_tstop) lp_tstop_precision,
	lp_shortname,
	lp_name,
	lp_location,
	gcat_helper.gcat_to_number(lp_longitude) lp_longitude,
	gcat_helper.gcat_to_number(lp_latitude) lp_latitude,
	gcat_helper.gcat_to_number(lp_error) lp_error,
	lp_shortename,
	lp_ename,
	lp_uname
from
(
	--Fix data.
	select
		lp_site_code,
		lp_code,
		lp_ucode,
		lp_type,
		lp_statecode,
		replace(lp_tstart, '1974 Nov  6:', '1974 Nov  6') lp_tstart,
		replace(lp_tstop, 'DZK3  -', null) lp_tstop,
		lp_shortname,
		lp_name,
		lp_location,
		lp_longitude,
		lp_latitude,
		lp_error,
		lp_shortename,
		lp_ename,
		lp_uname
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null_and_trim("Site"      ) lp_site_code,
			gcat_helper.convert_null_and_trim("Code"      ) lp_code,
			gcat_helper.convert_null_and_trim("UCode"     ) lp_ucode,
			gcat_helper.convert_null_and_trim("Type"      ) lp_type,
			gcat_helper.convert_null_and_trim("StateCode" ) lp_statecode,
			gcat_helper.convert_null_and_trim("TStart"    ) lp_tstart,
			gcat_helper.convert_null_and_trim("TStop"     ) lp_tstop,
			gcat_helper.convert_null_and_trim("ShortName" ) lp_shortname,
			gcat_helper.convert_null_and_trim("Name"      ) lp_name,
			gcat_helper.convert_null_and_trim("Location"  ) lp_location,
			gcat_helper.convert_null_and_trim("Longitude" ) lp_longitude,
			gcat_helper.convert_null_and_trim("Latitude"  ) lp_latitude,
			gcat_helper.convert_null_and_trim("Error"     ) lp_error,
			gcat_helper.convert_null_and_trim("ShortEName") lp_shortename,
			gcat_helper.convert_null_and_trim("EName"     ) lp_ename,
			gcat_helper.convert_null_and_trim("UName"     ) lp_uname
		from lp_staging
	) rename_columns
) fix_data;

alter table launch_point add constraint pk_launch_point primary key(lp_site_code, lp_code);
alter table launch_point add constraint fk_launch_point_site foreign key (lp_site_code) references site(site_code);


--LAUNCH_POINT_ORG
create table launch_point_org compress as
select
	cast("Site" as varchar2(1000)) lpo_site_code,
	cast("Code" as varchar2(1000)) lpo_lp_code,
	replace(replace(replace(column_value, '?'), 'PRC', 'CN'), 'DNVG', 'DVNG') lpo_o_code
from lp_staging
cross join gcat_helper.get_nt_from_list("Parent", '/')
where "Parent" <> '-'
order by 1,2;

alter table launch_point_org add constraint pk_launch_point_org primary key(lpo_site_code, lpo_lp_code, lpo_o_code);
alter table launch_point_org add constraint fk_launch_point_org_launch_point foreign key(lpo_site_code, lpo_lp_code) references launch_point(lp_site_code, lp_code);
alter table launch_point_org add constraint fk_launch_point_org_org foreign key(lpo_o_code) references organization(o_code);


--LAUNCH_VEHICLE_FAMILY
create table launch_vehicle_family compress as
--FIX: Added distinct because there are duplicates.
select distinct "Family" lvf_family
from family_staging
where "Family" <> '-'
order by 1;

alter table launch_vehicle_family add constraint pk_launch_vehicle_family primary key(lvf_family);


--LAUNCH_VEHICLE
create table launch_vehicle compress as
select
	cast(lv_name as varchar2(1000)) lv_name,
	lv_lvf_family,
	lv_variant,
	lv_alias,
	gcat_helper.gcat_to_number(lv_min_stage) lv_min_stage,
	gcat_helper.gcat_to_number(lv_max_stage) lv_max_stage,
	gcat_helper.gcat_to_number(lv_length) lv_length,
	lv_lflag,
	gcat_helper.gcat_to_number(lv_diameter) lv_diameter,
	lv_dflag,
	gcat_helper.gcat_to_number(lv_launch_mass) lv_launch_mass,
	lv_mflag,
	gcat_helper.gcat_to_number(lv_leo_capacity) lv_leo_capacity,
	gcat_helper.gcat_to_number(lv_gto_capacity) lv_gto_capacity,
	gcat_helper.gcat_to_number(lv_to_thrust) lv_to_thrust,
	lv_class,
	gcat_helper.gcat_to_number(lv_apogee) lv_apogee,
	gcat_helper.gcat_to_number(lv_range) lv_range
from
(
	--Rename columns.
	select
		gcat_helper.convert_null_and_trim("LV_Name"        ) lv_name,
		gcat_helper.convert_null_and_trim("LV_Family"      ) lv_lvf_family,
		gcat_helper.convert_null_and_trim("LV_Variant"     ) lv_variant,
		gcat_helper.convert_null_and_trim("LV_Alias"       ) lv_alias,
		gcat_helper.convert_null_and_trim("LV_Min_Stage"   ) lv_min_stage,
		gcat_helper.convert_null_and_trim("LV_Max_Stage"   ) lv_max_stage,
		gcat_helper.convert_null_and_trim("Length"         ) lv_length,
		gcat_helper.convert_null_and_trim("LFlag"          ) lv_lflag,
		gcat_helper.convert_null_and_trim("Diameter"       ) lv_diameter,
		gcat_helper.convert_null_and_trim("DFlag"          ) lv_dflag,
		gcat_helper.convert_null_and_trim("Launch_Mass"    ) lv_launch_mass,
		gcat_helper.convert_null_and_trim("MFlag"          ) lv_mflag,
		gcat_helper.convert_null_and_trim("LEO_Capacity"   ) lv_leo_capacity,
		gcat_helper.convert_null_and_trim("GTO_Capacity"   ) lv_gto_capacity,
		gcat_helper.convert_null_and_trim("TO_Thrust"      ) lv_to_thrust,
		gcat_helper.convert_null_and_trim("Class"          ) lv_class,
		gcat_helper.convert_null_and_trim("Apogee"         ) lv_apogee,
		gcat_helper.convert_null_and_trim("Range"          ) lv_range
	from lv_staging
);

--(Nullable column LV_VARIANT prevents primary key)
alter table launch_vehicle add constraint uq_launch_vehicle unique(lv_name, lv_variant);
alter table launch_vehicle add constraint fk_launch_vehicle_launch_vehicle_family foreign key (lv_lvf_family) references launch_vehicle_family(lvf_family);


--LAUNCH_VEHICLE_ORG
create table launch_vehicle_org compress as
select
	cast(gcat_helper.convert_null_and_trim("LV_Name"   ) as varchar2(1000)) lvo_lv_name,
	cast(gcat_helper.convert_null_and_trim("LV_Variant") as varchar2(1000)) lvo_lv_variant,
	--FIXES:
	replace(replace(replace(replace(column_value, '?'),
		'SKYRO','SKYR'),
		'ROKTSN','ROKSN'),
		'NCSIST','') --TODO: Missing data from orgs.tsv?
	lvo_o_code
from lv_staging
cross join gcat_helper.get_nt_from_list("LV_Manufacturer", '/')
where "LV_Manufacturer" <> '-'
order by 1,2;

alter table launch_vehicle_org add constraint uq_launch_vehicle_org unique(lvo_lv_name, lvo_lv_variant, lvo_o_code);
alter table launch_vehicle_org add constraint fk_launch_vehicle_org_launch_vehicle foreign key(lvo_lv_name, lvo_lv_variant) references launch_vehicle(lv_name, lv_variant);
alter table launch_vehicle_org add constraint fk_launch_vehicle_org_org foreign key(lvo_o_code) references organization(o_code);


--REFERENCE
create table reference compress as
select
	r_cite,
	r_reference
from
(
	--Rename columns.
	select
		gcat_helper.convert_null_and_trim("Cite"      ) r_cite,
		gcat_helper.convert_null_and_trim("Reference" ) r_reference
	from refs_staging
) rename_columns;

alter table reference add constraint pk_reference primary key(r_cite);


--LAUNCH
create table launch nologging compress as
select /*+ no_gather_optimizer_statistics */
	l_launch_tag,
	gcat_helper.gcat_to_number(l_launch_jd) l_launch_jd,
	gcat_helper.vague_to_date(l_launch_date) l_launch_date,
	gcat_helper.vague_to_precision(l_launch_date) l_launch_date_precision,
	l_lv_name,
	l_lv_variant,
	l_flight_id,
	l_flight,
	l_mission,
	l_flightcode,
	l_p_code,
	l_launch_lp_site_code,
	l_launch_lp_code,
	l_ascent_lp_site_code,
	l_ascent_lp_code,
	gcat_helper.gcat_to_number(l_apogee) l_apogee,
	l_apoflag,
	gcat_helper.gcat_to_number(l_range) l_range,
	l_rangeflag,
	l_dest,
	l_launch_code,
	l_group,
	l_category,
	l_primary_r_cite,
	l_additional_r_cite,
	l_notes
from
(
	--Fix data
	select
		l_launch_tag,
		l_launch_jd,
		--FIX:
		replace(replace(replace(replace(replace(l_launch_date
			, '1963 Jun   5', '1963 Jun  5')
			, '1963 Jun  25', '1963 Jun 25')
			, '1963 Jun  26', '1963 Jun 26')
			, '1971 Mar 24 1832:0', '1971 Mar 24 1832:00')
			, '1971 Jul 31 2334:0', '1971 Jul 31 2334:00')
		l_launch_date,
		--FIX:
		case
			when l_lv_name = 'Minotaur-C 3210' then 'Minotaur C'
			when l_lv_name = 'Ghadr-110' then 'Ghadr 1'
			when l_lv_name = 'Angara A5/Persei' then 'Angara A5'
			else l_lv_name
		end l_lv_name,
		replace(l_lv_variant, '?') l_lv_variant,
		l_flight_id,
		l_flight,
		l_mission,
		l_flightcode,
		--FIX(?): Remove "?" from end, fix submarine name
		regexp_replace(rtrim(l_p_code, '?'), '^SS-088$', 'SS-083') l_p_code,
		--FIX:
		case
			when l_launch_lp_site_code = 'PSCA' and l_launch_lp_code in ('LP2', 'LP2?') then 'KLC'
			else
				regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
					rtrim(l_launch_lp_site_code, '?')
					,'^NIIP-53$', 'GIK-1')
					,'^NIIP-5$', 'GIK-5')
					,'^GNIIPV$', 'GIK-1')
					,'^GIP-53$', 'GNIIP')
					,'^GTsMP-4$', 'GTsP-4')
					,'^SDSC$', 'SHAR')
					,'^WIMB$', 'NAOTS')
					,'^VSFBS$', 'VS')
					,'^VSFB$', 'V')
					,'^USC', 'KASC')
		end l_launch_lp_site_code,
		--FIX:
		case
			when l_launch_lp_site_code = 'SPFL' and l_launch_lp_code = 'LC47' then 'SLC47'
			when rtrim(l_launch_lp_site_code, '?') = 'JQ' and rtrim(l_launch_lp_code, '?') = 'LC43/95A' then 'LC43/95'
			else
				regexp_replace(regexp_replace(
					rtrim(l_launch_lp_code, '?')
					,'^LC603  ?$', 'LC603')
					--I guessed which one it is
					,'^LC81$', 'LC81/23')
			end l_launch_lp_code,
		regexp_replace(rtrim(l_ascent_lp_site_code, '?')
			,'^DGAEML$', 'CEL')
		l_ascent_lp_site_code,
		--FIX:
		case
			when l_ascent_lp_site_code = 'KMR' and l_ascent_lp_code = 'Lp1' then 'LP1'
			when l_ascent_lp_site_code = 'A51' and l_ascent_lp_code = 'X' then 'X1'
			when l_launch_tag in ('1964-A158', '1964-A172', '1964-A170') and l_ascent_lp_code in ('A', 'A?') then 'ROS A'
			when l_launch_tag in ('1968-A58') and l_ascent_lp_code in ('B') then 'ROS B'
			else rtrim(l_ascent_lp_code, '?')
		end l_ascent_lp_code,
		l_apogee,
		l_apoflag,
		l_range,
		l_rangeflag,
		l_dest,
		l_launch_code,
		l_group,
		l_category,
		l_primary_r_cite,
		l_additional_r_cite,
		l_notes
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null_and_trim("Launch_Tag" ) l_launch_tag,
			gcat_helper.convert_null_and_trim("Launch_JD"  ) l_launch_jd,
			gcat_helper.convert_null_and_trim("Launch_Date") l_launch_date,
			gcat_helper.convert_null_and_trim("LV_Type"    ) l_lv_name,
			gcat_helper.convert_null_and_trim("Variant"    ) l_lv_variant,
			gcat_helper.convert_null_and_trim("Flight_ID"  ) l_flight_id,
			gcat_helper.convert_null_and_trim("Flight"     ) l_flight,
			gcat_helper.convert_null_and_trim("Mission"    ) l_mission,
			gcat_helper.convert_null_and_trim("FlightCode" ) l_flightcode,
			gcat_helper.convert_null_and_trim("Platform"   ) l_p_code,
			gcat_helper.convert_null_and_trim("Launch_Site") l_launch_lp_site_code,
			gcat_helper.convert_null_and_trim("Launch_Pad" ) l_launch_lp_code,
			gcat_helper.convert_null_and_trim("Ascent_Site") l_ascent_lp_site_code,
			gcat_helper.convert_null_and_trim("Ascent_Pad" ) l_ascent_lp_code,
			gcat_helper.convert_null_and_trim("Apogee"     ) l_apogee,
			gcat_helper.convert_null_and_trim("Apoflag"    ) l_apoflag,
			gcat_helper.convert_null_and_trim("Range"      ) l_range,
			gcat_helper.convert_null_and_trim("RangeFlag"  ) l_rangeflag,
			gcat_helper.convert_null_and_trim("Dest"       ) l_dest,
			gcat_helper.convert_null_and_trim("Launch_Code") l_launch_code,
			gcat_helper.convert_null_and_trim("Group"      ) l_group,
			gcat_helper.convert_null_and_trim("Category"   ) l_category,
			gcat_helper.convert_null_and_trim("LTCite"     ) l_primary_r_cite,
			gcat_helper.convert_null_and_trim("Cite"       ) l_additional_r_cite,
			gcat_helper.convert_null_and_trim("Notes"      ) l_notes
		from launch_staging
	) rename_columns
) fix_data;

alter table launch add constraint pk_launch primary key (l_launch_tag);
alter table launch add constraint fk_launch_platform foreign key (l_p_code) references platform(p_code);
alter table launch add constraint fk_launch_launch_site foreign key (l_launch_lp_site_code) references site(site_code);
alter table launch add constraint fk_launch_launch_point  foreign key (l_launch_lp_site_code, l_launch_lp_code) references launch_point(lp_site_code, lp_code);
alter table launch add constraint fk_launch_ascent_site foreign key (l_ascent_lp_site_code) references site(site_code);
alter table launch add constraint fk_launch_ascent_point foreign key (l_ascent_lp_site_code, l_ascent_lp_code) references launch_point(lp_site_code, lp_code);
alter table launch add constraint fk_launch_launch_vehicle foreign key (l_lv_name, l_lv_variant) references launch_vehicle(lv_name, lv_variant);

/*
--Keep these data checks - they may be useful if there are future error

--Check the L_LV_NAME and L_LV_VARIANT matches LAUNCH_VEHICLE.
select *
from launch
left join launch_vehicle
	on l_lv_name = lv_name
	and nvl(l_lv_variant, 'asdf') = nvl(lv_variant, 'asdf')
where lv_name is null;

--Check that "Launch_Site" matches site.site_code.
select *
from launch
left join site
	on launch.l_launch_lp_site_code = site.site_code
where site.site_code is null;

--Check that "Launch_Site" and "Launch_Pad" matches values in Launch_Point.
select l_launch_tag, l_launch_lp_site_code, l_launch_lp_code
from launch
left join launch_point
	on l_launch_lp_site_code = lp_site_code
	and l_launch_lp_code = lp_code
where l_launch_lp_code is not null
	and lp_site_code is null
	and lp_code is null
order by l_launch_lp_site_code;

--Check that "Ascent_Site" matches site.s_code.
select l_launch_tag, l_ascent_lp_site_code
from launch
left join site
	on launch.l_ascent_lp_site_code = site.site_code
where
	l_ascent_lp_site_code is not null
	and site.site_code is null;

--FK_LAUNCH_ASCENT_POINT - Check ascent values match Launch_Point.
select l_launch_tag, l_ascent_lp_site_code, l_ascent_lp_code
from launch
left join launch_point
	on l_ascent_lp_site_code = lp_site_code
	and l_ascent_lp_code = lp_code
where l_ascent_lp_code is not null
	and lp_site_code is null
	and lp_code is null
order by l_ascent_lp_site_code;
*/


--LAUNCH_REFERENCE
--(TODO: This data is perhaps too dirty to process.
-- There are too many rows that don't match, some R_CITEs have "/" in the name, etc.)


--LAUNCH_AGENCY_ORG
create table launch_agency_org compress as
select
	cast(lao_l_launch_tag as varchar2(1000)) lao_l_launch_tag,
	column_value lao_o_code
from
(
	--Get the list of agency orgs by including only things before the last slash.
	select
		"Launch_Tag" lao_l_launch_tag,
		replace("Agency", '?') agency_orgs
	from launch_staging
	where "Agency" <> '-'
) payload_list
cross join gcat_helper.get_nt_from_list(agency_orgs, '/')
where agency_orgs is not null
order by 1,2;

alter table launch_agency_org add constraint pk_launch_agency_org primary key(lao_l_launch_tag, lao_o_code);
alter table launch_agency_org add constraint fk_launch_agency_org_launch foreign key(lao_l_launch_tag) references launch(l_launch_tag);
alter table launch_agency_org add constraint fk_launch_agency_org_org foreign key(lao_o_code) references organization(o_code);


--LAUNCH_PAYLOAD_ORG
create table launch_payload_org compress as
select
	cast(lpo_l_launch_tag as varchar2(1000)) lpo_l_launch_tag,
	--FIX
	case
		when lpo_l_launch_tag = '1964-S509' and column_value = 'PARL' then 'LPARL'
		when lpo_l_launch_tag = '1965-W199' and column_value = 'AGPC' then 'APGC'
		else column_value
	end lpo_o_code
from
(
	--Get the list of payload orgs by including only things before the last slash.
	select
		"Launch_Tag" lpo_l_launch_tag,
		rtrim(regexp_substr(replace("Group", '?'), '.*/'), '/') payload_orgs
	from launch_staging
	where "Group" <> '-'
		--FIX: Ignore this value until RIT exists in orgs file.
		and not ("Launch_Tag" = '2021-S34' and "Group" = 'RIT/Zemcov')
) payload_list
cross join gcat_helper.get_nt_from_list(payload_orgs, '/')
where payload_orgs is not null
order by 1,2;

alter table launch_payload_org add constraint pk_launch_payload_org primary key(lpo_l_launch_tag, lpo_o_code);
alter table launch_payload_org add constraint fk_launch_payload_org_launch foreign key(lpo_l_launch_tag) references launch(l_launch_tag);
alter table launch_payload_org add constraint fk_launch_payload_org_org foreign key(lpo_o_code) references organization(o_code);

/*
--Payload orgs not in organization table.
select *
from launch_payload_org
left join organization
	on lpo_o_code = o_code
where o_code is null;
*/


--LAUNCH_INVESTIGATOR
create table launch_investigator compress as
select
	cast(li_l_launch_tag as varchar2(1000)) li_l_launch_tag,
	column_value li_investigator
from
(
	--Get the list of investigators by removing everything before the last slash.
	select
		"Launch_Tag" li_l_launch_tag,
		regexp_replace(replace("Group", '?'), '.*/') investigators
	from launch_staging
	where "Group" <> '-'
) investigators_list
cross join gcat_helper.get_nt_from_list(investigators, ',')
where investigators is not null;

alter table launch_investigator add constraint pk_launch_investigator primary key(li_l_launch_tag, li_investigator);
alter table launch_investigator add constraint fk_launch_investigator_launch foreign key(li_l_launch_tag) references launch(l_launch_tag);


--WORLD (including spin data)
create table world compress as
select w_id, w_idname, w_name, w_altname,
	gcat_helper.gcat_to_number(w_radius) w_radius,
	gcat_helper.gcat_to_number(w_polar_radius) w_polar_radius,
	gcat_helper.gcat_to_number(w_mass) w_mass,
	gcat_helper.gcat_to_number(w_semimajoraxis) w_semimajoraxis,
	gcat_helper.gcat_to_number(w_periapsis) w_periapsis,
	gcat_helper.gcat_to_number(w_ecc) w_ecc,
	gcat_helper.gcat_to_number(w_inc) w_inc,
	gcat_helper.gcat_to_number(w_node) w_node,
	gcat_helper.gcat_to_number(w_peri) w_peri,
	gcat_helper.gcat_to_number(w_m) w_m,
	gcat_helper.vague_to_date(w_epoch) w_epoch,
	gcat_helper.vague_to_precision(w_epoch) w_epoch_precision,
	gcat_helper.gcat_to_number(w_rotperiod) w_rotperiod,
	gcat_helper.gcat_to_number(w_orbperiod) w_orbperiod,
	w_ephemeris,
	w_WType,
	w_primary_w_name,
	gcat_helper.gcat_to_number(w_spin_rho) w_spin_rho,
	gcat_helper.gcat_to_number(w_spin_intertial_factor) w_spin_intertial_factor,
	gcat_helper.gcat_to_number(w_spin_icrs_position_ra) w_spin_icrs_position_ra,
	gcat_helper.gcat_to_number(w_spin_icrs_position_dec) w_spin_icrs_position_dec,
	gcat_helper.gcat_to_number(w_spin_meridian) w_spin_meridian,
	gcat_helper.gcat_to_number(w_spin_rate) w_spin_rate,
	gcat_helper.gcat_to_number(w_spin_j2) w_spin_j2,
	gcat_helper.gcat_to_number(w_spin_j4) w_spin_j4,
	gcat_helper.gcat_to_number(w_spin_j6) w_spin_j6,
	gcat_helper.gcat_to_number(w_spin_pole_ra_rate) w_spin_pole_ra_rate,
	gcat_helper.gcat_to_number(w_spin_pole_dec_rate) w_spin_pole_dec_rate,
	w_spin_pole_function,
	w_spin_spin_function,
	w_spin_init_function,
	w_spin_jfile
from
(
	--Fix data issues.
	select
		w_id, w_idname,w_name,w_altname,w_radius,w_polar_radius,w_mass,w_semimajoraxis,w_periapsis,w_ecc,w_inc,w_node,w_peri,w_m,
		--FIX: Add BC for one date, and remove extra spaces for other dates.
		case
			when w_epoch = '0 Jun 28  0000:00' then 'BC 0146 Jun 28 0000:00'
			else regexp_replace(w_epoch, '([0-9]+)(  )([0-9]+)', '\1 \3')
		end w_epoch,
		w_rotperiod,w_orbperiod,w_ephemeris,w_WType,
		--A null is considered Sol, per the documentation.
		case
			when w_primary_w_name is null then 'Sun'
			--FIX
			when w_primary_w_name = 'Sol' then 'Sun'
			when w_primary_w_name = 'EMB' then 'Earth-Moon System'
			else w_primary_w_name
		end w_primary_w_name,
		w_spin_rho,w_spin_intertial_factor,w_spin_icrs_position_ra,w_spin_icrs_position_dec,w_spin_meridian,w_spin_rate,w_spin_j2,w_spin_j4,
		w_spin_j6,w_spin_pole_ra_rate,w_spin_pole_dec_rate,w_spin_pole_function,w_spin_spin_function,w_spin_init_function,w_spin_jfile
	from
	(
		--Rename world columns.
		select
			gcat_helper.convert_null_and_trim("IDT"          ) w_id,
			gcat_helper.convert_null_and_trim("IDName"       ) w_idname,
			gcat_helper.convert_null_and_trim("Name"         ) w_name,
			gcat_helper.convert_null_and_trim("AltName"      ) w_altname,
			gcat_helper.convert_null_and_trim("Radius"       ) w_radius,
			gcat_helper.convert_null_and_trim("PolarRadius"  ) w_polar_radius,
			gcat_helper.convert_null_and_trim("Mass"         ) w_mass,
			gcat_helper.convert_null_and_trim("SemiMajorAxis") w_semimajoraxis,
			gcat_helper.convert_null_and_trim("Periapsis"    ) w_periapsis,
			gcat_helper.convert_null_and_trim("Ecc"          ) w_ecc,
			gcat_helper.convert_null_and_trim("Inc"          ) w_inc,
			gcat_helper.convert_null_and_trim("Node"         ) w_node,
			gcat_helper.convert_null_and_trim("Peri"         ) w_peri,
			gcat_helper.convert_null_and_trim("M"            ) w_m,
			gcat_helper.convert_null_and_trim("Epoch"        ) w_epoch,
			gcat_helper.convert_null_and_trim("RotPeriod"    ) w_rotperiod,
			gcat_helper.convert_null_and_trim("OrbPeriod"    ) w_orbperiod,
			gcat_helper.convert_null_and_trim("Ephemeris"    ) w_ephemeris,
			gcat_helper.convert_null_and_trim("WType"        ) w_WType,
			gcat_helper.convert_null_and_trim("Primary"      ) w_primary_w_name
		from worlds_staging
		--FIX: Avoid some duplicate rows.
		where ("IDName", "Mass") not in (('(47171)', '61200?'), ('(617)', '1360'), ('(79360)', '46600?'), ('2017 OF69', '100000?'))
	) world
	left join
	(
		--Rename spin columns.
		select
			gcat_helper.convert_null_and_trim("IDName"    ) spin_id_name,
			gcat_helper.convert_null_and_trim("Rho"       ) w_spin_rho,
			gcat_helper.convert_null_and_trim("IFac"      ) w_spin_intertial_factor,
			gcat_helper.convert_null_and_trim("PoleRA"    ) w_spin_icrs_position_ra,
			gcat_helper.convert_null_and_trim("PoleDec"   ) w_spin_icrs_position_dec,
			gcat_helper.convert_null_and_trim("Meridian"  ) w_spin_meridian,
			gcat_helper.convert_null_and_trim("SpinRate"  ) w_spin_rate,
			gcat_helper.convert_null_and_trim("J2"        ) w_spin_j2,
			gcat_helper.convert_null_and_trim("J4"        ) w_spin_j4,
			gcat_helper.convert_null_and_trim("J6"        ) w_spin_j6,
			gcat_helper.convert_null_and_trim("PoleRARate") w_spin_pole_ra_rate,
			gcat_helper.convert_null_and_trim("PoleDecDec") w_spin_pole_dec_rate,
			gcat_helper.convert_null_and_trim("PoleFunc"  ) w_spin_pole_function,
			gcat_helper.convert_null_and_trim("SpinFunc"  ) w_spin_spin_function,
			gcat_helper.convert_null_and_trim("InitFunc"  ) w_spin_init_function,
			gcat_helper.convert_null_and_trim("JFile"     ) w_spin_jfile
		from spin_staging
	) spin
		on world.w_idname = spin.spin_id_name
) fix_data;

alter table world add constraint pk_world primary key(w_idname);

--Check for bad foreign keys.
-- Block will raise an exception if there's a bad row, else it will do nothing.
--
--Ensure that every w_primary_w_name refers to one and only one world.
--(This is weird because W_NAME is not unique.)
begin
	for bad_rows in
	(
		select *
		from
		(
			select w1.w_idname, w1.w_primary_w_name, w2.w_idname w2_id_name,
				count(*) over (partition by w1.w_idname, w2.w_idname) row_count,
				case when w1.w_idname is not null and w2.w_idname is not null then 1 else 0 end has_match
			from world w1
			left join world w2
				on w1.w_primary_w_name = w2.w_name
		)
		where row_count <> 1 or has_match = 0
	) loop
		if bad_rows.row_count >= 2 then
			raise_application_error(-20000, 'The world "' || bad_rows.w_idname || '" has multiple matches for "' || bad_rows.w_primary_w_name || '"');
		elsif bad_rows.has_match = 0 then
			raise_application_error(-20000, 'The world "' || bad_rows.w_idname || '" has no matches for "' || bad_rows.w_primary_w_name || '"');
		end if;
	end loop;
end;
/

--Check that all spin rows loaded by comparing Rho count with Row count.
-- Block will raise an exception if there's a bad row, else it will do nothing.
declare
	v_rho_count number;
	v_row_count number;
begin
	select count(*) into v_rho_count from world where w_spin_rho is not null;
	select count(*) into v_row_count from spin_staging;
	if v_rho_count <> v_row_count then
		raise_application_error(-20000, 'One or more row in staging did not match to a row in world.');
	end if;
end;
/


--SATELLITE
--(Takes about 3 minutes to run.)
create table satellite nologging as
select /*+ no_gather_optimizer_statistics */
	s_catalog,
	s_jcat,
	s_satcat,
	s_piece,
	s_type_byte_1,
	s_type_byte_2,
	s_type_byte_3,
	s_type_byte_4,
	s_type_byte_5,
	s_type_byte_6,
	s_type_byte_7,
	s_type_byte_8,
	s_type_byte_9,
	s_name,
	s_PLName,
	gcat_helper.vague_to_date(s_LDate) s_LDate,
	gcat_helper.vague_to_precision(s_LDate) s_LDate_precision,
	case
		when s_parent_s_jcat_or_w_name = 'SEL1' then 'Sun-Earth L1'
		when s_parent_s_jcat_or_w_name = 'SEL2' then 'Sun-Earth L2'
		when s_parent_s_jcat_or_w_name = 'HCO' then 'Sun'
		when s_parent_s_jcat_or_w_name = '(243)' then 'Ida'
		when s_parent_s_jcat_or_w_name = '(253)' then 'Mathilde'
		when s_parent_s_jcat_or_w_name = '(951)' then 'Gaspra'
		when s_parent_s_jcat_or_w_name = '67P/' then 'Churyumov-Gerasimenko'
		when s_parent_s_jcat_or_w_name = '81P/Wild2' then 'Wild 2'
		when s_parent_s_jcat_or_w_name = '9P/Tempel1' then 'Tempel 1'
		when s_parent_s_jcat_or_w_name = 'EML1' then 'Earth-Moon L1'
		when s_parent_s_jcat_or_w_name = 'EML2' then 'Earth-Moon L2'
		else s_parent_s_jcat_or_w_name
	end s_parent_s_jcat_or_w_name,
	s_parent_port,
	s_parent_flag,
	gcat_helper.vague_to_date(s_SDate) s_SDate,
	gcat_helper.vague_to_precision(s_SDate) s_SDate_precision,
	s_primary_w_name,
	gcat_helper.vague_to_date(s_DDate) s_DDate,
	gcat_helper.vague_to_precision(s_DDate) s_DDate_precision,
	s_status,
	s_dest,
	s_state_o_code,
	s_bus,
	s_motor,
	gcat_helper.gcat_to_number(s_mass) s_mass,
	s_massflag,
	gcat_helper.gcat_to_number(s_drymass) s_drymass,
	s_dryflag,
	gcat_helper.gcat_to_number(s_TotMass) s_TotMass,
	s_TotFlag,
	gcat_helper.gcat_to_number(s_length) s_length,
	s_LFlag,
	gcat_helper.gcat_to_number(s_diameter) s_diameter,
	s_DFlag,
	gcat_helper.gcat_to_number(s_span) s_span,
	s_SpanFlag,
	s_shape,
	gcat_helper.vague_to_date(s_ODate) s_ODate,
	gcat_helper.vague_to_precision(s_ODate) s_ODate_precision,
	gcat_helper.gcat_to_number(s_perigee) s_perigee,
	s_PF,
	gcat_helper.gcat_to_binary_double(s_apogee) s_apogee,
	s_AF,
	gcat_helper.gcat_to_number(s_Inc) s_Inc,
	s_IF,
	s_OpOrbit,
	s_OQUAL,
	s_AltNames
from
(
	--Fix data issues.
	select
		s_catalog,
		s_jcat,
		s_satcat,
		s_piece,
		s_type_byte_1,
		s_type_byte_2,
		s_type_byte_3,
		s_type_byte_4,
		s_type_byte_5,
		s_type_byte_6,
		s_type_byte_7,
		s_type_byte_8,
		s_type_byte_9,
		s_name,
		s_PLName,
		case
			when s_LDate = '1963 Jun   5' then '1963 Jun  5'
			when s_LDate = '1963 Jun  25' then '1963 Jun 25'
			when s_LDate = '1963 Jun  26' then '1963 Jun 26'
			else s_LDate
		end s_LDate,
		--Everything up to the first space and remove any asterisks.
		replace(regexp_substr(s_parent, '[^ ]+'), '*') s_parent_s_jcat_or_w_name,
		--Everything after the spaces and remove any asterisks
		trim(regexp_substr(s_parent, '\s+.*', 1, 1)) s_parent_port,
		--Is there an asterisk or not?
		case when s_parent like '%*%' then '*' else null end s_parent_flag,
		case
			--I'm guessing for some of these dates.
			when s_SDate like '%Jan  0' then replace(s_SDate, 'Jan  0', 'Jan')
			when s_SDate like '%Jan 00' then replace(s_SDate, 'Jan 00', 'Jan')
			when s_SDate like '%Apr  0' then replace(s_SDate, 'Apr  0', 'Apr')
			when s_SDate like '%Mar  0' then replace(s_SDate, 'Mar  0', 'Mar')
			when s_SDate = '2002 Jan 16 0705s' then '2002 Jan 16 0705'
			when s_SDate = '2000 Nov  5  2000?' then '2000 Nov  5 2000?'
			else s_SDate
		end s_SDate,
		s_primary_w_name,
		case
			when s_DDate = '2011 Jun   2' then '2011 Jun  2'
			when s_DDate = '2014 Jan   2' then '2014 Jan  2'
			when s_DDate = '2011 Mar  10' then '2011 Mar 10'
			when s_DDate = '2014 Dec   2' then '2014 Dec  2'
			when s_DDate = '2015 Feb   5' then '2015 Feb  5'
			when s_DDate = '2020 Oct  15' then '2020 Oct 15'
			when s_DDate = '1971 May  16' then '1971 May 16'
			when s_DDate = '2011 Feb   2' then '2011 Feb  2'
			when s_DDate = '2009 Jan  0'  then '2009 Jan 01'
			when s_DDate = '2019 May  16'  then '2019 May 16'
			when s_DDate like '%Mar  0' then replace(s_DDate, 'Mar  0', 'Mar')
			when s_DDate = '2021 Sep  10 0900?' then '2021 Sep 10 0900?'
			else s_DDate
		end s_DDate,
		s_status,
		s_dest,
		s_state_o_code,
		s_bus,
		s_motor,
		s_mass,
		s_massflag,
		s_drymass,
		s_dryflag,
		s_TotMass,
		s_TotFlag,
		s_length,
		s_LFlag,
		s_diameter,
		s_DFlag,
		s_span,
		s_SpanFlag,
		s_shape,
		case
			when s_ODate = '1973 Jan  13' then '1973 Jan 13'
			when s_ODate = '2011 Aug  16' then '2011 Aug 16'
			else s_ODate
		end s_ODate,
		s_perigee,
		s_PF,
		s_apogee,
		s_AF,
		s_Inc,
		s_IF,
		s_OpOrbit,
		s_OQUAL,
		s_AltNames
	from
	(
		--Rename columns.
		select
			s_catalog,
			gcat_helper.convert_null_and_trim(sat_files."JCAT"    ) s_jcat,
			gcat_helper.convert_null_and_trim("Satcat"            ) s_satcat,
			gcat_helper.convert_null_and_trim("Piece"             ) s_piece,
			gcat_helper.convert_null_and_trim(substr("Type", 1, 1)) s_type_byte_1,
			gcat_helper.convert_null_and_trim(substr("Type", 2, 1)) s_type_byte_2,
			gcat_helper.convert_null_and_trim(substr("Type", 3, 1)) s_type_byte_3,
			gcat_helper.convert_null_and_trim(substr("Type", 4, 1)) s_type_byte_4,
			gcat_helper.convert_null_and_trim(substr("Type", 5, 1)) s_type_byte_5,
			gcat_helper.convert_null_and_trim(substr("Type", 6, 1)) s_type_byte_6,
			gcat_helper.convert_null_and_trim(substr("Type", 7, 1)) s_type_byte_7,
			gcat_helper.convert_null_and_trim(substr("Type", 8, 1)) s_type_byte_8,
			gcat_helper.convert_null_and_trim(substr("Type", 9, 1)) s_type_byte_9,
			gcat_helper.convert_null_and_trim(sat_files."Name"    ) s_name,
			gcat_helper.convert_null_and_trim("PLName"            ) s_PLName,
			gcat_helper.convert_null_and_trim("LDate"             ) s_LDate,
			gcat_helper.convert_null_and_trim("Parent"            ) s_parent,
			gcat_helper.convert_null_and_trim("SDate"             ) s_SDate,
			gcat_helper.convert_null_and_trim("Primary"           ) s_primary_w_name,
			gcat_helper.convert_null_and_trim("DDate"             ) s_DDate,
			gcat_helper.convert_null_and_trim("Status"            ) s_status,
			gcat_helper.convert_null_and_trim("Dest"              ) s_dest,
			gcat_helper.convert_null_and_trim("State"             ) s_state_o_code,
			gcat_helper.convert_null_and_trim("Bus"               ) s_bus,
			gcat_helper.convert_null_and_trim("Motor"             ) s_motor,
			gcat_helper.convert_null_and_trim("Mass"              ) s_mass,
			gcat_helper.convert_null_and_trim("MassFlag"          ) s_massflag,
			gcat_helper.convert_null_and_trim("DryMass"           ) s_drymass,
			gcat_helper.convert_null_and_trim("DryFlag"           ) s_dryflag,
			gcat_helper.convert_null_and_trim("TotMass"           ) s_TotMass,
			gcat_helper.convert_null_and_trim("TotFlag"           ) s_TotFlag,
			gcat_helper.convert_null_and_trim("Length"            ) s_length,
			gcat_helper.convert_null_and_trim("LFlag"             ) s_LFlag,
			gcat_helper.convert_null_and_trim("Diameter"          ) s_diameter,
			gcat_helper.convert_null_and_trim("DFlag"             ) s_DFlag,
			gcat_helper.convert_null_and_trim("Span"              ) s_span,
			gcat_helper.convert_null_and_trim("SpanFlag"          ) s_SpanFlag,
			gcat_helper.convert_null_and_trim("Shape"             ) s_shape,
			gcat_helper.convert_null_and_trim("ODate"             ) s_ODate,
			gcat_helper.convert_null_and_trim("Perigee"           ) s_perigee,
			gcat_helper.convert_null_and_trim("PF"                ) s_PF,
			gcat_helper.convert_null_and_trim("Apogee"            ) s_apogee,
			gcat_helper.convert_null_and_trim("AF"                ) s_AF,
			gcat_helper.convert_null_and_trim("Inc"               ) s_Inc,
			gcat_helper.convert_null_and_trim("IF"                ) s_IF,
			gcat_helper.convert_null_and_trim("OpOrbit"           ) s_OpOrbit,
			gcat_helper.convert_null_and_trim("OQUAL"             ) s_OQUAL,
			gcat_helper.convert_null_and_trim("AltNames"          ) s_AltNames,
			usatcat_staging."Name"                                  s_unicode_name
		from
		(
			select 'auxcat'  s_catalog, auxcat_staging.*  from auxcat_staging  union all
			select 'csocat'  s_catalog, csocat_staging.*  from csocat_staging  union all
			select 'deepcat' s_catalog, deepcat_staging.* from deepcat_staging union all
			select 'ecat'    s_catalog, ecat_staging.*    from ecat_staging    union all
			select 'ftocat'  s_catalog, ftocat_staging.*  from ftocat_staging  union all
			select 'hcocat'  s_catalog, hcocat_staging.*  from hcocat_staging  union all
			select 'lcat'    s_catalog, lcat_staging.*    from lcat_staging    union all
			select 'lprcat'  s_catalog, lprcat_staging.*  from lprcat_staging  union all
			select 'rcat'    s_catalog, rcat_staging.*    from rcat_staging    union all
			select 'satcat'  s_catalog, satcat_staging.*  from satcat_staging  union all
			select 'tmpcat'  s_catalog, tmpcat_staging.*  from tmpcat_staging
		) sat_files
		left join usatcat_staging
			on sat_files.JCAT = usatcat_staging.JCAT
	) rename_columns
) fix_data
order by 1,2,3;

alter table satellite add constraint fk_satellite_state_org foreign key (s_state_o_code) references organization(o_code);
--Too many duplicates. No good primary key for this table?
--alter table satellite add constraint pk_satellite primary key(s_jcat);
create index satellite_idx1 on satellite(s_jcat);
alter table satellite modify s_jcat not null;

--Ensure all parents exist.
--Can't use a foreign key because of the weird table structures.
begin
	for missing_parents in
	(
		select *
		from
		(
			select s_catalog, s_jcat, s_parent_s_jcat_or_w_name
			from satellite
			where s_parent_s_jcat_or_w_name is not null
		) satellites
		left join
		(
			select s_jcat from satellite
			union all
			select w_name from world
		) parents
			on satellites.s_parent_s_jcat_or_w_name = parents.s_jcat
		where parents.s_jcat is null
		order by 1
	) loop
		raise_application_error(-20000, 'This Parent value does not match: ' || missing_parents.s_parent_s_jcat_or_w_name);
	end loop;
end;
/


--SATELLITE_OWNER_ORG
create table satellite_owner_org compress as
select cast("JCAT" as varchar2(100)) soo_s_jcat, rtrim(column_value, '?') soo_o_code
from satcat_staging
cross join gcat_helper.get_nt_from_list("Owner", '/')
where "Owner" <> '-'
order by 1,2;

alter table satellite_owner_org add constraint pk_satellite_owner_org primary key(soo_s_jcat, soo_o_code);
alter table satellite_owner_org add constraint fk_satellite_owner_org_organization foreign key(soo_o_code) references organization(o_code);
--Too many duplicates for foreign key.
--alter table satellite_owner_org add constraint fk_satellite_owner_org_satellite foreign key(soo_s_jcat) references satellite(s_jcat);


--SATELLITE_MANUFACTURER_ORG
create table satellite_manufacturer_org compress as
select cast("JCAT" as varchar2(100)) smo_s_jcat, rtrim(column_value, '?') smo_o_code
from satcat_staging
cross join gcat_helper.get_nt_from_list(rtrim("Manufacturer", '?'), '/')
where "Manufacturer" <> '-'
order by 1,2;

alter table satellite_manufacturer_org add constraint pk_satellite_manufacturer_org primary key(smo_s_jcat, smo_o_code);
--Too many duplicates for foreign key.
--alter table satellite_manufacturer_org add constraint fk_satellite_manufacturer_org_satellite foreign key(smo_s_jcat) references satellite(s_jcat);
alter table satellite_manufacturer_org add constraint fk_satellite_manufacturer_org_organization foreign key(smo_o_code) references organization(o_code);


--PAYLOAD
create table payload nologging as
select /*+ no_gather_optimizer_statistics */
	pay_catalog,
	pay_JCAT,
	pay_Piece,
	pay_Name,
	gcat_helper.vague_to_date(pay_LDate) pay_LDate,
	gcat_helper.vague_to_precision(pay_LDate) pay_LDate_precision,
	gcat_helper.vague_to_date(pay_TLast) pay_TLast,
	gcat_helper.vague_to_precision(pay_TLast) pay_TLast_precision,
	gcat_helper.vague_to_date(pay_TOp) pay_TOp,
	gcat_helper.vague_to_precision(pay_TOp) pay_TOp_precision,
	gcat_helper.vague_to_date(pay_TDate) pay_TDate,
	gcat_helper.vague_to_precision(pay_TDate) pay_TDate_precision,
	pay_TF,
	pay_Program,
	pay_Plane,
	pay_Att,
	pay_Mvr,
	pay_Class,
	replace(replace(replace(pay_UNState, '*'), '['), ']') pay_UNState_o_code,
	case
		when pay_unstate is null then null
		when pay_unstate like '%[%]%' then 'Yes'
		else 'No'
	end pay_is_registered,
	pay_UNReg,
	gcat_helper.gcat_to_number(pay_UNPeriod) pay_UNPeriod,
	gcat_helper.gcat_to_number(pay_UNPerigee) pay_UNPerigee,
	gcat_helper.gcat_to_number(pay_UNApogee) pay_UNApogee,
	gcat_helper.gcat_to_number(pay_UNInc) pay_UNInc,
	pay_Result,
	pay_Control,
	pay_Comment
from
(
	--Fix data issues.
	select
		pay_catalog,
		pay_JCAT,
		pay_Piece,
		pay_Name,
		pay_LDate,
		pay_TLast,
		pay_TOp,
		case
			when pay_TDate in ('*', '* E') then null
			else pay_TDate
		end pay_TDate,
		pay_TF,
		pay_Program,
		pay_Plane,
		pay_Att,
		--FIX:
		case
			when pay_Mvr = 'm' then 'M'
			else pay_Mvr
		end pay_Mvr,
		pay_Class,
		pay_UNState,
		pay_UNReg,
		pay_UNPeriod,
		pay_UNPerigee,
		pay_UNApogee,
		pay_UNInc,
		pay_Result,
		pay_Control,
		pay_Comment
	from
	(
		--Rename columns.
		select
			pay_catalog,
			gcat_helper.convert_null_and_trim("JCAT"      ) pay_JCAT,
			gcat_helper.convert_null_and_trim("Piece"     ) pay_Piece,
			gcat_helper.convert_null_and_trim("Name"      ) pay_Name,
			gcat_helper.convert_null_and_trim("LDate"     ) pay_LDate,
			gcat_helper.convert_null_and_trim("TLast"     ) pay_TLast,
			gcat_helper.convert_null_and_trim("TOp"       ) pay_TOp,
			gcat_helper.convert_null_and_trim("TDate"     ) pay_TDate,
			gcat_helper.convert_null_and_trim("TF"        ) pay_TF,
			gcat_helper.convert_null_and_trim("Program"   ) pay_Program,
			gcat_helper.convert_null_and_trim("Plane"     ) pay_Plane,
			gcat_helper.convert_null_and_trim("Att"       ) pay_Att,
			gcat_helper.convert_null_and_trim("Mvr"       ) pay_Mvr,
			gcat_helper.convert_null_and_trim("Class"     ) pay_Class,
			gcat_helper.convert_null_and_trim("UNState"   ) pay_UNState,
			gcat_helper.convert_null_and_trim("UNReg"     ) pay_UNReg,
			gcat_helper.convert_null_and_trim("UNPeriod"  ) pay_UNPeriod,
			gcat_helper.convert_null_and_trim("UNPerigee" ) pay_UNPerigee,
			gcat_helper.convert_null_and_trim("UNApogee"  ) pay_UNApogee,
			gcat_helper.convert_null_and_trim("UNInc"     ) pay_UNInc,
			gcat_helper.convert_null_and_trim("Result"    ) pay_Result,
			gcat_helper.convert_null_and_trim("Control"   ) pay_Control,
			gcat_helper.convert_null_and_trim("Comment"   ) pay_Comment
		from
		(
			select 'pauxcat' pay_catalog, pauxcat_staging.* from pauxcat_staging union all
			select 'pftocat' pay_catalog, pftocat_staging.* from pftocat_staging union all
			select 'plcat'   pay_catalog, plcat_staging.*   from plcat_staging union all
			select 'psatcat' pay_catalog, psatcat_staging.* from psatcat_staging
		)
	) rename_columns
) fix_data
order by 1,2,3;

alter table payload add constraint pk_payload primary key(pay_jcat);
alter table payload add constraint fk_payload_org foreign key (pay_UNState_o_code) references organization(o_code);


--PAYLOAD_CATEGORY
create table payload_category nologging as
select
	cast("JCAT" as varchar2(100)) pc_pay_jcat,
	rtrim(rtrim(column_value, '?'), '*') pc_category,
	case when column_value like '%*%' then 'Yes' else 'No' end pc_is_secret
from
(
	select 'pauxcat' pay_catalog, pauxcat_staging.* from pauxcat_staging union all
	select 'pftocat' pay_catalog, pftocat_staging.* from pftocat_staging union all
	select 'plcat'   pay_catalog, plcat_staging.*   from plcat_staging union all
	select 'psatcat' pay_catalog, psatcat_staging.* from psatcat_staging
)
cross join gcat_helper.get_nt_from_list(rtrim("Category", '?'), '/')
where "Category" <> '-'
order by 1,2;

alter table payload_category add constraint pk_payload_category primary key(pc_pay_jcat, pc_category);
alter table payload_category add constraint fk_payload_category_payload foreign key(pc_pay_jcat) references payload(pay_jcat);


--PAYLOAD_DISCIPLINE
create table payload_discipline nologging as
select
	cast("JCAT" as varchar2(100)) pd_pay_jcat,
	replace(replace(column_value, '?'), '*') pd_discipline
from
(
	select 'pauxcat' pay_catalog, pauxcat_staging.* from pauxcat_staging union all
	select 'pftocat' pay_catalog, pftocat_staging.* from pftocat_staging union all
	select 'plcat'   pay_catalog, plcat_staging.*   from plcat_staging union all
	select 'psatcat' pay_catalog, psatcat_staging.* from psatcat_staging
)
cross join gcat_helper.get_nt_from_list(rtrim("Discipline", '?'), '/')
where "Discipline" <> '-'
order by 1,2;

alter table payload_discipline add constraint pk_payload_discipline primary key(pd_pay_jcat, pd_discipline);
alter table payload_discipline add constraint fk_payload_discipline_payload foreign key(pd_pay_jcat) references payload(pay_jcat);


--ENGINE
create table engine compress nologging as
select
	e_ID,
	cast(e_Name as varchar2(1000)) e_Name,
	e_Family,
	e_Alt_Name,
	gcat_helper.gcat_to_number(e_Mass) e_Mass,
	e_MFlag,
	gcat_helper.gcat_to_number(e_Impulse) e_Impulse,
	e_ImpFlag,
	gcat_helper.gcat_to_number(e_Thrust) e_Thrust,
	e_TFlag,
	gcat_helper.gcat_to_number(e_Isp) e_Isp,
	e_IspFlag,
	gcat_helper.gcat_to_number(e_Duration) e_Duration,
	e_DurFlag,
	gcat_helper.gcat_to_number(e_Chambers) e_Chambers,
	gcat_helper.vague_to_date(e_Date) e_Date,
	gcat_helper.vague_to_precision(e_Date) e_Date_precision,
	e_Usage,
	e_Group
from
(
	--Rename columns.
	select
		line_number                                       e_ID,
		gcat_helper.convert_null_and_trim("Name"        ) e_Name,
		gcat_helper.convert_null_and_trim("Family"      ) e_Family,
		gcat_helper.convert_null_and_trim("Alt_Name"    ) e_Alt_Name,
		gcat_helper.convert_null_and_trim("Mass"        ) e_Mass,
		gcat_helper.convert_null_and_trim("MFlag"       ) e_MFlag,
		gcat_helper.convert_null_and_trim("Impulse"     ) e_Impulse,
		gcat_helper.convert_null_and_trim("ImpFlag"     ) e_ImpFlag,
		gcat_helper.convert_null_and_trim("Thrust"      ) e_Thrust,
		gcat_helper.convert_null_and_trim("TFlag"       ) e_TFlag,
		gcat_helper.convert_null_and_trim("Isp"         ) e_Isp,
		gcat_helper.convert_null_and_trim("IspFlag"     ) e_IspFlag,
		gcat_helper.convert_null_and_trim("Duration"    ) e_Duration,
		gcat_helper.convert_null_and_trim("DurFlag"     ) e_DurFlag,
		gcat_helper.convert_null_and_trim("Chambers"    ) e_Chambers,
		gcat_helper.convert_null_and_trim("Date"        ) e_Date,
		gcat_helper.convert_null_and_trim("Usage"       ) e_Usage,
		gcat_helper.convert_null_and_trim("Group"       ) e_Group
	from engines_staging
) rename_columns
order by 1,2,3;

alter table engine add constraint pk_engine primary key(e_id);


--ENGINE_PROPELLANT:
create table engine_propellant compress as
select ep_e_id, ep_propellant, ep_fuel_or_oxidizer
from
(
	select
		ep_e_id,
		--Fix: It looks like some of the values got their last letter cut off.
		replace(replace(replace(replace(replace(column_value,
			'Al TP-H-334', 'Al TP-H-3340'),
			'AlTP-H-3062', 'AlTP-H-3062M'),
			'RFNA AK-20', 'RFNA AK-20F'),
			'Vinyl Isobutyl ethe', 'Vinyl Isobutyl ether'),
			--This column is a single value - ignore the slash.
			'JPX (JP-4/UDMH)', 'JPX (JP-4 and UDMH)')
		ep_propellant,
		ep_fuel_or_oxidizer
	from
	(
		--All chemicals
		--
		--Oxidizers.
		select line_number ep_e_id, replace("Oxidizer", '?') propellant_list, 'oxidizer' ep_fuel_or_oxidizer
		from engines_staging
		where "Oxidizer" <> '-'
		union
		--Fuels.
		--(Fix: one weird fuel that looks like there's a missing second value)
		select line_number ep_e_id, case when propellant_list = 'MMH/' then 'MMH' else propellant_list end, ep_fuel_or_oxidizer
		from
		(
			select line_number, replace("Fuel", '?') propellant_list, 'fuel' ep_fuel_or_oxidizer
			from engines_staging
			where "Fuel" <> '-'
		)
	)
	cross join gcat_helper.get_nt_from_list(propellant_list, '/')
)
order by ep_propellant;

alter table engine_propellant add constraint pk_engine_propellant primary key(ep_e_id, ep_propellant, ep_fuel_or_oxidizer);
alter table engine_propellant add constraint fk_engine_propellant_engine foreign key (ep_e_id) references engine(e_id);


--ENGINE_MANUFACTURER:
create table engine_manufacturer compress as
select em_e_id, em_manufacturer_o_code
from
(
	select
		em_e_id,
		replace(replace(replace(replace(
			column_value, '?'),
			'Zulfiqar', 'IRGC'),
			'TKSVA', 'OKB52'),
			'IRCPS', 'NGISP'
		) em_manufacturer_o_code
	from
	(
		select line_number em_e_id, "Manufacturer"
		from engines_staging
	)
	cross join gcat_helper.get_nt_from_list("Manufacturer", '/')
)
order by 1,2;

alter table engine_manufacturer add constraint pk_engine_manufacturer primary key(em_e_id, em_manufacturer_o_code);
alter table engine_manufacturer add constraint fk_engine_manufacturer_engine foreign key (em_e_id) references engine(e_id);
alter table engine_manufacturer add constraint fk_engine_manufacturer_organization foreign key (em_manufacturer_o_code) references organization(o_code);

/*
--Check for bad organization codes.
select *
from engine_manufacturer
left join organization
	on em_manufacturer_o_code = o_code
where o_code is null
order by 1;
*/


--STAGE
create table stage compress nologging as
select
	Stage_Name,
	Stage_LVF_Family,
	Stage_Alt_Name,
	gcat_helper.gcat_to_number(Stage_Length) stage_length,
	gcat_helper.gcat_to_number(Stage_Diameter) stage_diameter,
	gcat_helper.gcat_to_number(Stage_Launch_Mass) stage_launch_mass,
	gcat_helper.gcat_to_number(Stage_Dry_Mass) stage_dry_mass,
	gcat_helper.gcat_to_number(Stage_Thrust) stage_thrust,
	gcat_helper.gcat_to_number(Stage_Duration) stage_duration,
	stage_e_id,
	Stage_NEng
from
(
	--Rename columns.
	select
		gcat_helper.convert_null_and_trim("Stage_Name"    ) Stage_Name,
		gcat_helper.convert_null_and_trim("Stage_Family"  ) Stage_LVF_Family,
		gcat_helper.convert_null_and_trim("Stage_Alt_Name") Stage_Alt_Name,
		gcat_helper.convert_null_and_trim("Length"        ) Stage_Length,
		gcat_helper.convert_null_and_trim("Diameter"      ) Stage_Diameter,
		gcat_helper.convert_null_and_trim("Launch_Mass"   ) Stage_Launch_Mass,
		gcat_helper.convert_null_and_trim("Dry_Mass"      ) Stage_Dry_Mass,
		gcat_helper.convert_null_and_trim("Thrust"        ) Stage_Thrust,
		gcat_helper.convert_null_and_trim("Duration"      ) Stage_Duration,
		e_id stage_e_id,
		gcat_helper.convert_null_and_trim("NEng"          ) Stage_NEng
	from stages_staging
	left join engine
		on stages_staging."Engine" = engine.e_name
	--Manually adjust some joins, based on names.
	--When it's still ambiguous, choose the engine with either more complete values, or more precise-looking numbers, or later values.
	where not
	(
		("Stage_Name" = '11S86'           and e_usage = 'Blok DM') or
		("Stage_Name" = '11S861'          and e_usage = 'Blok D-1') or
		("Stage_Name" = '11S861-01'       and e_usage = 'Blok D-1') or
		("Stage_Name" = '11S861-03'       and e_usage = 'Blok D-1') or
		("Stage_Name" = '17S40'           and e_usage = 'Blok DM') or
		("Stage_Name" = '14S48'           and e_usage = 'Blok DM') or
		("Stage_Name" = 'Blok DM1'        and e_usage = 'Blok D-1') or
		("Stage_Name" = 'Blok DM3'        and e_usage = 'Blok D-1') or
		("Stage_Name" = 'Blok DM4'        and e_usage = 'Blok D-1') or
		("Stage_Name" = 'Blok DM-SL'      and e_usage = 'Blok D-1') or
		("Stage_Name" = 'Blok DM-SLB'     and e_usage = 'Blok D-1') or
		("Stage_Name" = 'Dragon V2'       and e_usage = 'Dragon') or
		("Stage_Name" = 'FB-1 Stage 2'    and e_usage = 'CZ-2 St2 vernier x4') or
		("Stage_Name" = 'CZ-4 Stage 3'    and e_usage = 'CZ-4B (3)') or
		("Stage_Name" = 'CZ-4B Stage 3'   and e_usage = 'CZ-4B (3)') or
		("Stage_Name" = 'RT-23 St 3'      and e_usage is null) or
		("Stage_Name" = 'M-34'            and e_usage = 'M-V [3]') or
		("Stage_Name" = 'GEM-60'          and e_date = date '2002-11-20') or
		("Stage_Name" = 'Star 48B'        and e_usage = 'STS PAM-D') or
		("Stage_Name" = 'Star 48B AKM'    and e_usage = 'STS PAM-D') or
		("Stage_Name" = 'H-II SSB'        and e_date is null) or
		("Stage_Name" = 'Taurion'         and e_date is null) or
		("Stage_Name" = 'Improved Orion'  and e_date is null) or
		("Stage_Name" = 'Hydac'           and e_alt_name = 'H-28') or
		("Stage_Name" = 'SDC Viper'       and e_alt_name is null) or
		("Stage_Name" = 'Arcas'           and e_date is null) or
		("Stage_Name" = 'Frangible Arcas' and e_impulse is null)
	)
	--Exclude a row with no name.
	and "Stage_Name" <> '-'
) rename_columns
order by 1,2,3;

--If the PK doesn't build there may be more duplicate Engine names. See the weird join above.
alter table stage add constraint pk_stage primary key(stage_name);
alter table stage add constraint fk_stage_launch_vehicle_family foreign key (stage_lvf_family) references launch_vehicle_family(lvf_family);
alter table stage add constraint fk_stage_engine foreign key (stage_e_id) references engine(e_id);


--STAGE_MANUFACTURER
create table stage_manufacturer compress as
select cast(sm_stage_name as varchar2(1000)) sm_stage_name, sm_manufacturer_o_code
from
(
	select
		sm_stage_name, replace(column_value, '?') sm_manufacturer_o_code
	from
	(
		select
			"Stage_Name" sm_stage_name,
			replace("Stage_Manufacturer", 'NCSIST', '') "Stage_Manufacturer" --TODO: Missing data from orgs.tsv?
		from stages_staging
		where "Stage_Name" not in ('-', '?')
			and "Stage_Manufacturer" <> '-'
	)
	cross join gcat_helper.get_nt_from_list("Stage_Manufacturer", '/')
)
order by 1,2;

alter table stage_manufacturer add constraint pk_stage_manufacturer primary key(sm_stage_name, sm_manufacturer_o_code);
alter table stage_manufacturer add constraint fk_stage_manufacturer_stage foreign key (sm_stage_name) references stage(stage_name);
alter table stage_manufacturer add constraint fk_stage_manufacturer_org foreign key (sm_manufacturer_o_code) references organization(o_code);

/*
--FK_STAGE_MANUFACTURER_STAGE errors:
select *
from stage_manufacturer
left join stage
	on sm_stage_name = stage_name
where stage_name is null;

--FK_STAGE_MANUFACTURER_ORG errors:
select *
from stage_manufacturer
left join organization
	on sm_manufacturer_o_code = o_code
where o_code is null;

*/


--LAUNCH_VEHICLE_STAGE
create table launch_vehicle_stage nologging as
select /*+ no_gather_optimizer_statistics */
	cast(lvs_LV_Name as varchar2(100)) lvs_lv_name,
	cast(lvs_LV_Variant as varchar2(100)) lvs_lv_variant,
	cast(lvs_Stage_No as varchar2(100)) lvs_stage_no,
	cast(lvs_Stage_Name as varchar2(100)) lvs_Stage_Name,
	lvs_stage_type,
	lvs_Qualifier,
	lvs_Dummy,
	gcat_helper.gcat_to_number(lvs_Multiplicity) lvs_Multiplicity,
	gcat_helper.gcat_to_number(lvs_Stage_Impulse) lvs_Stage_Impulse,
	gcat_helper.gcat_to_number(lvs_Stage_Apogee) lvs_Stage_Apogee,
	gcat_helper.gcat_to_number(lvs_Stage_Perigee) lvs_Stage_Perigee,
	lvs_Perigee_Qual
from
(
	--Fix data issues.
	select
		lvs_LV_Name,
		lvs_LV_Variant,
		lvs_Stage_No,
		case
			when stage_name is null then null
			else lvs_stage_name
		end lvs_stage_name,
		case
			when stage_name is null then lvs_stage_name
			else null
		end lvs_stage_type,
		lvs_Qualifier,
		lvs_Dummy,
		lvs_Multiplicity,
		lvs_Stage_Impulse,
		lvs_Stage_Apogee,
		lvs_Stage_Perigee,
		lvs_Perigee_Qual
	from
	(
		--Rename columns.
		select
			gcat_helper.convert_null_and_trim("LV_Name"      ) lvs_LV_Name,
			gcat_helper.convert_null_and_trim("LV_Variant"   ) lvs_LV_Variant,
			gcat_helper.convert_null_and_trim("Stage_No"     ) lvs_Stage_No,
			gcat_helper.convert_null_and_trim("Stage_Name"   ) lvs_Stage_Name,
			gcat_helper.convert_null_and_trim("Qualifier"    ) lvs_Qualifier,
			gcat_helper.convert_null_and_trim("Dummy"        ) lvs_Dummy,
			gcat_helper.convert_null_and_trim("Multiplicity" ) lvs_Multiplicity,
			gcat_helper.convert_null_and_trim("Stage_Impulse") lvs_Stage_Impulse,
			gcat_helper.convert_null_and_trim("Stage_Apogee" ) lvs_Stage_Apogee,
			gcat_helper.convert_null_and_trim("Stage_Perigee") lvs_Stage_Perigee,
			gcat_helper.convert_null_and_trim("Perigee_Qual" ) lvs_Perigee_Qual
		from lvs_staging
	) rename_columns
	--FIX: There are many "Stage_Name" values that do not match STAGE.STAGE_NAME, and they are more like Stage Types.
	left join stage
		on rename_columns.lvs_stage_name = stage_name
	--where not (lvs_lv_name in ('?', 'Unknown') and lvs_stage_name = '?' and lvs_stage_name = '?'
) fix_data
order by 1,2,3;

alter table launch_vehicle_stage add constraint uq_launch_vehicle_stage unique(lvs_lv_name, lvs_lv_variant, lvs_stage_no, lvs_stage_name, lvs_stage_type);
alter table launch_vehicle_stage add constraint fk_launch_vehicle_stage_launch_vehicle foreign key(lvs_lv_name, lvs_lv_variant) references launch_vehicle(lv_name, lv_variant);
alter table launch_vehicle_stage add constraint fk_launch_vehicle_stage_stage foreign key(lvs_stage_name) references stage(stage_name);

/*
--FK_LAUNCH_VEHICLE_STAGE_STAGE - Check for bad values.
select *
from launch_vehicle_stage
left join stage
	on lvs_stage_name = stage_name
where lvs_stage_name is not null
	and stage_name is null
*/



/*
--TODO: Convert to S_MOTOR to E_CODE? Currently the values look very different.
select distinct s_motor, engines_staging."Name"  engine_name
from satellite
full outer join engines_staging
	on s_motor = "Name"
order by coalesce(s_motor, "Name");
*/






--------------------------------------------------------------------------------
-- Shrink columns. (8 seconds.)
--------------------------------------------------------------------------------

--Automatically shrink column size as much as possible.
--(This doesn't save space, but can help with applications that use the max data size for presentation.)
declare
	p_table_name varchar2(128) := upper('ORGANIZATION');
	v_max_size number;
	v_ddl varchar2(32767);
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
			--Only shrink column if there is data in it.
			if v_max_size is not null then
				v_ddl := replace(varchar2_columns.v_alter, '?', v_max_size);
				begin
					execute immediate v_ddl;
				exception when others then
					raise_application_error(-20000, 'Problem with this DDL: ' || v_ddl || chr(10) || sqlerrm);
				end;
			end if;
		end loop;
	end loop;
end;
/




--------------------------------------------------------------------------------
-- Recreate OCI database and public access.
--------------------------------------------------------------------------------

--Drop cloud users.
declare
	v_user_does_not_exist exception;
	pragma exception_init(v_user_does_not_exist, -1918);
	v_profile_does_not_exist exception;
	pragma exception_init(v_profile_does_not_exist, -2380);
begin
	begin
		dbms_utility.exec_ddl_statement@gcat('drop user gcat cascade');
	exception when v_user_does_not_exist then null;
	end;

	begin
		dbms_utility.exec_ddl_statement@gcat('drop user gcat_public cascade');
	exception when v_user_does_not_exist then null;
	end;

	begin
		dbms_utility.exec_ddl_statement@gcat('drop profile gcat_public_profile');
	exception when v_profile_does_not_exist then null;
	end;

	dbms_utility.exec_ddl_statement@gcat(q'[ begin ords_admin.drop_rest_for_schema(p_schema => 'GCAT_PUBLIC'); end; ]');
end;
/


--Create user that will only hold the data.
begin
	dbms_utility.exec_ddl_statement@gcat('create user gcat no authentication quota unlimited on data default tablespace data');
end;
/


--Create a profile that won't lock or expire, and won't allow more
--than 10 seconds of CPU per statement. (These small tables shouldn't require much time.)
-- (It would be nice to allow simple password but ATP simply doesn't allow it.)
begin
	dbms_utility.exec_ddl_statement@gcat('
		create profile gcat_public_profile limit
		password_verify_function null
		failed_login_attempts unlimited
		password_life_time unlimited
		cpu_per_call 10000
	');
end;
/


--Create a public user to access GCAT data.
--(This read-only password is psuedo-public knowledge.)
begin
	dbms_utility.exec_ddl_statement@gcat('create user gcat_public identified by public_gcat#1A profile gcat_public_profile quota 1M on data');
	dbms_utility.exec_ddl_statement@gcat('grant create session to gcat_public');
end;
/


--Create a simple table that will appear on the initial login, for users who didn't read anything else.
begin
	dbms_utility.exec_ddl_statement@gcat(
	q'[
		create table gcat_public.readme as
		select 'This is a database and query tool built on top of Jonathan C. McDowell''s GCAT: General Catalog of Artificial Space Objects.
		To understand the data you should first read his website: https://planet4589.org/space/gcat/index.html
		
		The database code was created by Jon Heller.
		(TODO - See GitHub link?)
		Questions about how to query the data can be sent to me at jon@jonheller.org
		' readme from dual
	]');
end;
/


--Prevent user from altering objects or modifying data.
begin
	dbms_utility.exec_ddl_statement@gcat(
	q'[
		create or replace trigger prevent_user_from_altering_objects
		before ddl on gcat_public.schema
		begin
			raise_application_error(-20000, 'Please do not try to alter any objects on this system.');
		end;
	]');

	dbms_utility.exec_ddl_statement@gcat(
	q'[
		create or replace trigger prevent_user_from_modifying_data
		before insert or update or delete on gcat_public.readme
		begin
			raise_application_error(-20000, 'Please do not try to modify the data on this system.');
		end;
	]');
end;
/


--Prevent public user from changing the public password.
/*
This command:
	alter user gcat_public identified by "someNewPW#1234" replace "public_gcat#1A";
Will now throw this error:
	ORA-04088: error during execution of trigger 'ADMIN.CANNOT_CHANGE_PASSWORD_TR' ORA-00604: error occurred at recursive SQL level 1 ORA-20010: you cannot change your own password. ORA-06512: at line 5
*/
begin
	dbms_utility.exec_ddl_statement@gcat(
	q'[
		create or replace trigger cannot_change_password_tr
		before alter
		on database
		declare
		begin
			--From: http://www.petefinnigan.com/weblog/archives/00001198.htm
			if (ora_dict_obj_type = 'USER' and user = 'GCAT_PUBLIC') then
				raise_application_error(-20010,'you cannot change your own password.');
			end if;
		end;
	]');
end;
/


--Enable SQL Developer Web access.
--(To rollback: ords_admin.drop_rest_for_schema(p_schema => 'GCAT_PUBLIC');
begin
	dbms_utility.exec_ddl_statement@gcat(
	q'[
		create or replace procedure enable_rest authid current_user is
		begin
			execute immediate
			q'!
				begin
					ords_admin.enable_schema(
						p_enabled             => true,
						p_schema              => 'GCAT_PUBLIC',
						p_url_mapping_type    => 'BASE_PATH',
						p_url_mapping_pattern => 'GCAT_PUBLIC',
						p_auto_rest_auth      => true
					);
					commit;
				end;
			!';
		end;
	]');

	execute immediate
	q'[
		begin
			enable_rest@gcat;
		end;
	]';

	dbms_utility.exec_ddl_statement@gcat(
	q'[
		drop procedure enable_rest
	]');
end;
/


--Public URL:
--(Initial schema will be empty.)
--https://pa6nsglmabwahpe-gcat.adb.us-ashburn-1.oraclecloudapps.com/ords/GCAT_PUBLIC/_sdw/?nav=worksheet

;


--------------------------------------------------------------------------------
-- Copy tables to the cloud.
-- TODO: Can I do this with datapump?
--------------------------------------------------------------------------------

declare
	v_table_name varchar2(128);
	v_count number;
	v_sql clob;

	procedure drop_remote_gcat_tables_if_exists is
		v_tables_does_not_exist exception;
		pragma exception_init(v_tables_does_not_exist, -942);
	begin
		for i in reverse 1 .. gcat_helper.c_ordered_objects.count loop
			begin
				dbms_utility.exec_ddl_statement@gcat('drop table gcat.' || gcat_helper.c_ordered_objects(i) || ' purge');
			exception when v_tables_does_not_exist then
				null;
			end;
		end loop;
	end drop_remote_gcat_tables_if_exists;

	procedure create_remote_gcat_tables is
	begin
		dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', false);
		--Why doesn't this work? This would be much better than the "REPLACE" option.
		--dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'REMAP_SCHEMA', 'JHELLER', 'GCAT');

		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			v_table_name := gcat_helper.c_ordered_objects(i);

			--Check if table exists locally.
			select count(*) into v_count from user_tables where table_name = v_table_name;

			--If table exists, create it remotely and populate it.
			if v_count = 1 then
				select replace(dbms_metadata.get_ddl('TABLE', v_table_name) ,'"'||user||'"', '"GCAT"')
				into v_sql
				from dual;

				begin
					dbms_utility.exec_ddl_statement@gcat(v_sql);
				exception when others then
					raise_application_error(-20000, 'Error with this SQL: ' || v_sql || chr(10) || sqlerrm);
				end;
			end if;
		end loop;
	end create_remote_gcat_tables;

	procedure populate_remote_gcat_tables is
	begin
		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			v_table_name := gcat_helper.c_ordered_objects(i);

			--Check if table exists locally.
			select count(*) into v_count from user_tables where table_name = v_table_name;

			--If table exists, create it remotely and populate it.
			if v_count = 1 then
				begin
					v_sql :=
					'
						insert into gcat.'||v_table_name||'@gcat
						select * from '||v_table_name
					;

					execute immediate v_sql;
				exception when others then
					raise_application_error(-20000, 'Error with this SQL: ' || v_sql || chr(10) || sqlerrm);
				end;
			end if;
		end loop;
	end populate_remote_gcat_tables;

	procedure create_public_synonyms_and_grants is
	begin
		dbms_utility.exec_ddl_statement@gcat(
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
		]');

		execute immediate 'begin create_public_synonyms_and_grants@gcat; end;';

		dbms_utility.exec_ddl_statement@gcat(
		q'[
			drop procedure create_public_synonyms_and_grants
		]');
	end create_public_synonyms_and_grants;

begin
	drop_remote_gcat_tables_if_exists;
	create_remote_gcat_tables;
	populate_remote_gcat_tables;
	commit;
	create_public_synonyms_and_grants;
end;
/





--------------------------------------------------------------------------------
-- Create export data pump.
--------------------------------------------------------------------------------

--Based on: https://oracle-base.com/articles/misc/data-pump-api#table-export
declare
  l_dp_handle       number;
  v_date_string varchar2(100) := to_char(sysdate, 'YYYYMMDDHH24MISS');

	function get_table_expression return varchar2 is
		v_table_expression varchar2(32767) := 'in (';
	begin
		--Create an IN list of all table names.
		for i in 1 .. gcat_helper.c_ordered_objects.count loop
			v_table_expression := v_table_expression || '''' || gcat_helper.c_ordered_objects(i) || ''',';
		end loop;

		--Remove the last comma, add a closing parenthesis.
		v_table_expression := substr(v_table_expression, 1, length(v_table_expression) - 1);
		v_table_expression := v_table_expression || ')';

		--DEBUG:
		--dbms_output.put_line(v_table_expression);

		return v_table_expression;
	end;

begin
  -- Open a table export job.
  l_dp_handle := dbms_datapump.open(
    operation   => 'EXPORT',
    job_mode    => 'TABLE',
    remote_link => NULL,
    job_name    => 'GCAT_EXPORT_'||v_date_string,
    version     => 'LATEST');

  -- Specify the dump file name and directory object name.
  dbms_datapump.add_file(
    handle    => l_dp_handle,
    filename  => 'GCAT_'||v_date_string||'.dmp',
    directory => 'DATA_PUMP_DIR');

  -- Specify the log file name and directory object name.
  dbms_datapump.add_file(
    handle    => l_dp_handle,
    filename  => 'GCAT_'||v_date_string||'.log',
    directory => 'DATA_PUMP_DIR',
    filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);

  -- Specify the table to be exported, filtering the schema and table.
  dbms_datapump.metadata_filter(
    handle => l_dp_handle,
    name   => 'SCHEMA_EXPR',
    value  => '= ''JHELLER''');

  dbms_datapump.metadata_filter(
    handle => l_dp_handle,
    name   => 'NAME_EXPR',
	value  => get_table_expression());
    --value  => 'in (''LAUNCH'', ''SATELLITE'')');

  dbms_datapump.set_parameter(
    handle => l_dp_handle,
    name   => 'COMPRESSION'	,
    value  => 'ALL');

  dbms_datapump.start_job(l_dp_handle);

  dbms_datapump.detach(l_dp_handle);
end;
/

--Monitor the job:
select *
from   dba_datapump_jobs
where job_name like 'GCAT%'
order by 1, 2;


--Manually copy the file to OCI storage.
