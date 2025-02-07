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
select 'launch.tsv'    file_name, 'LAUNCH_STAGING'    staging_table_name, 73426 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/launch/launch.tsv'    url, '#Launch_Tag	Launch_JD	Launch_Date	LV_Type	Variant	Flight_ID	Flight	Mission	FlightCode	Platform	Launch_Site	Launch_Pad	Ascent_Site	Ascent_Pad	Apogee	Apoflag	Range	RangeFlag	Dest	OrbPay	Agency	Launch_Code	Group	Category	LTCite	Cite	Notes' first_line from dual union all
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
select 'psatcat.tsv'   file_name, 'PSATCAT_STAGING'   staging_table_name, 20000 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/psatcat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'pauxcat.tsv'   file_name, 'PAUXCAT_STAGING'   staging_table_name,  2000 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/pauxcat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'pftocat.tsv'   file_name, 'PFTOCAT_STAGING'   staging_table_name,   500 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/pftocat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'ptmpcat.tsv'   file_name, 'PTMPCAT_STAGING'   staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/ptmpcat.tsv'      url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'plcat.tsv'     file_name, 'PLCAT_STAGING'     staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/plcat.tsv'        url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'prcat.tsv'     file_name, 'PRCAT_STAGING'     staging_table_name,   100 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/prcat.tsv'        url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'pdeepcat.tsv'  file_name, 'PDEEPCAT_STAGING'  staging_table_name,    10 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/pdeepcat.tsv'     url, '#JCAT	Piece	Name	LDate	TLast	TOp	TDate	TF	Program	Plane	Att	Mvr	Class	Category	Result	Control	Discipline	UNState	UNReg	UNPeriod	UNPerigee	UNApogee	UNInc	DispEpoch	DispPeri	DispApo	DispInc	Comment' from dual union all
select 'usatcat.tsv'   file_name, 'USATCAT_STAGING'   staging_table_name,  6070 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/cat/usatcat.tsv'      url, '#JCAT	Name' from dual union all
select 'stages.tsv'    file_name, 'STAGES_STAGING'    staging_table_name,  1430 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/stages.tsv'    url, '#Stage_Name	Stage_Family	Stage_Manufacturer	Stage_Alt_Name	Length	Diameter	Launch_Mass	Dry_Mass	Thrust	Duration	Engine	NEng' from dual union all
select 'lvs.tsv'       file_name, 'LVS_STAGING'       staging_table_name,  4350 min_expected_rows, 'https://planet4589.org/space/gcat/tsv/tables/lvs.tsv'       url, '#LV_Name	LV_Variant	Stage_No	Stage_Name	Qualifier	Dummy	Multiplicity	Stage_Impulse	Stage_Apogee	Stage_Perigee	Perigee_Qual' from dual
order by file_name;



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


