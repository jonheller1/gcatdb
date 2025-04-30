-- This file creates the gcatdb schema for Postgres databases.
-- DO NOT MODIFY THIS FILE.  It is automatically generated.

-- Print intro message.
\echo ------------------------------------------------------------------------
\echo -- Installing gcatdb.
\echo --
\echo -- Data from GCAT (J. McDowell, planet4589.org/space/gcat)
\echo -- Schema and scripts from Jon Heller, jon@jonheller.org
\echo -- The database installs 30 tables and uses about 55MB of space.
\echo --
\echo -- The installation will run for about a minute and will stop on any
\echo -- errors.  You should see a "done" message at the end.
\echo ------------------------------------------------------------------------

-- Session settings.
set client_encoding to 'utf8';




--------------------------------------------------------------------------------
-- ORGANIZATION_TYPE
--------------------------------------------------------------------------------


CREATE TABLE ORGANIZATION_TYPE 
 (	OT_TYPE VARCHAR(3), 
	OT_MEANING VARCHAR(138), 
	OT_GROUP VARCHAR(28), 
	 CONSTRAINT PK_ORGANIZATION_TYPE PRIMARY KEY (OT_TYPE)
  
 ) ;

\copy ORGANIZATION_TYPE from 'c:\gcatdb\exports\ORGANIZATION_TYPE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- ORGANIZATION
--------------------------------------------------------------------------------


CREATE TABLE ORGANIZATION 
 (	O_CODE VARCHAR(8), 
	O_UCODE VARCHAR(8), 
	O_STATECODE VARCHAR(6), 
	O_CLASS VARCHAR(1), 
	O_TSTART DATE, 
	O_TSTART_PRECISION VARCHAR(7), 
	O_TSTOP DATE, 
	O_TSTOP_PRECISION VARCHAR(7), 
	O_SHORTNAME VARCHAR(17), 
	O_NAME VARCHAR(79), 
	O_LOCATION VARCHAR(48), 
	O_LONGITUDE NUMERIC, 
	O_LATITUDE NUMERIC, 
	O_ERROR NUMERIC, 
	O_PARENT_O_CODE VARCHAR(7), 
	O_SHORTENAME VARCHAR(16), 
	O_ENAME VARCHAR(60), 
	O_UNAME VARCHAR(227), 
	 CONSTRAINT PK_ORGANIZATION PRIMARY KEY (O_CODE)
  , 
	 CONSTRAINT FK_ORGANIZATION_ORGANIZATION FOREIGN KEY (O_PARENT_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy ORGANIZATION from 'c:\gcatdb\exports\ORGANIZATION.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- PLATFORM
--------------------------------------------------------------------------------


CREATE TABLE PLATFORM 
 (	P_CODE VARCHAR(9), 
	P_UCODE VARCHAR(9), 
	P_STATECODE VARCHAR(2), 
	P_TYPE VARCHAR(4), 
	P_CLASS VARCHAR(1), 
	P_TSTART DATE, 
	P_TSTART_PRECISION VARCHAR(5), 
	P_TSTOP DATE, 
	P_TSTOP_PRECISION VARCHAR(5), 
	P_SHORTNAME VARCHAR(17), 
	P_NAME VARCHAR(77), 
	P_LOCATION VARCHAR(26), 
	P_LONGITUDE NUMERIC, 
	P_LATITUDE NUMERIC, 
	P_ERROR NUMERIC, 
	P_SHORTENAME VARCHAR(4000), 
	P_ENAME VARCHAR(4000), 
	P_VCLASS VARCHAR(17), 
	P_VCLASSID VARCHAR(12), 
	P_VID VARCHAR(12), 
	P_GROUP VARCHAR(13), 
	P_UNAME VARCHAR(77), 
	 CONSTRAINT PK_PLATFORM PRIMARY KEY (P_CODE)
  
 ) ;

\copy PLATFORM from 'c:\gcatdb\exports\PLATFORM.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- PLATFORM_ORG
--------------------------------------------------------------------------------


CREATE TABLE PLATFORM_ORG 
 (	PO_P_CODE VARCHAR(9), 
	PO_O_CODE VARCHAR(6), 
	 CONSTRAINT PK_PLATFORM_ORG PRIMARY KEY (PO_P_CODE, PO_O_CODE)
  , 
	 CONSTRAINT FK_PLATFORM_ORG_PLATFORM FOREIGN KEY (PO_P_CODE)
	  REFERENCES PLATFORM (P_CODE) , 
	 CONSTRAINT FK_PLATFORM_ORG_ORG FOREIGN KEY (PO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy PLATFORM_ORG from 'c:\gcatdb\exports\PLATFORM_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- ORGANIZATION_ORG_TYPE
--------------------------------------------------------------------------------


CREATE TABLE ORGANIZATION_ORG_TYPE 
 (	OOT_O_CODE VARCHAR(8), 
	OOT_OT_TYPE VARCHAR(3), 
	 CONSTRAINT PK_ORGANIZATION_ORG_TYPE PRIMARY KEY (OOT_O_CODE, OOT_OT_TYPE)
  , 
	 CONSTRAINT FK_ORGANIZATION_ORG_TYPE_ORGANIZATION FOREIGN KEY (OOT_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) , 
	 CONSTRAINT FK_ORGANIZATION_ORG_TYPE_ORGANIZATION_TYPE FOREIGN KEY (OOT_OT_TYPE)
	  REFERENCES ORGANIZATION_TYPE (OT_TYPE) 
 ) ;

\copy ORGANIZATION_ORG_TYPE from 'c:\gcatdb\exports\ORGANIZATION_ORG_TYPE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- SITE
--------------------------------------------------------------------------------


CREATE TABLE SITE 
 (	SITE_CODE VARCHAR(7), 
	SITE_UCODE VARCHAR(7), 
	SITE_TYPE VARCHAR(2), 
	SITE_STATECODE VARCHAR(5), 
	SITE_TSTART DATE, 
	SITE_TSTART_PRECISION VARCHAR(7), 
	SITE_TSTOP DATE, 
	SITE_TSTOP_PRECISION VARCHAR(7), 
	SITE_SHORTNAME VARCHAR(16), 
	SITE_NAME VARCHAR(78), 
	SITE_LOCATION VARCHAR(41), 
	SITE_LONGITUDE NUMERIC, 
	SITE_LATITUDE NUMERIC, 
	SITE_ERROR NUMERIC, 
	SITE_SHORTENAME VARCHAR(10), 
	SITE_ENAME VARCHAR(20), 
	SITE_GROUP VARCHAR(9), 
	SITE_UNAME VARCHAR(82), 
	 CONSTRAINT PK_SITE PRIMARY KEY (SITE_CODE)
  
 ) ;

\copy SITE from 'c:\gcatdb\exports\SITE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- SITE_ORG
--------------------------------------------------------------------------------


CREATE TABLE SITE_ORG 
 (	SO_SITE_CODE VARCHAR(7), 
	SO_O_CODE VARCHAR(7), 
	 CONSTRAINT PK_SITE_ORG PRIMARY KEY (SO_SITE_CODE, SO_O_CODE)
  , 
	 CONSTRAINT FK_SITE_ORG_SITE FOREIGN KEY (SO_SITE_CODE)
	  REFERENCES SITE (SITE_CODE) , 
	 CONSTRAINT FK_SITE_ORG_ORG FOREIGN KEY (SO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy SITE_ORG from 'c:\gcatdb\exports\SITE_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_POINT
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_POINT 
 (	LP_SITE_CODE VARCHAR(7), 
	LP_CODE VARCHAR(12), 
	LP_UCODE VARCHAR(7), 
	LP_TYPE VARCHAR(2), 
	LP_STATECODE VARCHAR(4000), 
	LP_TSTART DATE, 
	LP_TSTART_PRECISION VARCHAR(7), 
	LP_TSTOP DATE, 
	LP_TSTOP_PRECISION VARCHAR(7), 
	LP_SHORTNAME VARCHAR(17), 
	LP_NAME VARCHAR(80), 
	LP_LOCATION VARCHAR(38), 
	LP_LONGITUDE NUMERIC, 
	LP_LATITUDE NUMERIC, 
	LP_ERROR NUMERIC, 
	LP_SHORTENAME VARCHAR(9), 
	LP_ENAME VARCHAR(15), 
	LP_UNAME VARCHAR(72), 
	 CONSTRAINT PK_LAUNCH_POINT PRIMARY KEY (LP_SITE_CODE, LP_CODE)
  , 
	 CONSTRAINT FK_LAUNCH_POINT_SITE FOREIGN KEY (LP_SITE_CODE)
	  REFERENCES SITE (SITE_CODE) 
 ) ;

\copy LAUNCH_POINT from 'c:\gcatdb\exports\LAUNCH_POINT.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_POINT_ORG
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_POINT_ORG 
 (	LPO_SITE_CODE VARCHAR(7), 
	LPO_LP_CODE VARCHAR(12), 
	LPO_O_CODE VARCHAR(6), 
	 CONSTRAINT PK_LAUNCH_POINT_ORG PRIMARY KEY (LPO_SITE_CODE, LPO_LP_CODE, LPO_O_CODE)
  , 
	 CONSTRAINT FK_LAUNCH_POINT_ORG_LAUNCH_POINT FOREIGN KEY (LPO_SITE_CODE, LPO_LP_CODE)
	  REFERENCES LAUNCH_POINT (LP_SITE_CODE, LP_CODE) , 
	 CONSTRAINT FK_LAUNCH_POINT_ORG_ORG FOREIGN KEY (LPO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy LAUNCH_POINT_ORG from 'c:\gcatdb\exports\LAUNCH_POINT_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_VEHICLE_FAMILY
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_VEHICLE_FAMILY 
 (	LVF_FAMILY VARCHAR(15), 
	 CONSTRAINT PK_LAUNCH_VEHICLE_FAMILY PRIMARY KEY (LVF_FAMILY)
  
 ) ;

\copy LAUNCH_VEHICLE_FAMILY from 'c:\gcatdb\exports\LAUNCH_VEHICLE_FAMILY.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_VEHICLE
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_VEHICLE 
 (	LV_NAME VARCHAR(24), 
	LV_LVF_FAMILY VARCHAR(12), 
	LV_VARIANT VARCHAR(6), 
	LV_ALIAS VARCHAR(14), 
	LV_MIN_STAGE NUMERIC, 
	LV_MAX_STAGE NUMERIC, 
	LV_LENGTH NUMERIC, 
	LV_LFLAG VARCHAR(1), 
	LV_DIAMETER NUMERIC, 
	LV_DFLAG VARCHAR(1), 
	LV_LAUNCH_MASS NUMERIC, 
	LV_MFLAG VARCHAR(1), 
	LV_LEO_CAPACITY NUMERIC, 
	LV_GTO_CAPACITY NUMERIC, 
	LV_TO_THRUST NUMERIC, 
	LV_CLASS VARCHAR(1), 
	LV_APOGEE NUMERIC, 
	LV_RANGE NUMERIC, 
	 CONSTRAINT UQ_LAUNCH_VEHICLE UNIQUE (LV_NAME, LV_VARIANT)
  , 
	 CONSTRAINT FK_LAUNCH_VEHICLE_LAUNCH_VEHICLE_FAMILY FOREIGN KEY (LV_LVF_FAMILY)
	  REFERENCES LAUNCH_VEHICLE_FAMILY (LVF_FAMILY) 
 ) ;

\copy LAUNCH_VEHICLE from 'c:\gcatdb\exports\LAUNCH_VEHICLE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_VEHICLE_ORG
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_VEHICLE_ORG 
 (	LVO_LV_NAME VARCHAR(24), 
	LVO_LV_VARIANT VARCHAR(6), 
	LVO_O_CODE VARCHAR(7), 
	 CONSTRAINT UQ_LAUNCH_VEHICLE_ORG UNIQUE (LVO_LV_NAME, LVO_LV_VARIANT, LVO_O_CODE)
  , 
	 CONSTRAINT FK_LAUNCH_VEHICLE_ORG_LAUNCH_VEHICLE FOREIGN KEY (LVO_LV_NAME, LVO_LV_VARIANT)
	  REFERENCES LAUNCH_VEHICLE (LV_NAME, LV_VARIANT) , 
	 CONSTRAINT FK_LAUNCH_VEHICLE_ORG_ORG FOREIGN KEY (LVO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy LAUNCH_VEHICLE_ORG from 'c:\gcatdb\exports\LAUNCH_VEHICLE_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- REFERENCE
--------------------------------------------------------------------------------


CREATE TABLE REFERENCE 
 (	R_CITE VARCHAR(21), 
	R_REFERENCE VARCHAR(120), 
	 CONSTRAINT PK_REFERENCE PRIMARY KEY (R_CITE)
  
 ) ;

\copy REFERENCE from 'c:\gcatdb\exports\REFERENCE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH 
 (	L_LAUNCH_TAG VARCHAR(10), 
	L_LAUNCH_JD NUMERIC, 
	L_LAUNCH_DATE DATE, 
	L_LAUNCH_DATE_PRECISION VARCHAR(8), 
	L_LV_NAME VARCHAR(24), 
	L_LV_VARIANT VARCHAR(6), 
	L_FLIGHT_ID VARCHAR(20), 
	L_FLIGHT VARCHAR(24), 
	L_MISSION VARCHAR(24), 
	L_FLIGHTCODE VARCHAR(24), 
	L_P_CODE VARCHAR(9), 
	L_LAUNCH_SITE_LP_SITE_CODE VARCHAR(7), 
	L_LAUNCH_PAD_LP_CODE VARCHAR(12), 
	L_ASCENT_SITE_LP_SITE_CODE VARCHAR(5), 
	L_ASCENT_PAD_LP_CODE VARCHAR(10), 
	L_APOGEE NUMERIC, 
	L_APOFLAG VARCHAR(1), 
	L_RANGE NUMERIC, 
	L_RANGEFLAG VARCHAR(1), 
	L_DEST VARCHAR(12), 
	L_ORBPAY VARCHAR(7), 
	L_LAUNCH_CATEGORY VARCHAR(1), 
	L_LAUNCH_STATUS VARCHAR(1), 
	L_LAUNCH_SUCCESS_FRACTION NUMERIC, 
	L_FAIL_CODE VARCHAR(6), 
	L_LAUNCH_SERVICE_TYPE VARCHAR(2), 
	L_CATEGORY VARCHAR(24), 
	L_PRIMARY_R_CITE VARCHAR(20), 
	L_ADDITIONAL_R_CITE VARCHAR(20), 
	L_NOTES VARCHAR(32), 
	 CONSTRAINT PK_LAUNCH PRIMARY KEY (L_LAUNCH_TAG)
  , 
	 CONSTRAINT FK_LAUNCH_PLATFORM FOREIGN KEY (L_P_CODE)
	  REFERENCES PLATFORM (P_CODE) , 
	 CONSTRAINT FK_LAUNCH_LAUNCH_SITE FOREIGN KEY (L_LAUNCH_SITE_LP_SITE_CODE)
	  REFERENCES SITE (SITE_CODE) , 
	 CONSTRAINT FK_LAUNCH_LAUNCH_POINT FOREIGN KEY (L_LAUNCH_SITE_LP_SITE_CODE, L_LAUNCH_PAD_LP_CODE)
	  REFERENCES LAUNCH_POINT (LP_SITE_CODE, LP_CODE) , 
	 CONSTRAINT FK_LAUNCH_ASCENT_SITE FOREIGN KEY (L_ASCENT_SITE_LP_SITE_CODE)
	  REFERENCES SITE (SITE_CODE) , 
	 CONSTRAINT FK_LAUNCH_ASCENT_POINT FOREIGN KEY (L_ASCENT_SITE_LP_SITE_CODE, L_ASCENT_PAD_LP_CODE)
	  REFERENCES LAUNCH_POINT (LP_SITE_CODE, LP_CODE) , 
	 CONSTRAINT FK_LAUNCH_LAUNCH_VEHICLE FOREIGN KEY (L_LV_NAME, L_LV_VARIANT)
	  REFERENCES LAUNCH_VEHICLE (LV_NAME, LV_VARIANT) 
 ) ;

\copy LAUNCH from 'c:\gcatdb\exports\LAUNCH.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_AGENCY_ORG
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_AGENCY_ORG 
 (	LAO_L_LAUNCH_TAG VARCHAR(10), 
	LAO_O_CODE VARCHAR(8), 
	 CONSTRAINT PK_LAUNCH_AGENCY_ORG PRIMARY KEY (LAO_L_LAUNCH_TAG, LAO_O_CODE)
  , 
	 CONSTRAINT FK_LAUNCH_AGENCY_ORG_LAUNCH FOREIGN KEY (LAO_L_LAUNCH_TAG)
	  REFERENCES LAUNCH (L_LAUNCH_TAG) , 
	 CONSTRAINT FK_LAUNCH_AGENCY_ORG_ORG FOREIGN KEY (LAO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy LAUNCH_AGENCY_ORG from 'c:\gcatdb\exports\LAUNCH_AGENCY_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_PAYLOAD_ORG
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_PAYLOAD_ORG 
 (	LPO_L_LAUNCH_TAG VARCHAR(1000), 
	LPO_O_CODE VARCHAR(4000), 
	 CONSTRAINT PK_LAUNCH_PAYLOAD_ORG PRIMARY KEY (LPO_L_LAUNCH_TAG, LPO_O_CODE)
  , 
	 CONSTRAINT FK_LAUNCH_PAYLOAD_ORG_LAUNCH FOREIGN KEY (LPO_L_LAUNCH_TAG)
	  REFERENCES LAUNCH (L_LAUNCH_TAG) , 
	 CONSTRAINT FK_LAUNCH_PAYLOAD_ORG_ORG FOREIGN KEY (LPO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy LAUNCH_PAYLOAD_ORG from 'c:\gcatdb\exports\LAUNCH_PAYLOAD_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_INVESTIGATOR
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_INVESTIGATOR 
 (	LI_L_LAUNCH_TAG VARCHAR(10), 
	LI_INVESTIGATOR VARCHAR(18), 
	 CONSTRAINT PK_LAUNCH_INVESTIGATOR PRIMARY KEY (LI_L_LAUNCH_TAG, LI_INVESTIGATOR)
  , 
	 CONSTRAINT FK_LAUNCH_INVESTIGATOR_LAUNCH FOREIGN KEY (LI_L_LAUNCH_TAG)
	  REFERENCES LAUNCH (L_LAUNCH_TAG) 
 ) ;

\copy LAUNCH_INVESTIGATOR from 'c:\gcatdb\exports\LAUNCH_INVESTIGATOR.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- WORLD
--------------------------------------------------------------------------------


CREATE TABLE WORLD 
 (	W_ID VARCHAR(2), 
	W_IDNAME VARCHAR(10), 
	W_NAME VARCHAR(23), 
	W_ALTNAME VARCHAR(17), 
	W_RADIUS NUMERIC, 
	W_POLAR_RADIUS NUMERIC, 
	W_MASS NUMERIC, 
	W_SEMIMAJORAXIS NUMERIC, 
	W_PERIAPSIS NUMERIC, 
	W_ECC NUMERIC, 
	W_INC NUMERIC, 
	W_NODE NUMERIC, 
	W_PERI NUMERIC, 
	W_M NUMERIC, 
	W_EPOCH DATE, 
	W_EPOCH_PRECISION VARCHAR(6), 
	W_ROTPERIOD NUMERIC, 
	W_ORBPERIOD NUMERIC, 
	W_EPHEMERIS VARCHAR(7), 
	W_WTYPE VARCHAR(2), 
	W_PRIMARY_W_NAME VARCHAR(17), 
	W_SPIN_RHO NUMERIC, 
	W_SPIN_INTERTIAL_FACTOR NUMERIC, 
	W_SPIN_ICRS_POSITION_RA NUMERIC, 
	W_SPIN_ICRS_POSITION_DEC NUMERIC, 
	W_SPIN_MERIDIAN NUMERIC, 
	W_SPIN_RATE NUMERIC, 
	W_SPIN_J2 NUMERIC, 
	W_SPIN_J4 NUMERIC, 
	W_SPIN_J6 NUMERIC, 
	W_SPIN_POLE_RA_RATE NUMERIC, 
	W_SPIN_POLE_DEC_RATE NUMERIC, 
	W_SPIN_POLE_FUNCTION VARCHAR(5), 
	W_SPIN_SPIN_FUNCTION VARCHAR(5), 
	W_SPIN_INIT_FUNCTION VARCHAR(4), 
	W_SPIN_JFILE VARCHAR(11), 
	 CONSTRAINT PK_WORLD PRIMARY KEY (W_IDNAME)
  
 ) ;

\copy WORLD from 'c:\gcatdb\exports\WORLD.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- SATELLITE
--------------------------------------------------------------------------------


CREATE TABLE SATELLITE 
 (	S_CATALOG VARCHAR(7), 
	S_JCAT VARCHAR(10) NOT NULL , 
	S_SATCAT VARCHAR(6), 
	S_L_LAUNCH_TAG VARCHAR(10), 
	S_PIECE VARCHAR(13), 
	S_TYPE_BYTE_1 VARCHAR(1), 
	S_TYPE_BYTE_2 VARCHAR(1), 
	S_TYPE_BYTE_3 VARCHAR(1), 
	S_TYPE_BYTE_4 VARCHAR(1), 
	S_TYPE_BYTE_5 VARCHAR(1), 
	S_TYPE_BYTE_6 VARCHAR(1), 
	S_TYPE_BYTE_7 VARCHAR(1), 
	S_TYPE_BYTE_8 VARCHAR(1), 
	S_TYPE_BYTE_9 VARCHAR(1), 
	S_NAME VARCHAR(28), 
	S_PLNAME VARCHAR(29), 
	S_LDATE DATE, 
	S_LDATE_PRECISION VARCHAR(8), 
	S_PARENT_S_JCAT_OR_W_NAME VARCHAR(21), 
	S_PARENT_PORT VARCHAR(6), 
	S_PARENT_FLAG VARCHAR(1), 
	S_SDATE DATE, 
	S_SDATE_PRECISION VARCHAR(8), 
	S_PRIMARY_W_NAME VARCHAR(12), 
	S_DDATE DATE, 
	S_DDATE_PRECISION VARCHAR(8), 
	S_STATUS VARCHAR(6), 
	S_DEST VARCHAR(14), 
	S_STATE_O_CODE VARCHAR(6), 
	S_BUS VARCHAR(17), 
	S_MOTOR VARCHAR(13), 
	S_MASS NUMERIC, 
	S_MASSFLAG VARCHAR(1), 
	S_DRYMASS NUMERIC, 
	S_DRYFLAG VARCHAR(1), 
	S_TOTMASS NUMERIC, 
	S_TOTFLAG VARCHAR(1), 
	S_LENGTH NUMERIC, 
	S_LFLAG VARCHAR(1), 
	S_DIAMETER NUMERIC, 
	S_DFLAG VARCHAR(1), 
	S_SPAN NUMERIC, 
	S_SPANFLAG VARCHAR(1), 
	S_SHAPE VARCHAR(27), 
	S_ODATE DATE, 
	S_ODATE_PRECISION VARCHAR(8), 
	S_PERIGEE NUMERIC, 
	S_PF VARCHAR(1), 
	S_APOGEE DOUBLE PRECISION, 
	S_AF VARCHAR(1), 
	S_INC NUMERIC, 
	S_IF VARCHAR(1), 
	S_OPORBIT VARCHAR(6), 
	S_OQUAL VARCHAR(3), 
	S_ALTNAMES VARCHAR(52), 
	 CONSTRAINT FK_SATELLITE_STATE_ORG FOREIGN KEY (S_STATE_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) , 
	 CONSTRAINT FK_SATELLITE_LAUNCH FOREIGN KEY (S_L_LAUNCH_TAG)
	  REFERENCES LAUNCH (L_LAUNCH_TAG) 
 ) ;


CREATE INDEX SATELLITE_IDX1 ON SATELLITE (S_JCAT) 
;
\copy SATELLITE from 'c:\gcatdb\exports\SATELLITE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- SATELLITE_OWNER_ORG
--------------------------------------------------------------------------------


CREATE TABLE SATELLITE_OWNER_ORG 
 (	SOO_S_JCAT VARCHAR(6), 
	SOO_O_CODE VARCHAR(7), 
	 CONSTRAINT PK_SATELLITE_OWNER_ORG PRIMARY KEY (SOO_S_JCAT, SOO_O_CODE)
  , 
	 CONSTRAINT FK_SATELLITE_OWNER_ORG_ORGANIZATION FOREIGN KEY (SOO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy SATELLITE_OWNER_ORG from 'c:\gcatdb\exports\SATELLITE_OWNER_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- SATELLITE_MANUFACTURER_ORG
--------------------------------------------------------------------------------


CREATE TABLE SATELLITE_MANUFACTURER_ORG 
 (	SMO_S_JCAT VARCHAR(6), 
	SMO_O_CODE VARCHAR(7), 
	 CONSTRAINT PK_SATELLITE_MANUFACTURER_ORG PRIMARY KEY (SMO_S_JCAT, SMO_O_CODE)
  , 
	 CONSTRAINT FK_SATELLITE_MANUFACTURER_ORG_ORGANIZATION FOREIGN KEY (SMO_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy SATELLITE_MANUFACTURER_ORG from 'c:\gcatdb\exports\SATELLITE_MANUFACTURER_ORG.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- PAYLOAD
--------------------------------------------------------------------------------


CREATE TABLE PAYLOAD 
 (	PAY_CATALOG VARCHAR(8), 
	PAY_JCAT VARCHAR(6), 
	PAY_PIECE VARCHAR(12), 
	PAY_NAME VARCHAR(28), 
	PAY_LDATE DATE, 
	PAY_LDATE_PRECISION VARCHAR(3), 
	PAY_TLAST DATE, 
	PAY_TLAST_PRECISION VARCHAR(7), 
	PAY_TOP DATE, 
	PAY_TOP_PRECISION VARCHAR(8), 
	PAY_TDATE DATE, 
	PAY_TDATE_PRECISION VARCHAR(8), 
	PAY_TF VARCHAR(1), 
	PAY_PROGRAM VARCHAR(16), 
	PAY_PLANE VARCHAR(8), 
	PAY_ATT VARCHAR(1), 
	PAY_MVR VARCHAR(1), 
	PAY_CLASS VARCHAR(2), 
	PAY_RESULT VARCHAR(1), 
	PAY_CONTROL VARCHAR(20), 
	PAY_UNSTATE_O_CODE VARCHAR(6), 
	PAY_IS_REGISTERED VARCHAR(3), 
	PAY_UNREG VARCHAR(17), 
	PAY_UNPERIOD NUMERIC, 
	PAY_UNPERIGEE NUMERIC, 
	PAY_UNAPOGEE NUMERIC, 
	PAY_UNINC NUMERIC, 
	PAY_DISPEPOCH DATE, 
	PAY_DISPEPOCH_PRECISION VARCHAR(3), 
	PAY_DISPPERI NUMERIC, 
	PAY_DISPAPO NUMERIC, 
	PAY_DISPINC NUMERIC, 
	PAY_COMMENT VARCHAR(94), 
	 CONSTRAINT PK_PAYLOAD PRIMARY KEY (PAY_JCAT)
  , 
	 CONSTRAINT FK_PAYLOAD_ORG FOREIGN KEY (PAY_UNSTATE_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy PAYLOAD from 'c:\gcatdb\exports\PAYLOAD.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- PAYLOAD_CATEGORY
--------------------------------------------------------------------------------


CREATE TABLE PAYLOAD_CATEGORY 
 (	PC_PAY_JCAT VARCHAR(6), 
	PC_CATEGORY VARCHAR(6), 
	PC_IS_SECRET VARCHAR(3), 
	 CONSTRAINT PK_PAYLOAD_CATEGORY PRIMARY KEY (PC_PAY_JCAT, PC_CATEGORY)
  , 
	 CONSTRAINT FK_PAYLOAD_CATEGORY_PAYLOAD FOREIGN KEY (PC_PAY_JCAT)
	  REFERENCES PAYLOAD (PAY_JCAT) 
 ) ;

\copy PAYLOAD_CATEGORY from 'c:\gcatdb\exports\PAYLOAD_CATEGORY.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- PAYLOAD_DISCIPLINE
--------------------------------------------------------------------------------


CREATE TABLE PAYLOAD_DISCIPLINE 
 (	PD_PAY_JCAT VARCHAR(6), 
	PD_DISCIPLINE VARCHAR(4), 
	 CONSTRAINT PK_PAYLOAD_DISCIPLINE PRIMARY KEY (PD_PAY_JCAT, PD_DISCIPLINE)
  , 
	 CONSTRAINT FK_PAYLOAD_DISCIPLINE_PAYLOAD FOREIGN KEY (PD_PAY_JCAT)
	  REFERENCES PAYLOAD (PAY_JCAT) 
 ) ;

\copy PAYLOAD_DISCIPLINE from 'c:\gcatdb\exports\PAYLOAD_DISCIPLINE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- ENGINE
--------------------------------------------------------------------------------


CREATE TABLE ENGINE 
 (	E_ID NUMERIC, 
	E_NAME VARCHAR(19), 
	E_FAMILY VARCHAR(17), 
	E_ALT_NAME VARCHAR(12), 
	E_MASS NUMERIC, 
	E_MFLAG VARCHAR(1), 
	E_IMPULSE NUMERIC, 
	E_IMPFLAG VARCHAR(1), 
	E_THRUST NUMERIC, 
	E_TFLAG VARCHAR(1), 
	E_ISP NUMERIC, 
	E_ISPFLAG VARCHAR(1), 
	E_DURATION NUMERIC, 
	E_DURFLAG VARCHAR(1), 
	E_CHAMBERS NUMERIC, 
	E_DATE DATE, 
	E_DATE_PRECISION VARCHAR(7), 
	E_USAGE VARCHAR(20), 
	E_GROUP VARCHAR(11), 
	 CONSTRAINT PK_ENGINE PRIMARY KEY (E_ID)
  
 ) ;

\copy ENGINE from 'c:\gcatdb\exports\ENGINE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- ENGINE_PROPELLANT
--------------------------------------------------------------------------------


CREATE TABLE ENGINE_PROPELLANT 
 (	EP_E_ID NUMERIC, 
	EP_PROPELLANT VARCHAR(20), 
	EP_FUEL_OR_OXIDIZER VARCHAR(8), 
	 CONSTRAINT PK_ENGINE_PROPELLANT PRIMARY KEY (EP_E_ID, EP_PROPELLANT, EP_FUEL_OR_OXIDIZER)
  , 
	 CONSTRAINT FK_ENGINE_PROPELLANT_ENGINE FOREIGN KEY (EP_E_ID)
	  REFERENCES ENGINE (E_ID) 
 ) ;

\copy ENGINE_PROPELLANT from 'c:\gcatdb\exports\ENGINE_PROPELLANT.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- ENGINE_MANUFACTURER
--------------------------------------------------------------------------------


CREATE TABLE ENGINE_MANUFACTURER 
 (	EM_E_ID NUMERIC, 
	EM_MANUFACTURER_O_CODE VARCHAR(7), 
	 CONSTRAINT PK_ENGINE_MANUFACTURER PRIMARY KEY (EM_E_ID, EM_MANUFACTURER_O_CODE)
  , 
	 CONSTRAINT FK_ENGINE_MANUFACTURER_ENGINE FOREIGN KEY (EM_E_ID)
	  REFERENCES ENGINE (E_ID) , 
	 CONSTRAINT FK_ENGINE_MANUFACTURER_ORGANIZATION FOREIGN KEY (EM_MANUFACTURER_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy ENGINE_MANUFACTURER from 'c:\gcatdb\exports\ENGINE_MANUFACTURER.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- STAGE
--------------------------------------------------------------------------------


CREATE TABLE STAGE 
 (	STAGE_NAME VARCHAR(20), 
	STAGE_LVF_FAMILY VARCHAR(15), 
	STAGE_ALT_NAME VARCHAR(19), 
	STAGE_LENGTH NUMERIC, 
	STAGE_DIAMETER NUMERIC, 
	STAGE_LAUNCH_MASS NUMERIC, 
	STAGE_DRY_MASS NUMERIC, 
	STAGE_THRUST NUMERIC, 
	STAGE_DURATION NUMERIC, 
	STAGE_E_ID NUMERIC, 
	STAGE_NENG VARCHAR(2), 
	 CONSTRAINT PK_STAGE PRIMARY KEY (STAGE_NAME)
  , 
	 CONSTRAINT FK_STAGE_LAUNCH_VEHICLE_FAMILY FOREIGN KEY (STAGE_LVF_FAMILY)
	  REFERENCES LAUNCH_VEHICLE_FAMILY (LVF_FAMILY) , 
	 CONSTRAINT FK_STAGE_ENGINE FOREIGN KEY (STAGE_E_ID)
	  REFERENCES ENGINE (E_ID) 
 ) ;

\copy STAGE from 'c:\gcatdb\exports\STAGE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- LAUNCH_VEHICLE_STAGE
--------------------------------------------------------------------------------


CREATE TABLE LAUNCH_VEHICLE_STAGE 
 (	LVS_LV_NAME VARCHAR(24), 
	LVS_LV_VARIANT VARCHAR(6), 
	LVS_STAGE_NO VARCHAR(2), 
	LVS_STAGE_NAME VARCHAR(20), 
	LVS_STAGE_TYPE VARCHAR(18), 
	LVS_QUALIFIER VARCHAR(1), 
	LVS_DUMMY VARCHAR(1), 
	LVS_MULTIPLICITY NUMERIC, 
	LVS_STAGE_IMPULSE NUMERIC, 
	LVS_STAGE_APOGEE NUMERIC, 
	LVS_STAGE_PERIGEE NUMERIC, 
	LVS_PERIGEE_QUAL VARCHAR(1), 
	 CONSTRAINT UQ_LAUNCH_VEHICLE_STAGE UNIQUE (LVS_LV_NAME, LVS_LV_VARIANT, LVS_STAGE_NO, LVS_STAGE_NAME, LVS_STAGE_TYPE)
  , 
	 CONSTRAINT FK_LAUNCH_VEHICLE_STAGE_LAUNCH_VEHICLE FOREIGN KEY (LVS_LV_NAME, LVS_LV_VARIANT)
	  REFERENCES LAUNCH_VEHICLE (LV_NAME, LV_VARIANT) , 
	 CONSTRAINT FK_LAUNCH_VEHICLE_STAGE_STAGE FOREIGN KEY (LVS_STAGE_NAME)
	  REFERENCES STAGE (STAGE_NAME) 
 ) ;

\copy LAUNCH_VEHICLE_STAGE from 'c:\gcatdb\exports\LAUNCH_VEHICLE_STAGE.csv' delimiter ',' csv header;


--------------------------------------------------------------------------------
-- STAGE_MANUFACTURER
--------------------------------------------------------------------------------


CREATE TABLE STAGE_MANUFACTURER 
 (	SM_STAGE_NAME VARCHAR(20), 
	SM_MANUFACTURER_O_CODE VARCHAR(7), 
	 CONSTRAINT PK_STAGE_MANUFACTURER PRIMARY KEY (SM_STAGE_NAME, SM_MANUFACTURER_O_CODE)
  , 
	 CONSTRAINT FK_STAGE_MANUFACTURER_STAGE FOREIGN KEY (SM_STAGE_NAME)
	  REFERENCES STAGE (STAGE_NAME) , 
	 CONSTRAINT FK_STAGE_MANUFACTURER_ORG FOREIGN KEY (SM_MANUFACTURER_O_CODE)
	  REFERENCES ORGANIZATION (O_CODE) 
 ) ;

\copy STAGE_MANUFACTURER from 'c:\gcatdb\exports\STAGE_MANUFACTURER.csv' delimiter ',' csv header;



-- Print outro message.
\echo ------------------------------------------------------------------------
\echo Done.  Gcatdb was successfully installed.
\echo ------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- DONE
--------------------------------------------------------------------------------
