/***************************************************************************
Sub Program	: QF4 Input
Main		: Customer Datamart
Purpose		: Get all customer information from qfund4 POS system
Programmer  : Spencer Hopkins
****************************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
  01/08/2016	Spencer Hopkins		Add email address field    
  02/01/2016    Spencer Hopkins     Revise phone number logic
									 (old: primary, secondary)
									 (new: home, mobile, work, other)
  02/01/2016    Spencer Hopkins     POS_CUSTNBR = character & CUSTNBR = numeric
  02/19/2016    Spencer Hopkins     Add DATASHARECD & DO_NOT_MAIL columns
  03/28/2016    Spencer Hopkins		Remove ECA branch # 415,427 from locnbr join (closed before acquisition)
  04/12/2016    Spencer Hopkins		Optimization - only copy necessary columns
  05/13/2016	Spencer Hopkins		Remove Custnbr (numeric) & rename
									 pos_custnbr to custnbr (varchar)
  06/27/2016	Spencer Hopkins		Fix missing DL issue
  07/19/2016	Spencer Hopkins		Added placeholder for languagecd
  09/19/2016  	Spencer Hopkins		Fixed 5-digit location numbers
  02/27/2017	Spencer Hopkins		Add ETL check

*****************************************************************************
*****************************************************************************
*/


/*
============================================================================= 
     INCLUDE ERROR_CHECK
=============================================================================
*/
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\CUSTOMER\CUSTDM_ERROR_INPUTS.SAS";


/*
============================================================================= 
     SET UP LIBRARIES & MAKE COPIES OF PRODUCTION TABLES
=============================================================================
*/

%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\TOP SECRET PROGRAM.SAS";


%LET DAYS_BACK = 5;



/*** QFUND4 ***/
DATA WORK.QF4_CUST_COPY(KEEP=CUSTOMER_NBR BRANCH_NBR FIRST_NAME LAST_NAME MIDDLE_NAME PHOTO_ID_TYPE PHOTO_ID_STATE PHOTO_ID 
							SSN DOB DATE_CREATED CUSTOMER_STATUS HOME_PHONE MOBILE_PHONE WORK_PHONE EMAIL DATE_UPDATED);
	SET ECA.QF_CUSTOMER_DETAILS;
	WHERE (DATE_UPDATED >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR DATE_CREATED >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR CREATE_DATE_TIME >= DHMS(TODAY()-&DAYS_BACK.,00,00,00));
%RUNQUIT(&job,&sub4);

DATA WORK.QF4_CUSTADDRESS_COPY;
	SET ECA.QF_CUSTOMER_ADDRESS
		(KEEP=CUSTOMER_NBR ADDRESS_LN ADDRESS_LN2 PRIMARY_CITY STATE_ID POSTAL_ID);
%RUNQUIT(&job,&sub4);

DATA WORK.QF4_CUSTBANKACCT_COPY;
	SET ECA.QF_CUSTOMER_BANK_ACCNT
		(KEEP=CUSTOMER_NBR ROUTING_NUM ACCOUNT_NUM DATE_UPDATED);
%RUNQUIT(&job,&sub4);

DATA WORK.QF4_CUSTINCOME_COPY;
	SET ECA.QF_CUSTOMER_INCOME_DETAILS
		(KEEP=CUSTOMER_NBR PAY_CYCLE_ID GROSS_INCOME INCOME_STATUS);
%RUNQUIT(&job,&sub4);

DATA WORK.EADV_CUSTOMERSTATUSCODE_COPY;
	SET EADV.CUSTOMERSTATUSCODE;
%RUNQUIT(&job,&sub4);

/*** LOCATION SPECIFIC INFO ***/
DATA WORK.EDW_D_LOC_COPY;		
	SET EDW.D_LOCATION
		(KEEP=LOC_NBR BUSN_UNIT_ID BRND_CD CTRY_CD ST_PVC_CD ADR_CITY_NM MAIL_CD HIER_ZONE_NBR HIER_ZONE_NM HIER_RGN_NBR HIER_RDO_NM HIER_DIV_NBR HIER_DDO_NM LOC_NM OPEN_DT CLS_DT);
%RUNQUIT(&job,&sub4);		

DATA WORK.SKYNET_LOCATION_LATLONG_COPY;
	SET SKYNET.LOCATION_LATLONG
		(KEEP=LOCNBR LATITUDE LONGITUDE);
%RUNQUIT(&job,&sub4);

DATA WORK.ECA_LOC_XREF_COPY;
	SET CADA.ECA_LOCATION_XREF;
%RUNQUIT(&job,&sub4);


/*
============================================================================= 
     CUSTOMER INFORMATION
=============================================================================
*/

/* KEEP DRIVERS LICENSE #S */
DATA WORK.QF4_CUST_SETUP;
	SET WORK.QF4_CUST_COPY;
	IF PHOTO_ID_TYPE = 'DL' THEN DO;
		DRIVERSLICST = PHOTO_ID_STATE;
		DRIVERSLICNBR = PHOTO_ID;
	DROP PHOTO_ID_TYPE PHOTO_ID_STATE PHOTO_ID;
	END;
%RUNQUIT(&job,&sub4);

/* REPLACE ECA LOC NBRS WITH AA LOC NBRS */
PROC SQL;
	CREATE TABLE WORK.QF4_CUST_FINAL AS 
	SELECT CUST.*
			,CASE WHEN XREF.LOCATION_AA IS MISSING THEN CUST.BRANCH_NBR
				ELSE XREF.LOCATION_AA
			END AS NEW_LOC
	FROM WORK.QF4_CUST_SETUP CUST
		LEFT JOIN WORK.ECA_LOC_XREF_COPY XREF ON CUST.BRANCH_NBR = XREF.BRANCH_ECA
	;
%RUNQUIT(&job,&sub4);


/*
============================================================================= 
     LOCATION INFORMATION
=============================================================================
*/

/* CONVERT BUSINESS_UNIT FROM NUMERIC TO CHAR IN D_LOC TABLE*/
/*DATA WORK.EDW_D_LOC_FINAL;*/
/*	SET WORK.EDW_D_LOC_COPY;*/
/*	BUSINESS_UNIT = COMPRESS(PUT(BUSN_UNIT_ID, 3.));*/
/*	DROP BUSN_UNIT_ID;*/
/*%RUNQUIT(&job,&sub4);*/
	

/*
============================================================================= 
     CUSTOMER BANK ACCOUNT INFORMATION
=============================================================================
*/

/* SORT BANK ACCOUNTS BY CUSTNBR & DATE_UPDATED */
PROC SORT DATA=WORK.QF4_CUSTBANKACCT_COPY;
	BY CUSTOMER_NBR DATE_UPDATED;
%RUNQUIT(&job,&sub4);

/* FLATTEN BANK ACCOUNTS - KEEP MOST RECENT UPDATED RECORD */
DATA WORK.QF4_CUSTBANKACCT_FINAL;
	SET WORK.QF4_CUSTBANKACCT_COPY;
	BY CUSTOMER_NBR;
	IF LAST.CUSTOMER_NBR;
%RUNQUIT(&job,&sub4);


/*
============================================================================= 
     CUSTOMER INCOME INFORMATION
=============================================================================
*/

/* CALCULATE GMI FOR EACH INCOME BY PAY FREQUENCY - INLUDE OTHER WITH MONTHLY - EXCLUDE INCOME STATUS ='I' */	
PROC SQL;
   CREATE TABLE WORK.QF4_CUSTINCOME_GMI AS 
   SELECT CUSTOMER_NBR,
		CASE WHEN PAY_CYCLE_ID = 'BI' THEN (GROSS_INCOME*26)/12
			 WHEN PAY_CYCLE_ID = 'BIM' THEN GROSS_INCOME*2
			 WHEN PAY_CYCLE_ID IN ('MON', 'OTH') THEN GROSS_INCOME
			 WHEN PAY_CYCLE_ID = 'WK' THEN (GROSS_INCOME*52)/12
			 END AS GMI_PARTIAL
   FROM WORK.QF4_CUSTINCOME_COPY
   WHERE INCOME_STATUS ^= 'I'
   ORDER BY	CUSTOMER_NBR;
%RUNQUIT(&job,&sub4);

/* AGGREGATE GMI FOR EACH CUSTOMER */
PROC SQL;
	CREATE TABLE WORK.QF4_CUSTINCOME_FINAL AS
	SELECT CUSTOMER_NBR
			,ROUND(SUM(GMI_PARTIAL),.01) AS GMI
	FROM WORK.QF4_CUSTINCOME_GMI
	GROUP BY CUSTOMER_NBR
	;
%RUNQUIT(&job,&sub4);


/*
============================================================================= 
     PUTTING IT ALL TOGETHER
=============================================================================
*/

PROC SQL;
	CREATE TABLE MARKETING_AQUISITION AS
	SELECT INSTANCE
		  ,SSN
		  ,CUSTNBR
		  ,(UPCASE(MARKETING_SOURCE))	AS MARKETING_SOURCE 
		  ,MARKETING_SOURCE_DATE
	FROM BIOR.CUSTOMER_AQUISITION
	WHERE INSTANCE = 'QFUND4'
	ORDER BY SSN
			,CUSTNBR
			,MARKETING_SOURCE_DATE
;
%RUNQUIT(&job,&sub1);

/*GET MOST RECENT MARKETING SOURCE DATE*/

DATA MOST_RECENT_AQU;
	SET MARKETING_AQUISITION;
	BY SSN
	   CUSTNBR
	   MARKETING_SOURCE_DATE;
	IF LAST.CUSTNBR THEN OUTPUT MOST_RECENT_AQU;
%RUNQUIT(&job,&sub1);


PROC SQL;
	CREATE TABLE CUSTOMER_DATAMART_QF4 AS
	SELECT	
		'QFUND' AS POS length=15 format=$15.
		,'QFUND4' AS INSTANCE length=15 format=$15.
		,'STOREFRONT'		AS CHANNELCD
		,LOC.BRND_CD AS BRANDCD
		,LOC.CTRY_CD AS COUNTRYCD
		,LOC.ST_PVC_CD AS STATE
		,LOC.ADR_CITY_NM AS CITY
		,LOC.MAIL_CD AS ZIP
		,LOC.HIER_ZONE_NBR AS ZONENBR
		,LOC.HIER_ZONE_NM AS ZONENAME 
		,LOC.HIER_RGN_NBR AS REGIONNBR
		,LOC.HIER_RDO_NM AS REGIONRDO 
		,LOC.HIER_DIV_NBR AS DIVISIONNBR
		,LOC.HIER_DDO_NM AS DIVISIONDDO 
		,CASE WHEN C.NEW_LOC > 10000
				THEN C.NEW_LOC/100
				ELSE C.NEW_LOC
			END AS LOCNBR format=16.
		,LOC.LOC_NM AS LOCATION_NAME
		,LOC.OPEN_DT AS LOC_OPEN_DT
		,LOC.CLS_DT AS LOC_CLOSE_DT
		,C.DATE_CREATED AS APPLICATIONDT
		,DHMS(TODAY()-1,0,0,0) as LAST_REPORT_DT format DATETIME20.
		,LATI.LATITUDE AS LATITUDE
		,LATI.LONGITUDE AS LONGITUDE
		,PUT(C.CUSTOMER_NBR, 20. -l) AS CUSTNBR
		,''			AS OMNINBR
/*		,C.CUSTOMER_NBR AS CUSTNBR*/
		,C.SSN
		,PUT(LOC.BUSN_UNIT_ID, 5. -l) AS BUSINESSUNITCD format=$5.
		,propcase(STATUS.DESCRIPTION) as STND_CUSTOMER_STATUS length= 35 format=$35.
		,propcase(C.FIRST_NAME) AS FIRSTNM length=35 format=$35.
		,propcase(C.LAST_NAME) AS LASTNM length=35 format=$35.
		,propcase(C.MIDDLE_NAME) AS MIDDLENM length=35 format=$35.
		,C.DRIVERSLICST length=2 format=$2.
		,C.DRIVERSLICNBR length=50 format=$50.
		,C.DOB
		,propcase(ADD.ADDRESS_LN) AS ADDRESS_LN1 length=60 format=$60.
		,propcase(ADD.ADDRESS_LN2) AS ADDRESS_LN2 length=60 format=$60.
		,propcase(ADD.PRIMARY_CITY) AS ADDRESS_CITY length=60 format=$60.
		,upcase(ADD.STATE_ID) AS ADDRESS_STATE length=2 format=$2.
		,ADD.POSTAL_ID AS ADDRESS_ZIP length=9 format=$9.
		,'USA' AS ADDRESS_COUNTRY format=$3.
		,C.HOME_PHONE AS HOME_PHONENBR length=10 format=$10.
		,C.MOBILE_PHONE AS MOBILE_PHONENBR length=10 format=$10.
		,C.WORK_PHONE AS WORK_PHONENBR length=10 format=$10.
		,'' AS OTHER_PHONENBR format=$10.
		,'' AS OTHER_PHONECD length=5 format=$5.
		,C.EMAIL AS EMAILADDRESS length=100 format=$100.
		,BANK.ROUTING_NUM AS BANK_ABANBR length=9 format=$9.
		,BANK.ACCOUNT_NUM AS BANK_ACCOUNTNBR length=20 format=$20.
		,INCOME.GMI
		,'' AS DATASHARECD length=35 format=$35.
		,'' AS DO_NOT_MAIL length=1 format=$1.
		,'' AS LANGUAGECD 			length=2 format=$2.
		,C.DATE_UPDATED AS UPDATEDT

	FROM WORK.QF4_CUST_FINAL C
		LEFT JOIN WORK.QF4_CUSTADDRESS_COPY ADD ON C.CUSTOMER_NBR = ADD.CUSTOMER_NBR
		LEFT JOIN WORK.QF4_CUSTBANKACCT_FINAL BANK ON C.CUSTOMER_NBR = BANK.CUSTOMER_NBR
		LEFT JOIN WORK.QF4_CUSTINCOME_FINAL INCOME ON C.CUSTOMER_NBR = INCOME.CUSTOMER_NBR
		LEFT JOIN WORK.EDW_D_LOC_COPY LOC ON C.NEW_LOC = LOC.LOC_NBR
		LEFT JOIN WORK.SKYNET_LOCATION_LATLONG_COPY LATI ON C.NEW_LOC = LATI.LOCNBR
		LEFT JOIN WORK.EADV_CUSTOMERSTATUSCODE_COPY STATUS ON C.CUSTOMER_STATUS = STATUS.CUSTSTATUSCD
		;
%RUNQUIT(&job,&sub4);

PROC SQL;
	CREATE TABLE CUSTDM.CUSTOMER_DATAMART_QF4 AS
	SELECT A.*
		  ,B.MARKETING_SOURCE
		  ,B.MARKETING_SOURCE_DATE
	FROM CUSTOMER_DATAMART_QF4 A
	LEFT JOIN MOST_RECENT_AQU B
		ON (A.INSTANCE = B.INSTANCE
		AND A.SSN = B.SSN
		AND A.CUSTNBR = B.CUSTNBR)
	;
%RUNQUIT(&job,&sub1);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&JOB,&SUB9);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
    CALL SYMPUTX('TIMESTAMP',CATX('_',PUT(TODAY(),CHECKTHEDAY.),PUT(TIME(),CHECKTHETIME.)),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\QFUND4",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\QFUND4\DIR2\",'G');
%RUNQUIT(&JOB,&SUB9);

PROC SQL;
    INSERT INTO SKY.CUSTOMER_DATAMART_QF4 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM CUSTDM.CUSTOMER_DATAMART_QF4
	WHERE STATE NOT IN ('TN');
%RUNQUIT(&JOB,&SUB9);


/*UPLOAD QF4*/
%INCLUDE "&CUST_FILE_PATH.\CUSTDM_UPLOAD_QF4.SAS";
