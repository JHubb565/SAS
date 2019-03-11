/***************************************************************************
Sub Program	: QF3 Input
Main		: Customer Datamart
Purpose		: Get all customer information from qfund3 POS system
Programmer  : Spencer Hopkins
****************************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
  12/16/2015	Spencer Hopkins		Caputure TXTITLE customers not in QFUND3    
  01/08/2016    Spencer Hopkins     Add email address field
  02/01/2016    Spencer Hopkins     Revise phone number logic
									 (old: primary, secondary)
									 (new: home, mobile, work, other)
  02/01/2016    Spencer Hopkins     POS_CUSTNBR = character & CUSTNBR = numeric
  02/19/2016    Spencer Hopkins     Add DATASHARECD & DO_NOT_MAIL columns
  04/12/2016    Spencer Hopkins		Optimization - only copy necessary columns
  05/13/2016	Spencer Hopkins		Remove Custnbr (numeric) & rename
									 pos_custnbr to custnbr (varchar)
  06/16/2016	Spencer Hopkins		Tweaked phone number algorithm
  07/19/2016	Spencer Hopkins		Added placeholder for languagecd
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

/*	INCLUDE LIBNAMES SCRIPT */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\TOP SECRET PROGRAM.SAS";


%LET DAYS_BACK = 5;



/*** QFUND3 (& EADV CUST STATUS TABLE) ***/
DATA WORK.QF3_CUST_COPY(KEEP=BO_ID BO_CODE BO_ST_CODE BUSINESS_UNIT FIRST_NAME LAST_NAME MIDDLE_NAME PHOTO_ID PHOTO_ID_NUM PHOTO_ID_STATE
							SSN DOB DATE_CREATED DATE_UPDATED BO_STATUS_ID ADD_LINE1 ADD_LINE2 CITY STATE ZIPCODE PRIMARY_PHONE_NUMBER PRIMARY_PHONE_TYPE SECONDARY_PHONE_NUMBER SECONDARY_PHONE_TYPE
							EMAIL_ID CUST_BANK_ABA_NUMBER CUST_BANK_ACNT_NUMBER DO_NOT_MAIL);
	SET QFUND3.CUSTOMER;
	WHERE (DATE_CREATED >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR DATE_UPDATED >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR ETL_DT >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR CREATE_DATE_TIME >= DHMS(TODAY()-&DAYS_BACK.,00,00,00));
%RUNQUIT(&job,&sub3);

DATA WORK.QF3_CUSTINCOME_COPY;
	SET QFUND3.CUSTOMER_INCOME
		(KEEP=BO_CODE SEQ_NUM BO_ID PAY_CYCLE_ID INCOME_AMOUNT);
%RUNQUIT(&job,&sub3);

DATA WORK.EADV_CUSTOMERSTATUSCODE_COPY;
	SET EADV.CUSTOMERSTATUSCODE;
%RUNQUIT(&job,&sub3);

/*** TXTITLE ***/
DATA WORK.TXTITLE_CUST_COPY (KEEP=CUSTNBR LOCNBR UPDATEDT FIRST_NAME LAST_NAME SSN APPL_DATE ADDRESS_LN APT_NBR CITY STATE ZIP PHONE_ID1 PHONE1 PHONE_ID2 PHONE2 CUSTOMER_STATUS);
	SET EDW.TITLE_CUSTOMER_DETAIL;
	WHERE (UPDATEDT >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR ETL_DT >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR LATEST_ACTIVITY_DATE >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR APPL_DATE >= DHMS(TODAY()-&DAYS_BACK.,00,00,00));
%RUNQUIT(&job,&sub3);

/*** LOCATION SPECIFIC INFO ***/
DATA WORK.EDW_D_LOC_COPY;		
	SET EDW.D_LOCATION
		(KEEP=LOC_NBR BRND_CD CTRY_CD ST_PVC_CD ADR_CITY_NM MAIL_CD HIER_ZONE_NBR HIER_ZONE_NM HIER_RGN_NBR HIER_RDO_NM HIER_DIV_NBR HIER_DDO_NM LOC_NM OPEN_DT CLS_DT);
%RUNQUIT(&job,&sub3);		

DATA WORK.SKYNET_LOCATION_LATLONG_COPY;
	SET SKYNET.LOCATION_LATLONG
		(KEEP=LOCNBR LATITUDE LONGITUDE);
%RUNQUIT(&job,&sub3);


/*
============================================================================= 
     CUSTOMER INFORMATION (NON-INCOME)
=============================================================================
*/

PROC SORT DATA=WORK.QF3_CUST_COPY OUT=WORK.QF3_CUST_SORTED;
	BY BO_CODE BO_ID;
%RUNQUIT(&job,&sub3);

/* GET MOST RECENT CUSTOMER INFORMATION (FLATTEN) */
DATA WORK.QF3_CUST_FLATTEN (DROP=BO_ID);
	SET WORK.QF3_CUST_SORTED;
	BY BO_CODE BO_ID;
	IF LAST.BO_CODE;
%RUNQUIT(&job,&sub3);

/* ONLY CAPTURE DRIVER'S LICENSE #S & CONVERT BUSINESS UNIT FIELD FROM NUMERIC TO CHARACTER */
DATA WORK.QF3_CUST_FINAL (DROP=PHOTO_ID PHOTO_ID_NUM PHOTO_ID_STATE);
	SET WORK.QF3_CUST_FLATTEN;
	IF PHOTO_ID = 'DL' THEN DO;
		DRIVERSLICST = PHOTO_ID_STATE;
		DRIVERSLICNBR = PHOTO_ID_NUM;
	END;
	CHAR_BU = PUT(BUSINESS_UNIT,3.);
	DROP BUSINESS_UNIT;
	RENAME CHAR_BU = BUSINESS_UNIT;
%RUNQUIT(&job,&sub3);

/*
============================================================================= 
     RE-ARRANGE CUSTOMER PHONE INFO (HOME, MOBILE, WORK, OTHER, OTHER PHONE CD)
=============================================================================
*/

DATA QF3_CUSTPHONE_FINAL;
	SET WORK.QF3_CUST_FINAL;

	IF PRIMARY_PHONE_TYPE = 'H' THEN HOME_PHONENBR = PRIMARY_PHONE_NUMBER;
	ELSE IF PRIMARY_PHONE_TYPE = 'C' THEN MOBILE_PHONENBR = PRIMARY_PHONE_NUMBER;
	ELSE IF PRIMARY_PHONE_TYPE = 'W' THEN WORK_PHONENBR = PRIMARY_PHONE_NUMBER;
	ELSE DO;
		OTHER_PHONENBR = PRIMARY_PHONE_NUMBER;
		OTHER_PHONECD = PRIMARY_PHONE_TYPE;
	END;

	IF SECONDARY_PHONE_TYPE = 'H' AND PRIMARY_PHONE_TYPE = 'H' THEN DO;
		OTHER_PHONENBR = SECONDARY_PHONE_NUMBER;
		OTHER_PHONECD = SECONDARY_PHONE_TYPE;
	END;
	ELSE IF SECONDARY_PHONE_TYPE = 'H' THEN HOME_PHONENBR = SECONDARY_PHONE_NUMBER;
	ELSE IF SECONDARY_PHONE_TYPE = 'C' AND PRIMARY_PHONE_TYPE = 'C' THEN DO;
		OTHER_PHONENBR = SECONDARY_PHONE_NUMBER;
		OTHER_PHONECD = SECONDARY_PHONE_TYPE;
	END;
	ELSE IF SECONDARY_PHONE_TYPE = 'C' THEN MOBILE_PHONENBR = SECONDARY_PHONE_NUMBER;
	ELSE IF SECONDARY_PHONE_TYPE = 'W' AND PRIMARY_PHONE_TYPE = 'W' THEN DO;
		OTHER_PHONENBR = SECONDARY_PHONE_NUMBER;
		OTHER_PHONECD = SECONDARY_PHONE_TYPE;
	END;
	ELSE IF SECONDARY_PHONE_TYPE = 'W' THEN WORK_PHONENBR = SECONDARY_PHONE_NUMBER;
	ELSE IF PRIMARY_PHONE_TYPE IN ('H','W','C') AND SECONDARY_PHONE_TYPE NOT IN ('H','W','C') THEN DO;
		OTHER_PHONENBR = SECONDARY_PHONE_NUMBER;
		OTHER_PHONECD = SECONDARY_PHONE_TYPE;
	END;

	KEEP BO_CODE HOME_PHONENBR MOBILE_PHONENBR WORK_PHONENBR OTHER_PHONENBR OTHER_PHONECD;
%RUNQUIT(&job,&sub3);


/*
============================================================================= 
     CUSTOMER INCOME INFORMATION
=============================================================================
*/

/* SORT CUSTOMER INCOME INFO BY BO_CODE, THEN BY SEQ_NUM AND LASTLY BY BO_ID */
PROC SORT DATA=WORK.QF3_CUSTINCOME_COPY;
	BY BO_CODE SEQ_NUM BO_ID;
%RUNQUIT(&job,&sub3);

/* FLATTEN CUSTOMER INCOME INFO BY MOST RECENT UPDATED SEQ_NUM BY CUSTOMER */
DATA WORK.QF3_CUSTINCOME_FLATTEN;
	SET WORK.QF3_CUSTINCOME_COPY;
	BY BO_CODE SEQ_NUM;
	IF LAST.SEQ_NUM;
%RUNQUIT(&job,&sub3);

/* CALCULATE GMI FOR EACH INCOME BY PAY FREQUENCY - INLUDE OTHER WITH MONTHLY */		
PROC SQL;		
	CREATE TABLE WORK.QF3_CUSTINCOME_GMI AS	
	SELECT BO_CODE,	
		CASE WHEN PAY_CYCLE_ID = 'BI' THEN (INCOME_AMOUNT*26)/12
			 WHEN PAY_CYCLE_ID = 'BIM' THEN INCOME_AMOUNT*2
			 WHEN PAY_CYCLE_ID IN ('MON', 'OTH') THEN INCOME_AMOUNT
			 WHEN PAY_CYCLE_ID = 'WK' THEN (INCOME_AMOUNT*52)/12
			 END AS GMI_PARTIAL
	FROM WORK.QF3_CUSTINCOME_FLATTEN
	ORDER BY BO_CODE;	
%RUNQUIT(&job,&sub3);	


/* AGGREGATE GMI FOR EACH CUSTOMER */
PROC SQL;
	CREATE TABLE WORK.QF3_CUSTINCOME_FINAL AS
	SELECT BO_CODE
			,ROUND(SUM(GMI_PARTIAL),.01) AS GMI
	FROM WORK.QF3_CUSTINCOME_GMI
	GROUP BY BO_CODE
	;
%RUNQUIT(&job,&sub3);

/*
============================================================================= 
     CREATE FINAL TABLE FOR QFUND3
=============================================================================
*/

PROC SQL;
	CREATE TABLE WORK.FINAL_QF3 AS
	SELECT	
		'QFUND' AS POS length=15 format=$15.
		,'QFUND3' AS INSTANCE length=15 format=$15.
		,'STOREFRONT'				AS CHANNELCD
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
		,C.BO_ST_CODE AS LOCNBR format=16.
		,LOC.LOC_NM AS LOCATION_NAME
		,LOC.OPEN_DT AS LOC_OPEN_DT
		,LOC.CLS_DT AS LOC_CLOSE_DT
		,C.DATE_CREATED AS APPLICATIONDT
		,DHMS(TODAY()-1,0,0,0) as LAST_REPORT_DT format DATETIME20.
		,LATI.LATITUDE AS LATITUDE
		,LATI.LONGITUDE AS LONGITUDE
		,PUT(C.BO_CODE, 20. -l) AS CUSTNBR
		,''						AS OMNINBR
/*		,C.BO_CODE AS CUSTNBR*/
		,C.SSN
		,C.BUSINESS_UNIT AS BUSINESSUNITCD length=5 format=$5.
		,propcase(STATUS.DESCRIPTION) as STND_CUSTOMER_STATUS length= 35 format=$35.
		,propcase(C.FIRST_NAME) AS FIRSTNM length=35 format=$35.
		,propcase(C.LAST_NAME) AS LASTNM length=35 format=$35.
		,propcase(C.MIDDLE_NAME) AS MIDDLENM length=35 format=$35.
		,C.DRIVERSLICST length=2 format=$2.
		,C.DRIVERSLICNBR length=50 format=$50.
		,C.DOB
		,propcase(C.ADD_LINE1) AS ADDRESS_LN1 length=60 format=$60.
		,propcase(C.ADD_LINE2) AS ADDRESS_LN2 length=60 format=$60.
		,propcase(C.CITY) AS ADDRESS_CITY length=60 format=$60.
		,upcase(C.STATE) AS ADDRESS_STATE length=2 format=$2.
		,C.ZIPCODE AS ADDRESS_ZIP length=9 format=$9.
		,'USA' AS ADDRESS_COUNTRY length=3 format=$3.
		,PHONE.HOME_PHONENBR		length=10 format=$10.
		,PHONE.MOBILE_PHONENBR		length=10 format=$10.
		,PHONE.WORK_PHONENBR		length=10 format=$10.
		,PHONE.OTHER_PHONENBR		length=10 format=$10.
		,PHONE.OTHER_PHONECD		length=5 format=$5.
		,C.EMAIL_ID AS EMAILADDRESS length=100 format=$100.
		,C.CUST_BANK_ABA_NUMBER AS BANK_ABANBR length=9 format=$9.
		,C.CUST_BANK_ACNT_NUMBER AS BANK_ACCOUNTNBR length=20 format=$20.
		,INCOME.GMI
		,'' AS DATASHARECD length=35 format=$35.
		,C.DO_NOT_MAIL length=1 format=$1.
		,'' AS LANGUAGECD 			length=2 format=$2.
		,CASE WHEN C.DATE_UPDATED = . THEN C.DATE_CREATED
				ELSE C.DATE_UPDATED
			END AS UPDATEDT FORMAT DATETIME20.
	FROM WORK.QF3_CUST_FINAL C
		LEFT JOIN WORK.QF3_CUSTINCOME_FINAL INCOME ON C.BO_CODE = INCOME.BO_CODE
		LEFT JOIN WORK.QF3_CUSTPHONE_FINAL PHONE ON C.BO_CODE = PHONE.BO_CODE
		LEFT JOIN WORK.EDW_D_LOC_COPY LOC ON C.BO_ST_CODE = LOC.LOC_NBR
		LEFT JOIN WORK.SKYNET_LOCATION_LATLONG_COPY LATI ON C.BO_ST_CODE = LATI.LOCNBR
		LEFT JOIN WORK.EADV_CUSTOMERSTATUSCODE_COPY STATUS ON C.BO_STATUS_ID = STATUS.CUSTSTATUSCD
	;
%RUNQUIT(&job,&sub3);


/*
============================================================================= 
     PULL TXTITLE CUSTOMERS NOT IN QFUND3.CUSTOMER
=============================================================================
*/

/* GET MOST RECENT CUSTOMER INFORMATION (FLATTEN) */
PROC SORT DATA=WORK.TXTITLE_CUST_COPY;
    BY CUSTNBR UPDATEDT;
%RUNQUIT(&job,&sub3);

DATA WORK.TXTITLE_CUST_FLAT;
    SET WORK.TXTITLE_CUST_COPY;
    BY CUSTNBR UPDATEDT;
    IF LAST.CUSTNBR;
%RUNQUIT(&job,&sub3);

/* PULL CUSTOMERS FROM TXTITLE THAT ARE NOT IN QFUND3 */
DATA WORK.TXTITLE_CUST_FINAL;
	MERGE WORK.QF3_CUST_FINAL (KEEP=BO_CODE IN=QF3 RENAME=(BO_CODE=CUSTNBR))
			WORK.TXTITLE_CUST_FLAT (IN=TXT);
	BY CUSTNBR;
	IF QF3=0 AND TXT=1;
%RUNQUIT(&job,&sub3);


/*
============================================================================= 
     RE-ARRANGE CUSTOMER PHONE INFO (HOME, MOBILE, WORK, OTHER, OTHER PHONE CD)
=============================================================================
*/

DATA TXTITLE_CUSTPHONE_FINAL;
	SET WORK.TXTITLE_CUST_FINAL;

	IF PHONE_ID1 = 'H' THEN HOME_PHONENBR = PHONE1;
	ELSE IF PHONE_ID1 = 'C' THEN MOBILE_PHONENBR = PHONE1;
	ELSE IF PHONE_ID1 = 'W' THEN WORK_PHONENBR = PHONE1;
	ELSE DO;
		OTHER_PHONENBR = PHONE1;
		OTHER_PHONECD = PHONE_ID1;
	END;

	IF PHONE_ID2 = 'H' AND PHONE_ID1 = 'H' THEN DO;
		OTHER_PHONENBR = PHONE2;
		OTHER_PHONECD = PHONE_ID2;
	END;
	
	ELSE IF PHONE_ID2 = 'C' AND PHONE_ID1 = 'C' THEN DO;
		OTHER_PHONENBR = PHONE2;
		OTHER_PHONECD = PHONE_ID2;
	END;
	ELSE IF PHONE_ID2 = 'W' AND PHONE_ID1 = 'W' THEN DO;
		OTHER_PHONENBR = PHONE2;
		OTHER_PHONECD = PHONE_ID2;
	END;
	ELSE IF PHONE_ID1 IN ('H','W','C') AND PHONE_ID2 NOT IN ('H','W','C') THEN DO;
		OTHER_PHONENBR = PHONE2;
		OTHER_PHONECD = PHONE_ID2;
	END;

	KEEP CUSTNBR HOME_PHONENBR MOBILE_PHONENBR WORK_PHONENBR OTHER_PHONENBR OTHER_PHONECD;
%RUNQUIT(&job,&sub3);




/* CREATE FINAL TXTITLE TABLE TO COMBINE WITH QFUND3 TABLE */
PROC SQL;
	CREATE TABLE WORK.FINAL_TXTITLE AS
	SELECT	
		'QFUND' AS POS length=15 format=$15.
		,'QFUND3' AS INSTANCE length=15 format=$15.
		,'STOREFRONT'			AS CHANNELCD
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
		,C.LOCNBR AS LOCNBR format=16.
		,LOC.LOC_NM AS LOCATION_NAME
		,LOC.OPEN_DT AS LOC_OPEN_DT
		,LOC.CLS_DT AS LOC_CLOSE_DT
		,C.APPL_DATE AS APPLICATIONDT
		,DHMS(TODAY()-1,0,0,0) as LAST_REPORT_DT format=DATETIME20.
		,LATI.LATITUDE AS LATITUDE
		,LATI.LONGITUDE AS LONGITUDE
		,PUT(C.CUSTNBR, 20. -l) AS CUSTNBR
		,''	AS OMNINBR
/*		,C.CUSTNBR AS CUSTNBR*/
		,C.SSN length=9 format=$9.
		,'' AS BUSINESSUNITCD length=5 format=$5.
		,propcase(STATUS.DESCRIPTION) as STND_CUSTOMER_STATUS length=35 format=$35.
		,propcase(C.FIRST_NAME) AS FIRSTNM length=35 format=$35.
		,propcase(C.LAST_NAME) AS LASTNM length=35 format=$35.
		,'' AS MIDDLENM length=35 format=$35.
		,'' AS DRIVERSLICST length=2 format=$2.
		,'' AS DRIVERSLICNBR length=50 format=$50.
		,. AS DOB format=DATETIME20.
		,propcase(C.ADDRESS_LN) AS ADDRESS_LN1 length=60 format=$60.
		,propcase(C.APT_NBR) AS ADDRESS_LN2 length=60 format=$60.
		,propcase(C.CITY) AS ADDRESS_CITY length=60 format=$60.
		,upcase(C.STATE) AS ADDRESS_STATE length=2 format=$2.
		,C.ZIP AS ADDRESS_ZIP length=9 format=$9.
		,'USA' AS ADDRESS_COUNTRY length=3 format=$3.
		,PHONE.HOME_PHONENBR		length=10 format=$10.
		,PHONE.MOBILE_PHONENBR		length=10 format=$10.
		,PHONE.WORK_PHONENBR		length=10 format=$10.
		,PHONE.OTHER_PHONENBR		length=10 format=$10.
		,PHONE.OTHER_PHONECD		length=5 format=$5.
		,'' AS EMAILADDRESS length=100 format=$100.
		,'' AS BANK_ABANBR length=9 format=$9.
		,'' AS BANK_ACCOUNTNBR length=20 format=$20.
		,. AS GMI format=20.
		,'' AS DATASHARECD length=35 format=$35.
		,'' AS DO_NOT_MAIL length=1 format=$1.
		,'' AS LANGUAGECD 			length=2 format=$2.
		,C.UPDATEDT
	FROM WORK.TXTITLE_CUST_FINAL C
		LEFT JOIN WORK.TXTITLE_CUSTPHONE_FINAL PHONE ON C.CUSTNBR = PHONE.CUSTNBR
		LEFT JOIN WORK.EDW_D_LOC_COPY LOC ON C.LOCNBR = LOC.LOC_NBR
		LEFT JOIN WORK.SKYNET_LOCATION_LATLONG_COPY LATI ON C.LOCNBR = LATI.LOCNBR
		LEFT JOIN WORK.EADV_CUSTOMERSTATUSCODE_COPY STATUS ON C.CUSTOMER_STATUS = STATUS.CUSTSTATUSCD
	;
%RUNQUIT(&job,&sub3);



/*
============================================================================= 
     COMBINE QFUND3 CUSTOMERS & TXTITLE CUSTOMERS
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
	WHERE INSTANCE = 'QFUND3'
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


DATA CUSTOMER_DATAMART_QF3;
	SET WORK.FINAL_QF3
		WORK.FINAL_TXTITLE;
%RUNQUIT(&job,&sub3);

PROC SQL;
	CREATE TABLE CUSTDM.CUSTOMER_DATAMART_QF3 AS
	SELECT A.*
		  ,B.MARKETING_SOURCE
		  ,B.MARKETING_SOURCE_DATE
	FROM CUSTOMER_DATAMART_QF3 A
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
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\QFUND3",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\QFUND3\DIR2\",'G');
%RUNQUIT(&JOB,&SUB9);

PROC SQL;
    INSERT INTO SKY.CUSTOMER_DATAMART_QF3 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM CUSTDM.CUSTOMER_DATAMART_QF3;
%RUNQUIT(&JOB,&SUB9);

/*UPLOAD QF3*/
%INCLUDE "&CUST_FILE_PATH.\CUSTDM_UPLOAD_QF3.SAS";

