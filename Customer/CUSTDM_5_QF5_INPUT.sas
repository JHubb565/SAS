/****************************************************************************
Sub Program	: QF5 Input
Main		: Customer Datamart
Purpose		: Get all customer information from qfund5 POS system
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
  04/12/2016    Spencer Hopkins		Optimization - only copy necessary columns
  05/13/2016	Spencer Hopkins		Remove Custnbr (numeric) & rename
									 pos_custnbr to custnbr (varchar)
  06/16/2016	Spencer Hopkins		Add NCP customers
  07/19/2016	Spencer Hopkins		Added placeholder for languagecd
  10/03/2016	Spencer Hopkins		Remove NCP customers
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


/*** QFUND5 ***/
DATA WORK.QF5_CUST_COPY(KEEP=BO_CODE BO_ID BO_ST_CODE BUSINESS_UNIT FIRST_NAME LAST_NAME MIDDLE_NAME PHOTO_ID_STATE PHOTO_ID_NUM
							SSN DOB DATE_CREATED BO_STATUS_ID ADD_LINE1 ADD_LINE2 CITY STATE ZIPCODE PRIMARY_PHONE_NUMBER PRIMARY_PHONE_TYPE
							SECONDARY_PHONE_NUMBER SECONDARY_PHONE_TYPE EMAIL_ID CUST_BANK_ABA_NUMBER CUST_BANK_ACNT_NUMBER DO_NOT_MAIL DATE_UPDATED);
	SET EDW.CSO_CUSTOMER;
	WHERE (DATE_UPDATED >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR DATE_CREATED >= DHMS(TODAY()-&DAYS_BACK.,00,00,00)
		  OR ETL_DT >= DHMS(TODAY()-&DAYS_BACK.,00,00,00));
%RUNQUIT(&job,&sub5);

DATA WORK.QF5_CUSTINCOME_COPY;
	SET EDW.CSO_CUSTOMER_INCOME
		(KEEP=BO_CODE SEQ_NUM BO_ID PAY_CYCLE_ID INCOME_AMOUNT);
%RUNQUIT(&job,&sub5);

DATA WORK.EADV_CUSTOMERSTATUSCODE_COPY;
	SET EADV.CUSTOMERSTATUSCODE;
%RUNQUIT(&job,&sub5);

/*** NCP ***/
/*DATA WORK.NCP_CUST_COPY;*/
/*	SET QFUND5.NCP_CUSTOMER;*/
/*%RUNQUIT(&job,&sub5);*/


/*** LOCATION SPECIFIC INFO ***/
DATA WORK.EDW_D_LOC_COPY;		
	SET EDW.D_LOCATION
		(KEEP=LOC_NBR BUSN_UNIT_ID BRND_CD CTRY_CD ST_PVC_CD ADR_CITY_NM MAIL_CD HIER_ZONE_NBR HIER_ZONE_NM HIER_RGN_NBR HIER_RDO_NM HIER_DIV_NBR HIER_DDO_NM LOC_NM OPEN_DT CLS_DT);
%RUNQUIT(&job,&sub5);		

DATA WORK.SKYNET_LOCATION_LATLONG_COPY;
	SET SKYNET.LOCATION_LATLONG
		(KEEP=LOCNBR LATITUDE LONGITUDE);
%RUNQUIT(&job,&sub5);


/*
============================================================================= 
     CUSTOMER INFORMATION (NON-INCOME)
=============================================================================
*/

/* SORT CUST INFO BY BO_CODE THEN BY BO_ID */
PROC SORT DATA=WORK.QF5_CUST_COPY;
	BY BO_CODE BO_ID;
%RUNQUIT(&job,&sub5);

/* FLATTEN CUSTOMER TO MOST UPDATED RECORD - FILTER OUT DL #S */
DATA WORK.CSO_CUST_FINAL;
	SET WORK.QF5_CUST_COPY;
	BY BO_CODE;
	IF LAST.BO_CODE;
	IF PHOTO_ID = 'DL' THEN DO;
		DRIVERSLICST = PHOTO_ID_STATE;
		DRIVERSLICNBR = PHOTO_ID_NUM;
	END;
	DROP PHOTO_ID_STATE PHOTO_ID_NUM;
%RUNQUIT(&job,&sub5);


/*
============================================================================= 
     RE-ARRANGE CUSTOMER PHONE INFO (HOME, MOBILE, WORK, OTHER, OTHER PHONE CD)
=============================================================================
*/

DATA QF5_CUSTPHONE_FINAL;
	SET WORK.CSO_CUST_FINAL;

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
%RUNQUIT(&job,&sub5);


/*
============================================================================= 
     CUSTOMER INCOME INFORMATION
=============================================================================
*/

/* SORT CUSTOMER INCOME INFO BY BO_CODE, THEN BY SEQ_NUM AND LASTLY BY BO_ID */
PROC SORT DATA=WORK.QF5_CUSTINCOME_COPY;
	BY BO_CODE SEQ_NUM BO_ID;
%RUNQUIT(&job,&sub5);

/* FLATTEN CUSTOMER INCOME INFO BY MOST RECENT UPDATED SEQ_NUM BY CUSTOMER */
DATA WORK.QF5_CUSTINCOME_FLATTEN;
	SET WORK.QF5_CUSTINCOME_COPY;
	BY BO_CODE SEQ_NUM;
	IF LAST.SEQ_NUM;
%RUNQUIT(&job,&sub5);

/* CALCULATE GMI FOR EACH INCOME BY PAY PREQUENCY - INLUDE '' WITH MONTHLY */		
PROC SQL;		
	CREATE TABLE WORK.QF5_CUSTINCOME_GMI AS	
	SELECT BO_CODE,	
		CASE WHEN PAY_CYCLE_ID = 'BI' THEN (INCOME_AMOUNT*26)/12
			 WHEN PAY_CYCLE_ID = 'BIM' THEN INCOME_AMOUNT*2
			 WHEN PAY_CYCLE_ID IN ('MON', '') THEN INCOME_AMOUNT
			 WHEN PAY_CYCLE_ID = 'WK' THEN (INCOME_AMOUNT*52)/12
			 END AS GMI_PARTIAL
	FROM WORK.QF5_CUSTINCOME_FLATTEN
	ORDER BY BO_CODE;	
%RUNQUIT(&job,&sub5);	


/* AGGREGATE GMI FOR EACH CUSTOMER */
PROC SQL;
	CREATE TABLE WORK.QF5_CUSTINCOME_FINAL AS
	SELECT BO_CODE
			,ROUND(SUM(GMI_PARTIAL),.01) AS GMI
	FROM WORK.QF5_CUSTINCOME_GMI
	GROUP BY BO_CODE
	;
%RUNQUIT(&job,&sub5);

/*
============================================================================= 
     PUTTING IT ALL TOGETHER (CSO)
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
	WHERE INSTANCE = 'QFUND5'
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
	CREATE TABLE CUSTOMER_DATAMART_QF5 AS
	SELECT	
		'QFUND' AS POS length=15 format=$15.
		,'QFUND5' AS INSTANCE length=15 format=$15.
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
		,C.BO_ST_CODE AS LOCNBR format=16.
		,LOC.LOC_NM AS LOCATION_NAME
		,LOC.OPEN_DT AS LOC_OPEN_DT
		,LOC.CLS_DT AS LOC_CLOSE_DT
		,C.DATE_CREATED AS APPLICATIONDT
		,DHMS(TODAY()-1,0,0,0) as LAST_REPORT_DT format DATETIME20.
		,LATI.LATITUDE AS LATITUDE
		,LATI.LONGITUDE AS LONGITUDE
		,PUT(C.BO_CODE, 20. -l) AS CUSTNBR
		,''		AS OMNINBR
/*		,C.BO_CODE AS CUSTNBR*/
		,C.SSN
		,C.BUSINESS_UNIT AS BUSINESSUNITCD
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
		,'USA' AS ADDRESS_COUNTRY format=$3.
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
		,DATE_UPDATED AS UPDATEDT

	FROM WORK.CSO_CUST_FINAL C
		LEFT JOIN WORK.QF5_CUSTINCOME_FINAL INCOME ON C.BO_CODE = INCOME.BO_CODE
		LEFT JOIN WORK.QF5_CUSTPHONE_FINAL PHONE ON C.BO_CODE = PHONE.BO_CODE
		LEFT JOIN WORK.EDW_D_LOC_COPY LOC ON C.BO_ST_CODE = LOC.LOC_NBR
		LEFT JOIN WORK.SKYNET_LOCATION_LATLONG_COPY LATI ON C.BO_ST_CODE = LATI.LOCNBR
		LEFT JOIN WORK.EADV_CUSTOMERSTATUSCODE_COPY STATUS ON C.BO_STATUS_ID = STATUS.CUSTSTATUSCD
		;
%RUNQUIT(&job,&sub5);

PROC SQL;
	CREATE TABLE CUSTDM.CUSTOMER_DATAMART_QF5 AS
	SELECT A.*
		  ,B.MARKETING_SOURCE
		  ,B.MARKETING_SOURCE_DATE
	FROM CUSTOMER_DATAMART_QF5 A
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
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\QFUND5",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\QFUND5\DIR2\",'G');
%RUNQUIT(&JOB,&SUB9);

PROC SQL;
    INSERT INTO SKY.CUSTOMER_DATAMART_QF5 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM CUSTDM.CUSTOMER_DATAMART_QF5;
%RUNQUIT(&JOB,&SUB9);


/*UPLOAD QF5*/
%INCLUDE "&CUST_FILE_PATH.\CUSTDM_UPLOAD_QF5.SAS";

