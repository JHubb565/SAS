/***************************************************************************
SUB PROGRAM	: ONLINE INPUT
MAIN		: CUSTOMER DATAMART
PURPOSE		: GET ALL CUSTOMER INFORMATION FROM AA.NET POS SYSTEM
PROGRAMMER  : SPENCER HOPKINS
****************************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
  06/03/2016	SPENCER HOPKINS		OUTPUT FINAL TABLE TO CUSTDM
  07/19/2016	SPENCER HOPKINS		ADDED PLACEHOLDER FOR LANGUAGECD
  04/25/2018	JUSTIN HUBBARD		AANET > AA_LG CHANGE

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



/* AA.NET */
 * LoanGuard;
/*LIBNAME AA_LG oledb datasource='RPTDB02.AEAONLINE.NET\AANET' provider=sqloledb dbmax_text=32767 */
/*               user='SVC_SASUser' password='{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16'*/
/*               properties=("initial catalog"=LGv4) schema=dbo;*/
 
/*******************************************/


/* ACCT TABLE */
DATA WORK.AANET_ACCT;
	SET AA_LG.ACCT;
%RUNQUIT(&job,&sub7);

/* ADDR TABLE */
DATA WORK.AANET_ADDR;
	SET AA_LG.ADDR;
%RUNQUIT(&job,&sub7);

/* BANK TABLE */
DATA WORK.AANET_BANK;
	SET AA_LG.BANK;
%RUNQUIT(&job,&sub7);

/* CUSTIDENTITY TABLE */
DATA WORK.AANET_CUSTIDENTITY;
	SET AA_LG.CUSTIDENTITY;
%RUNQUIT(&job,&sub7);

/* EMAIL TABLE */
DATA WORK.AANET_EMAIL;
	SET AA_LG.EMAIL;
%RUNQUIT(&job,&sub7);

/* PAY TABLE */
DATA WORK.AANET_PAY;
	SET AA_LG.PAY;
%RUNQUIT(&job,&sub7);

/* PHONE TABLE */
DATA WORK.AANET_PHONE;
	SET AA_LG.PHONE;
%RUNQUIT(&job,&sub7);

/* COMPANY TABLE */
DATA WORK.AA_BTAG_COMPANY;
	SET AA_BTAG.COMPANY;
%RUNQUIT(&job,&sub7)

PROC SQL;
	CREATE TABLE CUSTDM.CUSTOMER_DATAMART_OL AS
	SELECT 
		'ONLINE' AS POS length=15 format=$15.
		,'AANET' AS INSTANCE length=15 format=$15.
		,'ONLINE'	AS CHANNELCD
		,'AA' AS BRANDCD length=8 format=$8.
		,'USA' AS COUNTRYCD length=10 format=$10.
		,COMP.COMPANYCODE AS STATE length=2 format=$2.
		,'' AS CITY length=50 format=$50.
		,'' AS ZIP length=15 format=$15.
		,0 	AS ZONENBR length=8 format=16.
		,'ONLINE' AS ZONENAME length=100 format=$100.
		,0 	AS REGIONNBR length=8 format=16.
		,'ONLINE' AS REGIONRDO length=100 format=$100.
		,0	AS DIVISIONNBR length=8 format=16.
		,'ONLINE' AS DIVISIONDDO length=100 format=$100.
		,COMP.COMPANYID  AS LOCNBR length=8 format=16.
		,'ONLINE' AS LOCATION_NAME length=100 format=$100.
		,.  AS LOC_OPEN_DT format=DATETIME20.
		,.  AS LOC_CLOSE_DT format=DATETIME20.
		,ACCT.DATECREATED AS APPLICATIONDT format=DATETIME20.
		,DHMS(TODAY()-1,0,0,0) AS LAST_REPORT_DT format=DATETIME20.	
		,. AS LATITUDE length=8 format=BEST12.
		,. AS LONGITUDE length=8 format=BEST12.
		,ACCT.CUSTACCTNBR AS CUSTNBR length=20 format=$20.
		,''		AS OMNINBR
		,CUSTID.SSN length=9 format=$9.
		,'' AS BUSINESSUNITCD length=5 format=$5.
		,'' AS STND_CUSTOMER_STATUS length= 35 format=$35.
		,propcase(CUSTID.FIRSTNAME) AS FIRSTNM length=35 format=$35.
		,propcase(CUSTID.LASTNAME) AS LASTNM length=35 format=$35.
		,propcase(CUSTID.MIDDLENAME) AS MIDDLENM length=35 format=$35.
		,upcase(CUSTID.DLSTATEPROVINCEID) AS DRIVERSLICST length=2 format=$2.
		,upcase(CUSTID.DLNBR) AS DRIVERSLICNBR length=50 format=$50.
		,DHMS(INPUT(CUSTID.DOB,YYMMDD10.),00,00,00)			FORMAT = DATETIME20.			AS DOB/* THIS IS CHARACTER */		
		,propcase(ADDR.ADDR1) AS ADDRESS_LN1 length=60 format=$60.
		,(propcase(ADDR.ADDR2 || ADDR.ADDR3)) AS ADDRESS_LN2 length=60 format=$60.
		,propcase(ADDR.CITYTOWN) AS ADDRESS_CITY length=60 format=$60.
		,upcase(ADDR.STATEPROVINCEID) AS ADDRESS_STATE length=2 format=$2.
		,ADDR.POSTALCODE AS ADDRESS_ZIP length =9 format=$9.
		,'USA' AS ADDRESS_COUNTRY length=3 format=$3.
		,H_PHONE.PHONENBR AS HOME_PHONENBR 		length=10 format=$10.
		,M_PHONE.PHONENBR AS MOBILE_PHONENBR	length=10 format=$10.
		,W_PHONE.PHONENBR AS WORK_PHONENBR		length=10 format=$10.
		,'' AS OTHER_PHONENBR					length=10 format=$10.
		,'' AS OTHER_PHONECD					length=5 format=$5.
		,lowcase(EMAIL.EMAILADDR) AS EMAILADDRESS length=100 format=$100.
		,BANK.ABANBR AS BANK_ABANBR length=9 format=$9.
		,BANK.ACCTNBR AS BANK_ACCOUNTNBR length=20 format=$20.
		,PAY.GROSSPAYPERMONTH AS GMI 
		,'' AS DATASHARECD length=35 format=$35.
		,'' AS DO_NOT_MAIL length=1 format=$1.
		,'' AS LANGUAGECD 			length=2 format=$2.
		,ACCT.DATEMODIFIED AS UPDATEDT
		,''			AS MARKETING_SOURCE
		,''DT		FORMAT=DATETIME20. AS MARKETING_SOURCE_DATE
	FROM WORK.AANET_ACCT ACCT
		LEFT JOIN WORK.AANET_CUSTIDENTITY CUSTID ON ACCT.CUSTIDENTITYID = CUSTID.CUSTIDENTITYID
		LEFT JOIN WORK.AANET_ADDR ADDR ON ACCT.ADDRID = ADDR.ADDRID
		LEFT JOIN WORK.AANET_PHONE H_PHONE ON ACCT.HOMEPHONEID=H_PHONE.PHONEID
		LEFT JOIN WORK.AANET_PHONE M_PHONE ON ACCT.MOBILEPHONEID=M_PHONE.PHONEID
		LEFT JOIN WORK.AANET_PHONE W_PHONE ON ACCT.WORKPHONEID=W_PHONE.PHONEID
		LEFT JOIN WORK.AANET_EMAIL EMAIL ON ACCT.EMAILID=EMAIL.EMAILID
		LEFT JOIN WORK.AANET_BANK BANK ON ACCT.BANKID=BANK.BANKID
		LEFT JOIN WORK.AANET_PAY PAY ON ACCT.PAYID=PAY.PAYID
		LEFT JOIN WORK.AA_BTAG_COMPANY COMP ON ACCT.COMPANYID = COMP.COMPANYID
	;
%RUNQUIT(&job,&sub7);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&JOB,&SUB9);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
    CALL SYMPUTX('TIMESTAMP',CATX('_',PUT(TODAY(),CHECKTHEDAY.),PUT(TIME(),CHECKTHETIME.)),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\ONLINE",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\CUSTOMER\ONLINE\DIR2\",'G');
%RUNQUIT(&JOB,&SUB9);

PROC SQL;
    INSERT INTO SKY.CUSTOMER_DATAMART_OL (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM CUSTDM.CUSTOMER_DATAMART_OL;
%RUNQUIT(&JOB,&SUB9);


/*UPLOAD ONLINE*/
%INCLUDE "&CUST_FILE_PATH.\CUSTDM_UPLOAD_OL.SAS";


