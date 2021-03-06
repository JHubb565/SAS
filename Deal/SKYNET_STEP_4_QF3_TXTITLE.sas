%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub4);

LIBNAME TXTITLE ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH = EDWPRD
	SCHEMA=EDW DEFER=YES;

LIBNAME EDW ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH = EDWPRD
	SCHEMA=EDW DEFER=YES;

LIBNAME BIOR ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH = BIOR
	SCHEMA=BIOR DEFER=YES;

LIBNAME QFUND3 ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH=EDWPRD
	SCHEMA=QFUND3 DEFER=YES;

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM\";

DATA _NULL_;
    LENGTH FULL_RUN $ 1;
    FORMAT FULL_RUN $CHAR1.;
    INFORMAT FULL_RUN $CHAR1.;
    INFILE 'E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\SKYNET\WEEKLY INSERT DEV CODE\FULL_RUN.CSV'
        LRECL=1
        ENCODING="WLATIN1"
        TERMSTR=CRLF
        DLM='7F'x
        MISSOVER
        DSD ;
    INPUT FULL_RUN : $CHAR1.;
	IF _N_ ^= 1;
	IF FULL_RUN = 'Y' THEN 
		DO;
			CALL SYMPUTX('LASTWEEK',DHMS(INTNX('YEAR',TODAY(),-6,'B'),00,00,00),'G');
		END;
	ELSE IF FULL_RUN = 'N' THEN 
		DO;
		 	CALL SYMPUTX('LASTWEEK',DHMS(INTNX('DAY',TODAY(),-5,'B'),00,00,00),'G');
		END;
%RUNQUIT(&job,&sub4);

PROC SQL;
   CREATE TABLE WORK.TXTITLE_BASE_METRICS AS 
   SELECT /* PRODUCT */
            ("TITLE") LABEL="PRODUCT" AS PRODUCT, 
          /* POS */
            ("QFUND") LABEL="POS" AS POS, 
          /* INSTANCE */
            ("QFUND3") LABEL="INSTANCE" AS INSTANCE, 
          /* BANKMODEL */
            ("CSO") LABEL="BANKMODEL" AS BANKMODEL, 
          t2.BRND_CD AS BRANDCD, 
          t2.CTRY_CD AS COUNTRYCD, 
          t2.ST_PVC_CD AS STATE, 
          t2.ADR_CITY_NM AS CITY, 
          t2.MAIL_CD AS ZIP, 
          t2.HIER_ZONE_NBR AS ZONENBR, 
          t2.HIER_ZONE_NM AS ZONENAME, 
          t2.HIER_RGN_NBR AS REGIONNBR, 
          t2.HIER_RDO_NM AS REGIONRDO, 
          t2.HIER_DIV_NBR AS DIVISIONNBR, 
          t2.HIER_DDO_NM AS DIVISIONDDO, 
          t2.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t1.LOCNBR, 
          t2.OPEN_DT AS LOC_OPEN_DATE, 
          t2.CLS_DT AS LOC_CLOSE_DATE, 
          /* BEGINDT */
            (INTNX('MONTH',TODAY(),-60,'BEGINNING')) FORMAT=MMDDYY10. LABEL="BEGINDT" AS BEGINDT, 
          t1.LOAN_DATE AS DEAL_DTTM, 
          /* DEAL_DT */
            (DATEPART(t1.LOAN_DATE)) FORMAT=MMDDYY10. LABEL="DEAL_DT" AS DEAL_DT, 
          t1.LOAN_CODE AS DEALNBR, 
          t1.ADV_AMT AS ADVAMT, 
          t1.CUSTNBR, 
          t1.FEE_AMT AS FEEAMT, 
          /* INTERESTFEE */
            (.) AS INTERESTFEE, 
          /* NSFFEEAMT */
            (.) AS NSFFEEAMT, 
          t1.LATE_FEE_AMT AS LATEFEEAMT, 
          /* OTHERFEEAMT */
            (.) AS OTHERFEEAMT, 
          /* WAIVEDFEEAMT */
            (.) AS WAIVEDFEEAMT, 
          t1.REBATE_AMT AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t1.TOTAL_OWED AS TOTALOWED, 
          t1.TOTAL_PAID AS TOTALPAID, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* CASHAGAINCNT */
            (.) AS CASHAGAINCNT, 
          t1.DUE_DATE AS DUEDT, 
          t1.DEAL_END_DATE AS DEALENDDT, 
          /* DEPOSITDT */
            (""DT) FORMAT=DATETIME20. AS DEPOSITDT, 
          t1.WRITEOFF_DATE AS WRITEOFFDT, 
          /* DEFAULTDT */
            (""DT) FORMAT=DATETIME20. AS DEFAULTDT, 
          /* ACHSTATUS */
            ("") AS ACHSTATUS, 
          /* CHECKSTATUS */
            ("") AS CHECKSTATUS, 
          t1.LOAN_STATUS AS DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            ('TITLE') AS COLLATERAL_TYPE, 
          t1.ETL_DT AS ETLDT, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          t1.PRODUCT_TYPE AS PRODUCTCD, 
          t1.UPDATEDTE AS UPDATEDT, 
          /* ACHAUTHFLG */
            ("") AS ACHAUTHFLG, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'BEGINNING')) FORMAT=MMDDYY10. LABEL="ENDDT" AS ENDDT
      FROM TXTITLE.TITLE_LOAN_SUMMARY t1
           LEFT JOIN EDW.D_LOCATION t2 ON (t1.LOCNBR = t2.LOC_NBR)
      WHERE t2.ST_PVC_CD NOT IS MISSING AND t1.PRODUCT_TYPE = 'TLP' AND t1.UPDATEDTE >= &lastweek
      ORDER BY t1.CUSTNBR,
               t1.LOAN_CODE,
               t1.UPDATEDTE;
%RUNQUIT(&job,&sub4);

DATA MOST_RECENT_DEAL;
	SET WORK.TXTITLE_BASE_METRICS;
	BY CUSTNBR DEALNBR UPDATEDT;
 	IF LAST.DEALNBR;
%RUNQUIT(&job,&sub4);

PROC SORT DATA=TXTITLE.TITLE_CUSTOMER_DETAIL OUT=SORTED_CUSTS;
	BY CUSTNBR UPDATEDT;
%RUNQUIT(&job,&sub4);

DATA MOST_RECENT_CUST_PRE;
	SET WORK.SORTED_CUSTS;
	BY CUSTNBR UPDATEDT;
	IF LAST.CUSTNBR;
%RUNQUIT(&job,&sub4);

PROC SORT DATA=QFUND3.CUSTOMER OUT=SORTED_CUSTS_TXTITLE;
	BY BO_CODE BO_ID;
%RUNQUIT(&job,&sub4);

DATA MOST_RECENT_CUST_TXTITLE;
	SET WORK.SORTED_CUSTS_TXTITLE;
	BY BO_CODE BO_ID;
	IF LAST.BO_CODE;
	CUSTNBR = BO_CODE;
	KEEP CUSTNBR SSN;
%RUNQUIT(&job,&sub4);

PROC SQL;
CREATE TABLE WORK.COMB_CUSTS AS 
SELECT * FROM WORK.MOST_RECENT_CUST_TXTITLE
 OUTER UNION CORR 
SELECT * FROM WORK.MOST_RECENT_CUST_PRE;
%RUNQUIT(&job,&sub4);

PROC SORT DATA=WORK.COMB_CUSTS OUT=MOST_RECENT_CUST NODUPKEY;
BY CUSTNBR;
%RUNQUIT(&job,&sub4);

PROC SQL;
   CREATE TABLE WORK.DEF_TXNS AS 
   SELECT t1.LOCNBR, 
          t1.CUSTNBR, 
          t1.LOAN_CODE, 
          t1.TRANDTE, 
          t1.PRODUCT_ID, 
          t1.LOAN_TRAN_CODE, 
          t1.LOAN_DUE_DATE, 
          t1.LOAN_STATUS, 
          t1.TRAN_ID, 
          t1.VOID_ID, 
          t1.PRINCIPAL, 
          t1.CSO_FEE, 
          t1.LATE_FEE, 
          t1.WRITEOFF_AMT, 
          t1.WRITEOFF_FEE, 
          t1.ETL_DT, 
          t1.ETL_CREATE_DATE_TIME, 
          t1.ETL_UPDATE_DATE_TIME, 
          t1.ETL_CREATE_USER_NM, 
          t1.ETL_UPDATE_USER_NM, 
          t1.ETL_CREATE_PROGRAM_NM, 
          t1.ETL_UPDATE_PROGRAM_NM, 
          t1.PRODUCT_TYPE
      FROM TXTITLE.TITLE_TRANS_SUMMARY t1
      WHERE t1.TRAN_ID = 'DEF'
      ORDER BY t1.LOAN_CODE;
%RUNQUIT(&job,&sub4);

DATA FIRST_DEF;
	SET WORK.DEF_TXNS;
	BY LOAN_CODE;
	IF FIRST.LOAN_CODE;
%RUNQUIT(&job,&sub4);

PROC SQL;
   CREATE TABLE QF3_TXTITLE_DAILY_UPDATE AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BANKMODEL, 
          t1.BRANDCD, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.BUSINESS_UNIT, 
          t1.LOCNBR, 
          t1.LOC_OPEN_DATE, 
          t1.LOC_CLOSE_DATE, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.DEAL_DT, 
          t1.CUSTNBR, 
          t2.SSN, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.INTERESTFEE, 
          t1.NSFFEEAMT, 
          t1.LATEFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.WAIVEDFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.TOTALOWED, 
          t1.TOTALPAID, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGAINCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t3.TRANDTE AS DEFAULTDT, 
          t1.ACHSTATUS, 
          t1.CHECKSTATUS, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT
      FROM WORK.MOST_RECENT_DEAL t1
           LEFT JOIN WORK.MOST_RECENT_CUST t2 ON (t1.CUSTNBR = t2.CUSTNBR)
           LEFT JOIN WORK.FIRST_DEF t3 ON (t1.DEALNBR = t3.LOAN_CODE)
      ORDER BY t1.CUSTNBR,
               t1.DEALNBR,
               t1.DEAL_DTTM;
%RUNQUIT(&job,&sub4);

PROC SQL;
   CREATE TABLE WORK.DEAL_SUMMARY_TMP AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.BANKMODEL, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.BUSINESS_UNIT, 
          t1.LOCNBR, 
          t1.LOC_OPEN_DATE AS LOC_OPEN_DT, 
          t1.LOC_CLOSE_DATE AS LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR,
		  .						AS TITLE_DEALNBR,
          t1.CUSTNBR, 
          t1.SSN, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.NSFFEEAMT, 
          t1.LATEFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.WAIVEDFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGAINCNT AS CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT LABEL='', 
          t1.ACHSTATUS AS ACHSTATUSCD, 
          t1.CHECKSTATUS AS CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT,
		  . AS OUTSTANDING_DRAW_AMT,
		  '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM QF3_TXTITLE_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub4);

PROC SQL;
   CREATE TABLE WORK.DEAL_SUM_DAILY_UPDATE_PRE AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE,
		  'STOREFRONT'					AS CHANNELCD,  
          t1.BRANDCD, 
          t1.BANKMODEL, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          /* BUSINESS_UNIT */
            (compress(put(t1.BUSINESS_UNIT,BEST9.))) AS BUSINESS_UNIT, 
          t1.LOCNBR, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          /* DEAL_DT */
            (dhms(t1.DEAL_DT,0,0,0)) FORMAT=datetime20. AS DEAL_DT, 
          t1.DEAL_DTTM, 
          /* LAST_REPORT_DT */
            (dhms(today()-1,0,0,0)) FORMAT=datetime20. LABEL="LAST_REPORT_DT" AS LAST_REPORT_DT, 
          /* DEALNBR */
            (COMPRESS(PUT(DEALNBR,30.))) AS DEALNBR, 
          /* TITLE_DEALNBR */
            (COMPRESS(PUT((CASE 
               WHEN . = t1.TITLE_DEALNBR THEN 0
               ELSE t1.TITLE_DEALNBR
            END),30.))) AS TITLE_DEALNBR, 
          /* CUSTNBR */
            (COMPRESS(PUT(CUSTNBR,30.))) AS CUSTNBR, 
          t1.SSN,
		  ''	AS OMNINBR,
          t1.ADVAMT, 
          t1.FEEAMT,
		  .			AS CUSTOMARYFEE, 
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.LATEFEEAMT, 
          t1.WAIVEDFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD,
		  '' AS RETURNREASONCD LENGTH=5 FORMAT=$5.,
          t1.DEALSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT,
		  T1.OUTSTANDING_DRAW_AMT,
		  t1.UNDER_COLLATERALIZED
      FROM WORK.DEAL_SUMMARY_TMP t1;
%RUNQUIT(&job,&sub4);

DATA UNION_TABLE;
SET TMP_TBLS.UNION_TABLE ;
RUN;

PROC SQL;
	CREATE TABLE WORK.DEAL_SUM_DAILY_UPDATE_PRE AS
	SELECT *
	FROM UNION_TABLE
	UNION ALL CORR
	SELECT *
	FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE
;
QUIT;

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub4);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND3",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND3\DIR2\",'G');
%RUNQUIT(&job,&sub4);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF3TXTITLE (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE;
%RUNQUIT(&job,&sub4);


/*UPLOAD QF3TXTITLE*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF3TXTITLE.SAS";
