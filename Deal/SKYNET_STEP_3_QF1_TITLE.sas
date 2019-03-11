%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub3);

LIBNAME QFUND1 ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH = EDWPRD
	SCHEMA=QFUND1
	DBSLICEPARM=(ALL,4) DEFER=YES;

LIBNAME EDW ORACLE 
	USER=&USER 
	PASSWORD=&PASSWORD
	PATH=EDWPRD 
	SCHEMA=EDW DEFER=YES;

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
		 	CALL SYMPUTX('LASTWEEK',DHMS(INTNX('DAY',TODAY(),-30,'B'),00,00,00),'G');
		END;
%RUNQUIT(&job,&sub3);


PROC SQL;
	CREATE TABLE WORK.SORTED_DEALS AS
		SELECT * 
		FROM QFUND1.LOAN_SUMMARY t1
		WHERE t1.DATE_UPDATED >= &LASTWEEK.
		ORDER BY LOAN_CODE, LOAN_ID, LOAN_DATE;
%RUNQUIT(&job,&sub3);

DATA MOST_RECENT_DEALS;
	SET WORK.SORTED_DEALS;
	BY LOAN_CODE;
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.MERGE_WITH_LOC_DATA AS 
   SELECT /* PRODUCT */
            (case
              when t1.PRODUCT_TYPE = 'TLP' then 'TITLE' else 'QFTITLENA' end) LABEL="PRODUCT" AS PRODUCT, 
          /* PRODUCT_DESC */
            ("") LABEL="PRODUCT_DESC" AS PRODUCT_DESC, 
          /* POS */
            ('QFUND') LABEL="POS" AS POS, 
          /* INSTANCE */
            ('QFUND1') LABEL="INSTANCE" AS INSTANCE, 
          t2.BRND_CD AS BRANDCD, 
          /* BANKMODEL */
            ('STANDARD') LABEL="BANKMODEL" AS BANKMODEL, 
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
          t1.ST_CODE AS LOCNBR, 
          t2.LOC_NM AS LOCATION_NAME, 
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          /* DEAL_DT */
            (DATEPART(T1.LOAN_DATE)) FORMAT=MMDDYY10. LABEL="DEAL_DT" AS DEAL_DT, 
          t1.LOAN_DATE AS DEAL_DTTM, 
          /* BEGINDT */
            (intnx('month',today(),-60,'BEGINNING')) FORMAT=mmddyy10. LABEL="begindt" AS BEGINDT, 
          t1.LOAN_CODE AS DEALNBR, 
          t1.LOAN_AMT AS ADVAMT, 
          /* FEEAMT */
            (.) LABEL="FEEAMT" AS FEEAMT, 
          t1.BO_CODE AS CUSTNBR, 
          t1.RTN_FEE_AMT AS NSFFEEAMT, 
          t1.LATE_FEE_AMT AS LATEFEEAMT, 
          /* OTHERFEEAMT */
            (sum(t1.REPOSSESSION_FEE,t1.DMV_FEE)) LABEL="OTHERFEEAMT" AS OTHERFEEAMT, 
          /* WAIVEDFEEAMT */
            (SUM(t1.WAIVED_RTN_FEE_AMT,t1.WAIVED_LATE_FEE_AMT)) LABEL="WAIVEDFEEAMT" AS WAIVEDFEEAMT, 
          /* REBATEAMT */
            (.) LABEL="REBATEAMT" AS REBATEAMT, 
          /* COUPONAMT */
            (.) LABEL="COUPONAMT" AS COUPONAMT, 
          t1.TOTAL_PAID AS TOTALPAID, 
          t1.TOTAL_DUE AS TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) LABEL="CONSECUTIVEDEALFLG" AS CONSECUTIVEDEALFLG, 
          /* REFINANCECNT */
            (.) AS REFINANCECNT, 
          /* CASHAGNCNT */
            (.) LABEL="CASHAGNCNT" AS CASHAGNCNT, 
          /* DEPOSITDT */
            (""d) FORMAT=MMDDYY10. LABEL="DEPOSITDT" AS DEPOSITDT, 
          t1.WO_DATE AS WRITEOFFDT, 
          t1.DEFAULT_DATE AS DEFAULTDT, 
          t1.LOAN_END_DATE AS DEALENDDT, 
          /* ACHSTATUSCD */
            ("") LABEL="ACHSTATUSCD" AS ACHSTATUSCD, 
          t1.LOAN_STATUS_ID AS DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            (CASE WHEN UPCASE(t1.COLLATERAL_TYPE) = 'VEHICLE' THEN 'TITLE' ELSE 'UNKNOWN' END) AS COLLATERAL_TYPE, 
          t1.ETL_DT AS ETLDT, 
          t1.PRODUCT_TYPE AS PRODUCTCD, 
          t1.INTEREST AS INTERESTFEE, 
          /* ACHAUTHFLG */
            ("") LABEL="ACHAUTHFLG" AS ACHAUTHFLG, 
          t1.DATE_UPDATED AS UPDATEDT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'BEGINNING')) FORMAT=MMDDYY10. LABEL="ENDDT" AS ENDDT
      FROM WORK.MOST_RECENT_DEALS t1
           LEFT JOIN EDW.D_LOCATION t2 ON (t1.ST_CODE = t2.LOC_NBR)
      WHERE t1.DATE_UPDATED >= &LASTWEEK.
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.PREV_DEALNBRS_SORT AS 
   SELECT t1.BO_CODE AS CUSTNBR, 
          t1.LOAN_CODE
      FROM WORK.MOST_RECENT_DEALS t1
      ORDER BY t1.BO_CODE,
               t1.LOAN_CODE;
%RUNQUIT(&job,&sub3);

DATA PREV_DEALS;
	SET WORK.PREV_DEALNBRS_SORT;
	BY CUSTNBR;
	PREVDEALNBR = LAG(LOAN_CODE);
	IF FIRST.CUSTNBR THEN PREVDEALNBR = .;
%RUNQUIT(&job,&sub3);

PROC SQL;
	CREATE TABLE WORK.SORTED_CUSTS AS
		SELECT * FROM QFUND1.CUSTOMER
		ORDER BY BO_CODE, BO_ID;
%RUNQUIT(&job,&sub3);

DATA WORK.MOST_RECENT_CUSTS;
	SET WORK.SORTED_CUSTS;
	BY BO_CODE;
	IF LAST.BO_CODE;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.ADD_SSN AS 
   SELECT t1.PRODUCT, 
          t1.PRODUCT_DESC, 
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
          t1.LOCNBR, 
          t1.BUSINESS_UNIT, 
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          t2.SSN, 
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
          t1.REFINANCECNT, 
          t1.CASHAGNCNT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.DEALENDDT, 
          t1.ACHSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.MERGE_WITH_LOC_DATA t1
           LEFT JOIN WORK.MOST_RECENT_CUSTS t2 ON (t1.CUSTNBR = t2.BO_CODE);
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.ADD_PREV_DEALNBR AS 
   SELECT t1.PRODUCT, 
          t1.PRODUCT_DESC, 
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
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.CUSTNBR, 
          t1.DEALNBR, 
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
          t1.CASHAGNCNT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.DEALENDDT, 
          t1.ACHSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t2.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.ADD_SSN t1
           LEFT JOIN WORK.PREV_DEALS t2 ON (t1.DEALNBR = t2.LOAN_CODE)
      ORDER BY t1.LOCNBR,
               t1.CUSTNBR,
               t1.DEALNBR,
               t1.DEAL_DT;
%RUNQUIT(&job,&sub3);

PROC SQL;
	CREATE TABLE WORK.QF1_SORTED_SCHED AS
		SELECT * FROM QFUND1.LOAN_SCHEDULE
		ORDER BY LOAN_CODE, LOAN_ID, INST_DUE_DATE, PAID_FLAG;
%RUNQUIT(&job,&sub3);

DATA WORK.MOST_RECENT_SCHED;
	SET WORK.QF1_SORTED_SCHED;
	WHERE ACTIVE_FLG = 'Y' 
/*		  AND PAID_FLAG = 'N'*/;
	KEEP LOAN_CODE LOAN_ID PAID_FLAG INST_DUE_DATE;
%RUNQUIT(&job,&sub3);

DATA WORK.PAID;
	SET WORK.QF1_SORTED_SCHED;
	WHERE PAID_FLAG = 'N' AND ACTIVE_FLG = 'Y';
	KEEP LOAN_CODE LOAN_ID PAID_FLAG INST_DUE_DATE;
%RUNQUIT(&job,&sub3);

DATA DUE_DT_PAID;
	SET WORK.PAID;
	BY LOAN_CODE;
	IF FIRST.LOAN_CODE;
%RUNQUIT(&job,&sub3);

DATA FIRST_DUEDT;
	SET WORK.MOST_RECENT_SCHED;
	BY LOAN_CODE;
	IF FIRST.LOAN_CODE;
%RUNQUIT(&job,&sub3);

DATA DUE_DT_PRE;
	SET WORK.MOST_RECENT_SCHED;
	BY LOAN_CODE;
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub3);

PROC APPEND BASE=WORK.DUE_DT_PRE DATA=WORK.DUE_DT_PAID;
%RUNQUIT(&job,&sub3);

PROC SORT DATA=WORK.DUE_DT_PRE;
BY LOAN_CODE LOAN_ID INST_DUE_DATE;
%RUNQUIT(&job,&sub3);

DATA DUE_DT;
	SET WORK.DUE_DT_PRE;
	BY LOAN_CODE;
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.SORTED_TRANS AS 
   SELECT t1.LOAN_CODE, 
          t1.LOAN_ID, 
          t1.LOAN_TRAN_CODE, 
          t1.TRAN_DATE, 
          t1.TOTAL_DUE
      FROM QFUND1.LOAN_TRANSACTION t1
      ORDER BY t1.LOAN_CODE,
               t1.LOAN_ID,
               t1.LOAN_TRAN_CODE,
               t1.TRAN_DATE;
%RUNQUIT(&job,&sub3);

DATA MOST_RECENT_TXN;
	SET WORK.SORTED_TRANS;
	BY LOAN_CODE LOAN_ID LOAN_TRAN_CODE TRAN_DATE;
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.ADD_DUE_DT AS 
   SELECT t1.PRODUCT, 
          t1.PRODUCT_DESC, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.BANKMODEL, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.ZONENAME, 
          t1.ZONENBR, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.BUSINESS_UNIT, 
          t1.LOCNBR, 
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEALNBR, 
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
          t1.CASHAGNCNT, 
          t2.INST_DUE_DATE AS DUEDT, 
          t4.INST_DUE_DATE AS FIRST_DUEDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.DEALENDDT AS DEALENDDT, 
          t1.ACHSTATUSCD, 
          t1.DEALSTATUSCD AS CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.ADD_PREV_DEALNBR t1
           LEFT JOIN WORK.DUE_DT t2 ON (t1.DEALNBR = t2.LOAN_CODE)
           LEFT JOIN WORK.MOST_RECENT_TXN t3 ON (t1.DEALNBR = t3.LOAN_CODE)
           LEFT JOIN WORK.FIRST_DUEDT t4 ON (t1.DEALNBR = t4.LOAN_CODE);
%RUNQUIT(&job,&sub3);

DATA QF1_TITLE_DAILY_UPDATE;
	SET WORK.ADD_DUE_DT;
%RUNQUIT(&job,&sub3);


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
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
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
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          /* DEPOSITDT */
            (DHMS(t1.DEPOSITDT,00,00,00)) FORMAT=DATETIME20. AS DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          t1.ETLDT, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
        t1.ENDDT,
		. AS OUTSTANDING_DRAW_AMT,
		 '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM QF1_TITLE_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub3);

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
%RUNQUIT(&job,&sub3);

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
%RUNQUIT(&job,&sub3);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND1_QFUND2\TITLE",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND1_QFUND2\TITLE\DIR2\",'G');
%RUNQUIT(&job,&sub3);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF1T (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE;
%RUNQUIT(&job,&sub3);


/*UPLOAD QF1T*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF1T.SAS";
