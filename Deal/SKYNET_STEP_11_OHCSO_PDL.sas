%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM\";
DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub11);

LIBNAME QFUND5 ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=QFUND5 DEFER=YES;

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
%RUNQUIT(&job,&sub11);

PROC SORT DATA=OHCSO.CSO_LOAN_SUMMARY OUT=CSO_LOANS_SORT;
	BY LOAN_CODE LOAN_ID;
	WHERE PRODUCT_TYPE = 'PDL';
%RUNQUIT(&job,&sub11);

PROC SORT DATA=QFUND5.NCP_LOAN_SUMMARY OUT=WORK.NCP_LOANS_SORT;
	BY LOAN_CODE LOAN_ID;
	WHERE PRODUCT_TYPE = 'PDL';
%RUNQUIT(&job,&sub11);

DATA WORK.CSO_RECENT_LOAN;
	SET WORK.CSO_LOANS_SORT;
	BY LOAN_CODE;
	SOURCE = 'CSO';
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub11);

DATA WORK.NCP_RECENT_LOAN;
	SET WORK.NCP_LOANS_SORT;
	BY LOAN_CODE;
	SOURCE = 'NCP';
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub11);

PROC SQL;
CREATE TABLE WORK.QF5_COMB_LOAN AS 
SELECT * FROM WORK.NCP_RECENT_LOAN
 OUTER UNION CORR 
SELECT * FROM WORK.CSO_RECENT_LOAN
;
%RUNQUIT(&job,&sub11);


PROC SORT DATA=WORK.QF5_COMB_LOAN OUT=QF5_SORTED_LOANS;
	BY LOAN_CODE SOURCE LOAN_ID;
%RUNQUIT(&job,&sub11);

DATA MOST_RECENT_CSO_NCP;
	SET WORK.QF5_SORTED_LOANS;
	BY LOAN_CODE source LOAN_ID;
	TEST = LAG(DUE_DATE);
	IF FIRST.LOAN_CODE THEN DUE_DATE = DUE_DATE;
	ELSE IF DUE_DATE = . THEN DUE_DATE = TEST;
	IF FIRST.LOAN_CODE;
	IF ST_CODE = . THEN ST_CODE = CSO_ST_CODE;
	FORMAT TEST DATETIME20.;
/*KEEP LOAN_CODE LOAN_ID DUE_DATE TEST;*/
%RUNQUIT(&job,&sub11);


PROC SQL;
   CREATE TABLE WORK.QF5_PDL_DAILY_UPDATE_PRE AS 
   SELECT /* PRODUCT */
            ("PAYDAY") AS PRODUCT, 
          /* POS */
            ("QFUND") AS POS, 
          /* INSTANCE */
            ("QFUND5") AS INSTANCE, 
          /* BANKMODEL */
            ("CSO") AS BANKMODEL, 
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
          t1.ST_CODE AS LOCNBR, 
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          /* BEGINDT */
            (INTNX('MONTH',TODAY(),-60,'B')) FORMAT=MMDDYY10. AS BEGINDT, 
          /* DEAL_DT */
            (DATEPART(t1.LOAN_DATE)) FORMAT=MMDDYY10. AS DEAL_DT, 
          t1.LOAN_DATE AS DEAL_DTTM, 
          t1.LOAN_CODE AS DEALNBR, 
          /* TITLE_LOAN_NBR */
            (.) AS TITLE_LOAN_NBR, 
          t1.BO_CODE AS CUSTNBR, 
          t1.LOAN_AMT AS ADVAMT, 
          /* FEEAMT */
            (SUM(t1.CSO_FEE,t1.CRF_FEE,t1.INF_FEE)) AS FEEAMT, 
          t1.RTN_FEE_AMT AS NSFFEEAMT, 
          /* LATEFEEAMT */
            (.) AS LATEFEEAMT, 
          /* OTHERFEEAMT */
            (.) AS OTHERFEEAMT, 
          t1.WAIVED_RTN_FEE_AMT AS WAIVEDFEEAMT, 
          /* REBATEAMT */
            (.) AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t1.TOTAL_PAID AS TOTALPAID, 
          t1.TOTAL_DUE AS TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* REFINANCECNT */
            (.) AS REFINANCECNT, 
          /* CASHAGNCNT */
            (.) AS CASHAGNCNT, 
          t1.DUE_DATE AS DUEDT, 
          t1.LOAN_END_DATE AS DEALENDDT, 
          /* DEPOSITDT */
            (''DT) FORMAT=DATETIME20. AS DEPOSITDT, 
          t1.WO_DATE AS WRITEOFFDT, 
          t1.DEFAULT_DATE AS DEFAULTDT, 
          /* ACHSTATUSCD */
            (CASE WHEN t1.ACH_OPT_OUT = "Y" THEN "N"
                      WHEN t1.ACH_OPT_OUT = "N" THEN "Y"
                      ELSE ""
            END) AS ACHSTATUSCD, 
          /* CHECKSTATUSCD */
            ("") AS CHECKSTATUSCD, 
          t1.LOAN_STATUS_ID AS DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            (CASE WHEN t1.COLLATERAL_TYPE = "RCC" THEN "CHECK"
                       WHEN t1.COLLATERAL_TYPE = "ACH" THEN "ACH"
                       ELSE "UNKNOWN"
            END) AS COLLATERAL_TYPE, 
          t1.ETL_DT AS ETLDT, 
          t1.PREV_LOAN_CODE AS PREVDEALNBR, 
          t1.PRODUCT_TYPE AS PRODUCTCD, 
          /* INTERESTFEE */
            (t1.INT_FEE) AS INTERESTFEE, 
          /* ACHAUTHFLG */
            ('') AS ACHAUTHFLG, 
          t1.DATE_UPDATED AS UPDATEDT, 
          t1.SOURCE, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'B')) FORMAT=MMDDYY10. AS ENDDT
      FROM WORK.MOST_RECENT_CSO_NCP t1
           LEFT JOIN EDW.D_LOCATION t2 ON (t1.ST_CODE = t2.LOC_NBR)
      WHERE t1.PRODUCT_TYPE = 'PDL' AND t1.DATE_UPDATED >= &LASTWEEK;
%RUNQUIT(&job,&sub11);

PROC SORT DATA=OHCSO.CSO_CUSTOMER OUT=CSO_CUST_SORT;
	BY BO_CODE BO_ID;
%RUNQUIT(&job,&sub11);

PROC SORT DATA=QFUND5.NCP_CUSTOMER(KEEP=BO_CODE BO_ID SSN) OUT=NCP_CUST_SORT;
	BY BO_CODE BO_ID;
%RUNQUIT(&job,&sub11);

DATA WORK.CSO_RECENT_CUST;
	SET WORK.CSO_CUST_SORT;
	BY BO_CODE;
	IF LAST.BO_CODE;
%RUNQUIT(&job,&sub11);

DATA WORK.NCP_RECENT_CUST;
	SET WORK.NCP_CUST_SORT;	
	BY BO_CODE;
	IF LAST.BO_CODE;
%RUNQUIT(&job,&sub11);

PROC SQL;
CREATE TABLE WORK.COMB_CUSTS AS 
SELECT * FROM WORK.CSO_RECENT_CUST
 OUTER UNION CORR 
SELECT * FROM WORK.NCP_RECENT_CUST
;
%RUNQUIT(&job,&sub11);

PROC SORT DATA=WORK.COMB_CUSTS OUT=COMB_CUSTS_SORTED;
	BY BO_CODE BO_ID;
%RUNQUIT(&job,&sub11);

DATA MOST_RECENT_CUST_CSO_NCP;
	SET COMB_CUSTS_SORTED;
	BY BO_CODE;
	IF LAST.BO_CODE;
	KEEP BO_CODE SSN;
%RUNQUIT(&job,&sub11);

PROC SQL;
   CREATE TABLE WORK.QF5_PDL_DAILY_UPDATE_PRE2 AS 
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
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BEGINDT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.TITLE_LOAN_NBR, 
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
          t1.DUEDT LABEL='', 
          t1.DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.SOURCE, 
          t1.ENDDT
      FROM WORK.QF5_PDL_DAILY_UPDATE_PRE t1
           LEFT JOIN WORK.MOST_RECENT_CUST_CSO_NCP t2 ON (t1.CUSTNBR = t2.BO_CODE);
%RUNQUIT(&job,&sub11);

PROC SQL;
   CREATE TABLE QF5_PDL_DAILY_UPDATE AS 
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
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BEGINDT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.TITLE_LOAN_NBR, 
          /* CUSTNBR */
            (CASE WHEN SOURCE = 'NCP' THEN COALESCE(t2.BO_CODE,T1.CUSTNBR) ELSE t1.CUSTNBR END) AS CUSTNBR, 
          /* SSN */
            (CASE WHEN t1.SSN IS NULL THEN t3.SSN ELSE t1.SSN END ) AS SSN, 
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
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.QF5_PDL_DAILY_UPDATE_PRE2 t1
           LEFT JOIN EDW.CSO_APPLICATION_INFO t2 ON (t1.DEALNBR = t2.APP_NO)
           LEFT JOIN WORK.MOST_RECENT_CUST_CSO_NCP t3 ON (t2.BO_CODE = t3.BO_CODE);
%RUNQUIT(&job,&sub11);

PROC SQL;
   CREATE TABLE WORK.DEAL_SUMMARY_TMP AS 
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
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BEGINDT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.TITLE_LOAN_NBR AS TITLE_DEALNBR, 
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
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          t1.ETLDT, 
          t1.PREVDEALNBR LABEL='', 
          t1.PRODUCTCD, 
          t1.INTERESTFEE LABEL='', 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT,
		  . AS OUTSTANDING_DRAW_AMT,
		 '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM QF5_PDL_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub11);

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
%RUNQUIT(&job,&sub11);

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
%RUNQUIT(&job,&sub11);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND5\PDL",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND5\PDL\DIR2\",'G');
%RUNQUIT(&job,&sub11);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF5PDL (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE;
%RUNQUIT(&job,&sub11);

/*UPLOAD QF5PDL*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF5PDL.SAS";
