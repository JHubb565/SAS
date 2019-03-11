%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub9);

DATA _NULL_;
	CALL SYMPUTX('LASTWEEK',INTNX('DAY',TODAY(),-5,'B'),G);
RUN;

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
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.TXNS_SORTED_PRE AS 
   SELECT t1.LOAN_NBR, 
          t1.CUSTOMER_NBR, 
          t1.BRANCH_NBR, 
          t1.TRANSACTION_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.TRANSACTION_AMT, 
          t1.TRANSACTION_TYPE, 
          t1.DATE_CREATED, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED
      FROM ECA.QF_TRANSACTION_DATA t1
      WHERE t1.TRANSACTION_DATE >= &LASTWEEK
      ORDER BY t1.LOAN_NBR,
               t1.TRANSACTION_DATE,
               t1.TRANSACTION_NBR;
%RUNQUIT(&job,&sub9);

DATA QF4_PDL_VOID_TXNS;
	SET WORK.TXNS_SORTED_PRE;
	where void_flag ^= 'N';
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.TXNS_SORTED AS 
   SELECT t1.LOAN_NBR, 
          t1.CUSTOMER_NBR, 
          t1.BRANCH_NBR, 
          t1.TRANSACTION_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.TRANSACTION_AMT, 
          t1.TRANSACTION_TYPE, 
          t1.DATE_CREATED, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED
      FROM WORK.TXNS_SORTED_PRE t1
           LEFT JOIN WORK.QF4_PDL_VOID_TXNS t2 ON (t1.TRANSACTION_NBR = t2.REF_TRAN_CODE)
      WHERE t1.VOID_FLAG = 'N' AND t2.VOID_FLAG IS MISSING
      ORDER BY t1.LOAN_NBR,
               t1.TRANSACTION_NBR,
               t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub9);

/* GET WRITEOFF DATES */
PROC SQL;
	CREATE TABLE WORK.WRITEOFF_DATES_PDL
		AS SELECT LOAN_NBR AS DEALNBR, 
		   CASE WHEN UPCASE(TRANSACTION_TYPE) IN ("WO","WOB","WOT") THEN TRANSACTION_DATE ELSE ""DT END AS WRITEOFFDT FORMAT DATETIME20.
	FROM WORK.TXNS_SORTED
	WHERE CALCULATED WRITEOFFDT ^= .;
%RUNQUIT(&job,&sub9);

/* GET DEFAULT DATES */
PROC SQL;
	CREATE TABLE WORK.DEFAULT_DATES_PDL
		AS SELECT LOAN_NBR AS DEALNBR,
		   CASE WHEN UPCASE(TRANSACTION_TYPE) IN ("NSF", "ACHR") THEN TRANSACTION_DATE ELSE ""DT END AS DEFAULTDT FORMAT DATETIME20.
	FROM WORK.TXNS_SORTED
	WHERE CALCULATED DEFAULTDT ^= .;
%RUNQUIT(&job,&sub9);

/* GET DEAL END DATES */
DATA MOST_RECENT_TXN;
	SET WORK.TXNS_SORTED;
	BY LOAN_NBR TRANSACTION_NBR TRANSACTION_DATE;
	IF LAST.LOAN_NBR;
%RUNQUIT(&job,&sub9);

PROC SQL;
	CREATE TABLE PAID_PDL AS 
		SELECT LOAN_NBR AS DEALNBR,
			   SUM(CASE WHEN TRANSACTION_TYPE IN (
			   									  'ACHD',
												  'ACHP',
												  'ACHPP',
												  'BUY',
												  'CAB',
												  'DP',
												  'EPAY',
												  'NP',
												  'NPP',
												  'PAY',
												  'PPAY',
												  'WOR')
					THEN TRANSACTION_AMT
					WHEN TRANSACTION_TYPE IN('NSF','ACHR') 
					THEN -TRANSACTION_AMT ELSE 0
					END) as TOTALPAID
		FROM WORK.TXNS_SORTED
		GROUP BY LOAN_NBR;
%RUNQUIT(&job,&sub9);

DATA DEFAULT_LOANS_PDL;
	SET DEFAULT_DATES_PDL;
	BY DEALNBR;
	IF FIRST.DEALNBR;
%RUNQUIT(&job,&sub9);

DATA WRITEOFF_LOANS_PDL;
	SET WORK.WRITEOFF_DATES_PDL;
	BY DEALNBR;
	IF FIRST.DEALNBR;
%RUNQUIT(&job,&sub9);


PROC SQL;
   CREATE TABLE WORK.ECA_PAYDAY_DS AS 
   SELECT /* PRODUCT */
            ("PAYDAY") AS PRODUCT, 
          /* POS */
            ("QFUND") AS POS, 
          /* INSTANCE */
            ("QFUND4") AS INSTANCE, 
          /* BANKMODEL */
            ("STANDARD") AS BANKMODEL, 
          t1.LOAN_DATE AS DEAL_DTTM, 
          /* DEAL_DT */
            (DATEPART(T1.LOAN_DATE)) FORMAT=MMDDYY10. AS DEAL_DT, 
          /* BEGINDT */
            (INTNX('YEAR',TODAY(),-6,'B')) FORMAT=MMDDYY10. AS BEGINDT, 
          t1.LOAN_NBR AS DEALNBR, 
          t1.CUSTOMER_NBR, 
          t1.BRANCH_NBR, 
          t1.LOAN_STATUS, 
          t1.LOAN_CHECK_STATUS, 
          t1.DUE_DATE, 
          t1.LOAN_DUE, 
          t1.LOAN_AMOUNT, 
          t1.LOAN_FEE, 
          t1.LOAN_REBATE, 
          t1.LOAN_CHECK, 
          t1.LOAN_RETURN_FEE, 
          t1.LOAN_TOTAL_DUE, 
          t1.DATE_UPDATED, 
          t1.UPDATED_BY, 
          t1.DATE_CREATED, 
          t1.CREATED_BY, 
          t1.PAYMENT_PLAN_DATE, 
          t1.MULTIPLE_CHECK_FLAG, 
          t1.CHECK_EX_FLAG, 
          t1.ELAPSED_DUE_DATE, 
          t1.IS_NEW, 
          t1.LOAN_INTEREST, 
          t1.LOAN_MMF, 
          t1.LOAN_TYPE, 
          t1.INT_REBATE, 
          t1.MMF_REBATE, 
          t1.CHECK_NUMBER, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.PWO_DATE, 
          t1.PWO_AMT, 
          t1.SETTLEMENTAMT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'BEGINNING')) FORMAT=MMDDYY10. AS ENDDT
      FROM ECA.QF_PAYDAY_LOAN_DATA t1
      WHERE t1.LOAN_DATE >= &lastweek OR t1.DATE_UPDATED >= &lastweek
      ORDER BY t1.CUSTOMER_NBR,
               t1.LOAN_NBR;
%RUNQUIT(&job,&sub9);


PROC SQL;
   CREATE TABLE WORK.QF4_PAYDAY_DS_PRE AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BANKMODEL, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEALNBR, 
          t1.CUSTOMER_NBR AS CUSTNBR, 
          t1.LOAN_AMOUNT AS ADVAMT, 
          t1.LOAN_FEE AS FEEAMT, 
          /* LOCNBR */
            (CASE WHEN LENGTH(COMPRESS(PUT(CASE WHEN t2.LOCATION_AA = . THEN t1.BRANCH_NBR ELSE t2.LOCATION_AA 
                        END,8.))) = 5 THEN (CASE WHEN t2.LOCATION_AA = . THEN t1.BRANCH_NBR ELSE t2.LOCATION_AA 
            END)/100 ELSE (CASE 
                        WHEN t2.LOCATION_AA = . THEN t1.BRANCH_NBR ELSE t2.LOCATION_AA END) END) AS LOCNBR, 
          /* LATEFEEAMT */
            (.) AS LATEFEEAMT, 
          /* NSFFEEAMT */
            (t1.LOAN_RETURN_FEE) AS NSFFEEAMT, 
          /* OTHERFEEAMT */
            (.) AS OTHERFEEAMT, 
          /* REBATEAMT */
            (sum(t1.LOAN_REBATE,t1.INT_REBATE,t1.MMF_REBATE)) AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          /* TOTALPAID */
            (.) AS TOTALPAID, 
          t1.LOAN_TOTAL_DUE AS TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* CASHAGNCNT */
            (.) AS CASHAGNCNT, 
          t1.DUE_DATE AS DUEDT, 
          /* DEALENDDT */
            (""DT) FORMAT=DATETIME20. AS DEALENDDT, 
          /* DEPOSITDT */
            (""DT) FORMAT=DATETIME20. AS DEPOSITDT, 
          /* WRITEOFFDT */
            (""DT) FORMAT=DATETIME20. AS WRITEOFFDT, 
          /* DEFAULTDT */
            (""DT) FORMAT=DATETIME20. AS DEFAULTDT, 
          /* ACHSTATUSCD */
            ("") AS ACHSTATUSCD, 
          t1.LOAN_CHECK_STATUS AS CHECKSTATUSCD, 
          t1.LOAN_STATUS AS DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            (case when t1.CHECK_NUMBER ^= "" then "CHECK"
                     else "UNKNOWN"
            end) AS COLLATERAL_TYPE, 
          /* ETLDT */
            (""DT) FORMAT=DATETIME20. AS ETLDT, 
          t1.LOAN_TYPE AS PRODUCTCD, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          t1.LOAN_INTEREST AS INTERESTFEE, 
          /* ACHAUTHFLG */
            ("") AS ACHAUTHFLG, 
          t1.DATE_UPDATED AS UPDATEDT, 
          t1.ENDDT
      FROM WORK.ECA_PAYDAY_DS t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
      WHERE t1.DEAL_DT BETWEEN t1.BEGINDT AND t1.ENDDT;
%RUNQUIT(&job,&sub9);


PROC SQL;
   CREATE TABLE WORK.QF4_PAYDAY_DS AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BANKMODEL, 
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
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.LATEFEEAMT, 
          t1.NSFFEEAMT LABEL='', 
          t1.OTHERFEEAMT, 
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
          t1.ETLDT, 
          t1.PRODUCTCD, 
          t1.PREVDEALNBR, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.QF4_PAYDAY_DS_PRE t1
           LEFT JOIN EDW.D_LOCATION t2 ON (t1.LOCNBR = t2.LOC_NBR);
%RUNQUIT(&job,&sub9);


PROC SQL;
   CREATE TABLE WORK.QF4_DEALSUMMARY_PAYDAY_PRE AS 
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
          /* TITLE_DEALNBR */
            (.) AS TITLE_DEALNBR, 
          t1.CUSTNBR, 
          t2.SSN, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.LATEFEEAMT, 
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          /* DEALENDDT */
            (case when t3.TOTAL_AMOUNT_DUE <= 0 then t3.TRANSACTION_DATE else . end) FORMAT=DATETIME20. AS DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD LABEL='', 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.QF4_PAYDAY_DS t1
           LEFT JOIN ECA.QF_CUSTOMER_DETAILS t2 ON (t1.CUSTNBR = t2.CUSTOMER_NBR)
           LEFT JOIN WORK.MOST_RECENT_TXN t3 ON (t1.DEALNBR = t3.LOAN_NBR);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE QF4_PDL_DAILY_UPDATE AS 
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
          t1.TITLE_DEALNBR, 
          t1.CUSTNBR, 
          t1.SSN, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.LATEFEEAMT, 
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          /* TOTALPAID */
            (sum(t4.TOTALPAID,-t1.REBATEAMT)) AS TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.DEPOSITDT, 
          t2.WRITEOFFDT, 
          t3.DEFAULTDT, 
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
      FROM WORK.QF4_DEALSUMMARY_PAYDAY_PRE t1
           LEFT JOIN WORK.DEFAULT_LOANS_PDL t3 ON (t1.DEALNBR = t3.DEALNBR)
           LEFT JOIN WORK.WRITEOFF_LOANS_PDL t2 ON (t1.DEALNBR = t2.DEALNBR)
           LEFT JOIN WORK.PAID_PDL t4 ON (t1.DEALNBR = t4.DEALNBR);
%RUNQUIT(&job,&sub9);

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
          t1.TITLE_DEALNBR, 
          t1.CUSTNBR, 
          t1.SSN, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.LATEFEEAMT, 
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
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
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT,
		  . AS OUTSTANDING_DRAW_AMT,
		  '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM QF4_PDL_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub9);

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
          .								AS WAIVEDFEEAMT, 
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
%RUNQUIT(&job,&sub9);

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
%RUNQUIT(&job,&sub9);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND4\PDL",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND4\PDL\DIR2\",'G');
%RUNQUIT(&job,&sub9);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF4PDL (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE
	WHERE STATE NOT IN ('TN');
%RUNQUIT(&job,&sub9);

/*UPLOAD QF4PDL*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF4PDL.SAS";
