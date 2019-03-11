%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub8);

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
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.ECA_TITLE_DS_PRE AS 
   SELECT t1.LOAN_NBR, 
          t1.CUSTOMER_NBR, 
          t1.BRANCH_NBR AS BRANCH_NBR, 
          t1.TITLE_LOAN_NBR, 
          t1.PREV_TITLE_LOAN_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.TOTAL_DUE, 
          t1.RENEWAL_DATE, 
          t1.RENEWAL_CHARGE, 
          t1.LOAN_STATUS, 
          t1.TITLE_STATUS, 
          t1.DATE_UPDATED, 
          t1.UPDATED_BY, 
          t1.DATE_CREATED, 
          t1.CREATED_BY, 
          t1.REPO_FEE_BALANCE, 
          t1.ROUGH_VALUE, 
          t1.AVERAGE_VALUE, 
          t1.CLEAN_VALUE, 
          t1.EXTRA_CLEAN_VALUE, 
          t1.VALUE_USED, 
          t1.PERCENT_USED, 
          t1.PAYEE_NAME, 
          t1.PAYEE_DATE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.PWO_DATE, 
          t1.PWO_AMT, 
          t1.FINANCE_ADVANCE_VALUE, 
          /* BEGINDT */
            (DATEPART(&LASTWEEK)) FORMAT=MMDDYY10. AS BEGINDT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'BEGINNING')) FORMAT=MMDDYY10. AS ENDDT, 
          /* DEAL_DT */
            (DATEPART(T1.LOAN_DATE)) FORMAT=MMDDYY10. AS DEAL_DT
      FROM ECA.QF_TP_LOAN_DATA t1
      WHERE t1.LOAN_DATE >= &lastweek OR t1.DATE_UPDATED >= &lastweek
/*	  	LOAN_NBR = 62882075*/
      ORDER BY t1.LOAN_NBR,
               t1.TITLE_LOAN_NBR,
               t1.RENEWAL_DATE;
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.TXNS_SORTED_TLP AS 
   SELECT t2.LOAN_NBR, 
          t2.BRANCH_NBR, 
          t2.CUSTOMER_NBR, 
          t2.TRANSACTION_DATE, 
          t2.TRANSACTION_TYPE, 
          t2.TOTAL_AMOUNT_DUE, 
          t2.BAL_PRINCIPAL_AMT, 
          t2.BAL_LIEN_FEE_AMT, 
          t2.BAL_INT_AMOUNT, 
          t2.REPO_CHARGE, 
          t2.STORAGE_COST, 
          t2.CREATED_BY, 
          t2.DATE_CREATED, 
          t2.TRANSACTION_NBR, 
          t2.VOID_FLAG, 
          t2.REF_TRAN_CODE, 
          t2.TITLE_LOAN_NBR, 
          t2.IS_DECEASED, 
          t2.TRANSACTION_AMT, 
          t2.IS_CSR, 
          t2.LATE_FEE, 
          t2.OTHER_FEE, 
          t2.CREATE_DATE_TIME, 
          t2.UPDATE_DATE_TIME, 
          t2.CREATE_USER_NM, 
          t2.UPDATE_USER_NM, 
          t2.CREATE_PROGRAM_NM, 
          t2.UPDATE_PROGRAM_NM
      FROM WORK.ECA_TITLE_DS_PRE t1, ECA.QF_TP_TRANSACTION_DATA t2
      WHERE (t1.LOAN_NBR = t2.LOAN_NBR AND t1.TITLE_LOAN_NBR = t2.TITLE_LOAN_NBR)
      ORDER BY t2.LOAN_NBR,
               t2.TITLE_LOAN_NBR,
               t2.TRANSACTION_DATE,
               t2.TRANSACTION_NBR;
%RUNQUIT(&job,&sub8);

DATA QF4_TLP_VOIDED_TXNS;
	SET WORK.TXNS_SORTED_TLP;
	WHERE VOID_FLAG ^= 'N' AND TRANSACTION_TYPE NOT IN('ADV','AGN','CAGN','AGND');
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.TLP_NO_VOIDS AS 
   SELECT t1.LOAN_NBR, 
          t1.TITLE_LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.CUSTOMER_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.BAL_PRINCIPAL_AMT, 
          t1.BAL_LIEN_FEE_AMT, 
          t1.BAL_INT_AMOUNT, 
          t1.REPO_CHARGE, 
          t1.STORAGE_COST, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.LATE_FEE, 
          t1.OTHER_FEE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM
      FROM WORK.TXNS_SORTED_TLP t1
           LEFT JOIN WORK.QF4_TLP_VOIDED_TXNS t2 ON (t1.TRANSACTION_NBR = t2.REF_TRAN_CODE)
      WHERE t1.VOID_FLAG = 'N' AND t2.VOID_FLAG IS MISSING
      ORDER BY t1.LOAN_NBR,
               t1.TITLE_LOAN_NBR,
               t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.ADVAMT_TMP AS 
   SELECT t1.LOAN_NBR, 
          t1.TITLE_LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.CUSTOMER_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.BAL_PRINCIPAL_AMT, 
          t1.BAL_LIEN_FEE_AMT, 
          t1.BAL_INT_AMOUNT, 
          t1.REPO_CHARGE, 
          t1.STORAGE_COST, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.LATE_FEE, 
          t1.OTHER_FEE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM
      FROM WORK.TLP_NO_VOIDS t1
      WHERE t1.TRANSACTION_TYPE IN 
           (
           'ADV',
           'CAGN',
           'AGN',
           'AGND'
           )
      ORDER BY t1.LOAN_NBR,
               t1.TITLE_LOAN_NBR,
               t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.WRITEOFF_DATES AS 
   SELECT t1.TITLE_LOAN_NBR, 
          /* WRITEOFFDT */
            (case when t1.TRANSACTION_TYPE IN("WO","WOB","WOT") then t1.TRANSACTION_DATE else ""DT end) FORMAT=datetime20. AS 
            WRITEOFFDT
      FROM WORK.TLP_NO_VOIDS t1
      WHERE (CALCULATED WRITEOFFDT) NOT IS MISSING
      ORDER BY t1.TITLE_LOAN_NBR,
               t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_DATES AS 
   SELECT t1.LOAN_NBR, 
          t1.TITLE_LOAN_NBR, 
          /* DEFAULTDT */
            (case when t1.TRANSACTION_TYPE = "DEF" then t1.TRANSACTION_DATE else ""DT end) FORMAT=datetime20. AS 
            DEFAULTDT
      FROM WORK.TLP_NO_VOIDS t1
      WHERE (CALCULATED DEFAULTDT) NOT IS MISSING
      ORDER BY t1.TITLE_LOAN_NBR,
               t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub8);

DATA MOST_RECENT_TXN_TLP;
	SET WORK.TLP_NO_VOIDS;
	BY LOAN_NBR TITLE_LOAN_NBR TRANSACTION_DATE;
	IF LAST.LOAN_NBR;
%RUNQUIT(&job,&sub8);

DATA DEFAULT_LOANS;
	SET WORK.DEFAULT_DATES;
	BY TITLE_LOAN_NBR;
	IF FIRST.TITLE_LOAN_NBR;
%RUNQUIT(&job,&sub8);

DATA WRITEOFF_LOANS;
	SET WORK.WRITEOFF_DATES;
	BY TITLE_LOAN_NBR;
	IF FIRST.TITLE_LOAN_NBR;
%RUNQUIT(&job,&sub8);

DATA WORK.ADVAMT2;
	SET WORK.ADVAMT_TMP;
	BY LOAN_NBR TITLE_LOAN_NBR;
	IF FIRST.TITLE_LOAN_NBR;
	IF BAL_PRINCIPAL_AMT ^= . THEN PRINCIPAL_AMT = BAL_PRINCIPAL_AMT;
	ELSE IF BAL_PRINCIPAL_AMT = . THEN PRINCIPAL_AMT= SUM(TOTAL_AMOUNT_DUE,-BAL_LIEN_FEE_AMT,-BAL_INT_AMOUNT);
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.TLP_PAID AS 
   SELECT DISTINCT t1.LOAN_NBR, 
          t1.TITLE_LOAN_NBR, 
          /* INT_CHARGED */
            (SUM(case when t1.TRANSACTION_TYPE in('TAINT','SHD') THEN t1.BAL_INT_AMOUNT ELSE 0 END)) AS INT_CHARGED, 
          /* SUM_OF_TRANSACTION_AMT */
            (SUM(CASE WHEN t1.TRANSACTION_TYPE IN(
            'BUY',
            'CAB',
            'DFP',
            'PAY',
            'RPY',
            'TPAY',
            'WOR',
            'SET') THEN t1.TRANSACTION_AMT ELSE 0 END)) AS SUM_OF_TRANSACTION_AMT
      FROM WORK.TLP_NO_VOIDS t1
      WHERE t1.TRANSACTION_TYPE IN 
           (
           'BUY',
           'CAB',
           'DFP',
           'PAY',
           'RPY',
           'TPAY',
           'WOR',
           'SET',
           'TAINT',
           'SHD'
           )
      GROUP BY t1.LOAN_NBR,
               t1.TITLE_LOAN_NBR;
%RUNQUIT(&job,&sub8);

DATA MOST_RECENT_BALANCE;
	SET ECA_TITLE_DS_PRE;
	BY LOAN_NBR TITLE_LOAN_NBR;
	IF LAST.TITLE_LOAN_NBR;
%RUNQUIT(&job,&sub8);

PROC SORT DATA=WORK.ECA_TITLE_DS_PRE OUT=ECA_TITLE_DS_SORTED;
BY LOAN_NBR TITLE_LOAN_NBR CUSTOMER_NBR;
%RUNQUIT(&job,&sub8);

DATA MOST_RECENT_CUSTNBR;
	SET WORK.ECA_TITLE_DS_SORTED;
	BY LOAN_NBR TITLE_LOAN_NBR CUSTOMER_NBR;
	IF LAST.LOAN_NBR;
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.ECA_TITLE_DS AS 
   SELECT t1.LOAN_NBR, 
          t2.CUSTOMER_NBR, 
          t1.BRANCH_NBR, 
          t1.TITLE_LOAN_NBR, 
          t1.PREV_TITLE_LOAN_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.TOTAL_DUE, 
          t1.RENEWAL_DATE, 
          t1.RENEWAL_CHARGE, 
          t1.LOAN_STATUS, 
          t1.TITLE_STATUS, 
          t1.DATE_UPDATED, 
          t1.UPDATED_BY, 
          t1.DATE_CREATED, 
          t1.CREATED_BY, 
          t1.REPO_FEE_BALANCE, 
          t1.ROUGH_VALUE, 
          t1.AVERAGE_VALUE, 
          t1.CLEAN_VALUE, 
          t1.PERCENT_USED, 
          t1.EXTRA_CLEAN_VALUE, 
          t1.VALUE_USED, 
          t1.PAYEE_NAME, 
          t1.PAYEE_DATE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.PWO_DATE, 
          t1.PWO_AMT, 
          t1.FINANCE_ADVANCE_VALUE, 
          t1.BEGINDT, 
          t1.ENDDT, 
          t1.DEAL_DT
      FROM WORK.ECA_TITLE_DS_PRE t1
           LEFT JOIN WORK.MOST_RECENT_CUSTNBR t2 ON (t1.LOAN_NBR = t2.LOAN_NBR);
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_ECA_TITLE_DS AS 
   SELECT /* PRODUCT */
            ("TITLE") AS PRODUCT, 
          /* POS */
            ("QFUND") AS POS, 
          /* INSTANCE */
            ("QFUND4") AS INSTANCE, 
          /* BANKMODEL */
            ("STANDARD") AS BANKMODEL, 
          /* LOCNBR */
            (CASE WHEN LENGTH(COMPRESS(PUT(CASE WHEN t2.LOCATION_AA = . THEN t1.BRANCH_NBR ELSE t2.LOCATION_AA 
            END,8.))) = 5 THEN (CASE WHEN t2.LOCATION_AA = . THEN t1.BRANCH_NBR ELSE t2.LOCATION_AA END)/100 ELSE (CASE 
            WHEN t2.LOCATION_AA = . THEN t1.BRANCH_NBR ELSE t2.LOCATION_AA END) END) AS LOCNBR, 
          /* DEAL_DT */
            (DATEPART(T1.LOAN_DATE)) FORMAT=MMDDYY10. AS DEAL_DT, 
          t1.LOAN_DATE AS DEAL_DTTM, 
          t1.LOAN_NBR AS DEALNBR, 
          t1.TITLE_LOAN_NBR, 
          t1.CUSTOMER_NBR AS CUSTNBR, 
          t4.TOTAL_DUE AS TOTAL_OWED, 
          t1.LOAN_STATUS AS DEALSTATUSCD, 
          t1.DUE_DATE AS DUEDT, 
          t1.DATE_UPDATED AS UPDATE_DT
      FROM WORK.ECA_TITLE_DS t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
           LEFT JOIN WORK.MOST_RECENT_BALANCE t4 ON (t1.LOAN_NBR = t4.LOAN_NBR) AND (t1.TITLE_LOAN_NBR = 
          t4.TITLE_LOAN_NBR)
      WHERE (CALCULATED DEAL_DT) BETWEEN t4.BEGINDT AND t4.ENDDT;
%RUNQUIT(&job,&sub8);


PROC SQL;
   CREATE TABLE WORK.QF4_DEALSUMMARY_TITLE_pre AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t3.BRND_CD AS BRANDCD, 
          t1.BANKMODEL, 
          t3.CTRY_CD AS COUNTRYCD, 
          t3.ST_PVC_CD AS STATE, 
          t3.ADR_CITY_NM AS CITY, 
          t3.MAIL_CD AS ZIP, 
          t3.HIER_ZONE_NBR AS ZONENBR, 
          t3.HIER_ZONE_NM AS ZONENAME, 
          t3.HIER_RGN_NBR AS REGIONNBR, 
          t3.HIER_RDO_NM AS REGIONRDO, 
          t3.HIER_DIV_NBR AS DIVISIONNBR, 
          t3.HIER_DDO_NM AS DIVISIONDDO, 
          t3.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t1.LOCNBR, 
          t3.OPEN_DT AS LOC_OPEN_DT, 
          t3.CLS_DT AS LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.TITLE_LOAN_NBR, 
          t1.CUSTNBR, 
          t1.TOTAL_OWED AS TOTALOWED, 
          t1.DEALSTATUSCD, 
          t2.SSN, 
          t1.DUEDT, 
          t1.UPDATE_DT
      FROM WORK.QUERY_FOR_ECA_TITLE_DS t1
           LEFT JOIN ECA.QF_CUSTOMER_DETAILS t2 ON (t1.CUSTNBR = t2.CUSTOMER_NBR)
           LEFT JOIN EDW.D_LOCATION t3 ON (t1.LOCNBR = t3.LOC_NBR);
%RUNQUIT(&job,&sub8);

PROC SQL;
   CREATE TABLE QF4_TLP_DAILY_UPDATE AS 
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
          t1.LOC_CLOSE_DT, 
          t1.LOC_OPEN_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          t1.TITLE_LOAN_NBR AS TITLE_DEALNBR, 
          t1.SSN, 
          t2.PRINCIPAL_AMT AS ADVAMT, 
          t2.BAL_LIEN_FEE_AMT AS FEEAMT, 
          /* NSFFEEAMT */
            (.) AS NSFFEEAMT, 
          /* LATEFEEAMT */
            (.) AS LATEFEEAMT, 
          /* OTHERFEEAMT */
            (.) AS OTHERFEEAMT, 
          /* WAIVEDFEEAMT */
            (.) AS WAIVEDFEEAMT, 
          /* REBATEAMT */
            (.) AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t6.SUM_of_TRANSACTION_AMT AS TOTALPAID, 
          t1.TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* CASHAGNCNT */
            (.) AS CASHAGNCNT, 
          t1.DUEDT, 
          /* DEALENDDT */
            (CASE WHEN t5.TOTAL_AMOUNT_DUE = 0 THEN t5.TRANSACTION_DATE ELSE . END) FORMAT=DATETIME20. AS DEALENDDT, 
          /* DEPOSITDT */
            (''DT) FORMAT=DATETIME20. AS DEPOSITDT, 
          t4.WRITEOFFDT, 
          t3.DEFAULTDT, 
          /* ACHSTATUSCD */
            ('') AS ACHSTATUSCD, 
          /* CHECKSTATUSCD */
            ('') AS CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            ("TITLE") AS COLLATERAL_TYPE, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          /* ETLDT */
            (''DT) FORMAT=DATETIME20. AS ETLDT, 
          /* PRODUCTCD */
            ("TLP") AS PRODUCTCD, 
          /* INTERESTFEE */
            (sum(t2.BAL_INT_AMOUNT,t6.INT_CHARGED)) AS INTERESTFEE, 
          /* ACHAUTHFLG */
            ('') AS ACHAUTHFLG, 
          t1.UPDATE_DT
      FROM WORK.QF4_DEALSUMMARY_TITLE_PRE t1
           LEFT JOIN WORK.ADVAMT2 t2 ON (t1.TITLE_LOAN_NBR = t2.TITLE_LOAN_NBR)
           LEFT JOIN WORK.WRITEOFF_LOANS t4 ON (t1.TITLE_LOAN_NBR = t4.TITLE_LOAN_NBR)
           LEFT JOIN WORK.DEFAULT_LOANS t3 ON (t1.TITLE_LOAN_NBR = t3.TITLE_LOAN_NBR)
           LEFT JOIN WORK.MOST_RECENT_TXN_TLP t5 ON (t1.TITLE_LOAN_NBR = t5.TITLE_LOAN_NBR)
           LEFT JOIN WORK.TLP_PAID t6 ON (t1.TITLE_LOAN_NBR = t6.TITLE_LOAN_NBR) AND (t1.DEALNBR = t6.LOAN_NBR)
      ORDER BY t1.LOCNBR,
               t1.DEALNBR,
               t1.TITLE_LOAN_NBR;
%RUNQUIT(&job,&sub8);

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
          t1.LOC_CLOSE_DT, 
          t1.LOC_OPEN_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          t1.TITLE_DEALNBR, 
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
          t1.PREVDEALNBR, 
          t1.ETLDT, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATE_DT AS UPDATEDT,
		  . AS OUTSTANDING_DRAW_AMT,
		 '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM QF4_TLP_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub8);

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
%RUNQUIT(&job,&sub8);

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
RUN;

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND4",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND4\DIR2\",'G');
%RUNQUIT(&job,&sub8);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF4TLP (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE
	WHERE STATE NOT IN ('TN');
%RUNQUIT(&job,&sub8);

/*UPLOAD QF4TLP*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF4TLP.SAS";
