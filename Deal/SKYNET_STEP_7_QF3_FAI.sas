%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub7);

LIBNAME QFUND3 ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH=EDWPRD
	SCHEMA=QFUND3 DEFER=YES;

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";

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
%RUNQUIT(&job,&sub7);

PROC SQL;
   CREATE TABLE WORK.LOAN_SUMMARY_SORT AS 
   SELECT t1.LOAN_ID, 
          t1.LOAN_CODE, 
          t1.BO_CODE, 
          t1.ST_CODE, 
          t1.LOAN_DATE, 
          t1.LOAN_AMT, 
          t1.CSO_FEE, 
          t1.INTEREST, 
          t1.INTEREST_RATE, 
          t1.APR, 
          t1.NO_OF_INSTALLMENTS, 
          t1.DISB_TYPE, 
          t1.LOAN_STATUS_ID, 
          t1.DEF_NSF_COUNT, 
          t1.CSO_RTN_FEE_AMT, 
          t1.CSO_RTN_FEE_AMT_PAID, 
          t1.WAIVED_RTN_FEE_AMT, 
          t1.TOTAL_DUE, 
          t1.TOTAL_PAID, 
          t1.LATEST_PAYMENT_DATE, 
          t1.BALANCE_STATUS_ID, 
          t1.LOAN_END_DATE, 
          t1.DEFAULT_DATE, 
          t1.WO_DATE, 
          t1.LENDER_COLLATERAL_TYPE, 
          t1.ACH_OPT_OUT, 
          t1.ABA_CODE, 
          t1.BANK_ACNT_NUM, 
          t1.PRODUCT_TYPE, 
          t1.DATE_CREATED, 
          t1.CREATED_BY, 
          t1.DATE_UPDATED, 
          t1.UPDATED_BY, 
          t1.LENDER_NSF_COUNT, 
          t1.LENDER_NSF_FEE_PAID, 
          t1.LENDER_NSF_FEE, 
          t1.CSO_FEE_DAILY_RATE, 
          t1.CSO_COLLATERAL_TYPE, 
          t1.EEE_REQUEST_ID, 
          t1.LENDER_REQUEST_ID, 
          t1.LATE_FEE, 
          t1.LATE_FEE_PAID, 
          t1.ORIG_BANK_ACNT_NBR, 
          t1.ORIG_ABA_NBR, 
          t1.PREV_LOAN_NUM, 
          t1.ROLL_OVER_COUNT, 
          t1.EFFECTIVE_BEGIN_DT, 
          t1.EFFECTIVE_END_DT, 
          t1.ETL_DT, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.ACTIVE_FLG, 
          t1.DMV_FEE, 
          t1.DMV_FEE_PAID, 
          t1.TITLE_CYCLE, 
          t1.BLACKBOOK_VALUE, 
          t1.TITLE_TRACKING_ID, 
          t1.VEHICLE_STATUS, 
          t1.CALLOFF_DATE, 
          t1.CALLOFF_FEE_AMT, 
          t1.CALLOFF_FEE_PAID, 
          t1.REPO_DATE, 
          t1.REPOSSESSION_FEE, 
          t1.REPO_CHARGE_PAID, 
          t1.AUCTION_DATE, 
          t1.SALVAGE_DATE, 
          t1.SALE_DATE, 
          t1.SOLD_AMT, 
          t1.VEHICLE_SETTLEMENT_AMT, 
          t1.REPO_COMPANY, 
          t1.CALLOFF_COMPANY, 
          t1.SALVAGE_COMPANY, 
          t1.AUCTION_COMPANY
      FROM TETL.LOAN_SUMMARY t1
      WHERE t1.DATE_UPDATED >= &lastweek
      ORDER BY t1.LOAN_CODE,
               t1.LOAN_ID,
               t1.LOAN_DATE;
%RUNQUIT(&job,&sub7);

DATA TETL_LOAN_SUMMARY;
	SET WORK.LOAN_SUMMARY_SORT;
	BY LOAN_CODE LOAN_ID;
	IF LAST.LOAN_CODE;
%RUNQUIT(&job,&sub7);

PROC SQL;
   CREATE TABLE WORK.TETL_PRE_LAYOUT AS 
   SELECT /* PRODUCT */
            ("INSTALLMENT") AS PRODUCT, 
          /* POS */
            ("QFUND") AS POS, 
          /* INSTANCE */
            ("QFUND3") AS INSTANCE, 
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
          t2.LOC_NBR AS LOCNBR, 
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          t1.LOAN_DATE AS DEAL_DTTM, 
          /* BEGINDT */
            (INTNX('MONTH',TODAY(),-60,'BEGINNING')) FORMAT=MMDDYY10. AS BEGINDT, 
          /* DEAL_DT */
            (DATEPART(t1.LOAN_DATE)) FORMAT=MMDDYY10. AS DEAL_DT, 
          t1.LOAN_CODE AS DEALNBR, 
          t1.BO_CODE AS CUSTNBR, 
          t1.LOAN_AMT AS ADVAMT, 
          t1.CSO_FEE AS FEEAMT, 
          t1.INTEREST AS INTERESTFEE, 
          t1.LENDER_NSF_FEE AS NSFFEEAMT, 
          t1.LATE_FEE AS LATEFEEAMT, 
          /* OTHERFEEAMT */
            (SUM(t1.REPOSSESSION_FEE,t1.DMV_FEE,t1.CSO_RTN_FEE_AMT)) AS OTHERFEEAMT, 
          t1.WAIVED_RTN_FEE_AMT AS WAIVEDFEEAMT, 
          /* REBATEAMT */
            (.) AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t1.TOTAL_DUE AS TOTALOWED, 
          t1.TOTAL_PAID AS TOTALPAID, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* CASHAGAINCNT */
            (.) AS CASHAGAINCNT, 
          t1.LOAN_END_DATE AS DEALENDDT, 
          /* DEPOSITDT */
            (""DT) FORMAT=DATETIME20. AS DEPOSITDT, 
          t1.WO_DATE AS WRITEOFFDT, 
          t1.DEFAULT_DATE AS DEFAULTDT, 
          /* ACHSTATUS */
            ("") AS ACHSTATUS, 
          /* CHECKSTATUSCD */
            ("") AS CHECKSTATUSCD, 
          /* COLLATERAL_TYPE */
            (CASE WHEN t1.LENDER_COLLATERAL_TYPE = 'CK' THEN 'CHECK'
                       ELSE "UNKNOWN"
            END) AS COLLATERAL_TYPE, 
          t1.LOAN_STATUS_ID AS DEALSTATUSCD, 
          t1.ETL_DT AS ETLDT, 
          t1.PREV_LOAN_NUM AS PREVDEALNBR, 
          t1.PRODUCT_TYPE AS PRODUCTCD, 
          /* ACHAUTHFLG */
            (CASE WHEN t1.ACH_OPT_OUT = "Y" THEN "N"
                      WHEN t1.ACH_OPT_OUT = "N" THEN "Y"
                       ELSE ""
            END) AS ACHAUTHFLG, 
          t1.DATE_UPDATED AS UPDATEDT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'BEGINNING')) FORMAT=MMDDYY10. AS ENDDT
      FROM WORK.TETL_LOAN_SUMMARY t1
           INNER JOIN EDW.D_LOCATION t2 ON (t1.ST_CODE = t2.LOC_NBR)
      WHERE t1.PRODUCT_TYPE = 'FAI' AND t2.ST_PVC_CD NOT IS MISSING;
%RUNQUIT(&job,&sub7);

PROC SQL;
   CREATE TABLE WORK.SORT_FOR_MOST_RECENT_ENTRY AS 
   SELECT t1.BO_ID, 
          t1.BO_CODE, 
          t1.BO_ST_CODE, 
          t1.BUSINESS_UNIT, 
          t1.SSN, 
          t1.SUFFIX, 
          t1.TITLE, 
          t1.FIRST_NAME, 
          t1.LAST_NAME, 
          t1.MIDDLE_NAME, 
          t1.DOB, 
          t1.BO_STATUS_ID, 
          t1.IS_BANKRUPT, 
          t1.IS_DECEASED, 
          t1.TYPE_OF_BANKRUPTCY, 
          t1.BANKRUPTCY_DATE, 
          t1.ATTORNEY_NAME, 
          t1.PENDING_BANKRUPTCY, 
          t1.PENDING_BNK_DATE, 
          t1.IS_LEGAL, 
          t1.PHOTO_ID, 
          t1.PHOTO_ID_NUM, 
          t1.PHOTO_ID_STATE, 
          t1.ADD_LINE1, 
          t1.ADD_LINE2, 
          t1.CITY, 
          t1.STATE, 
          t1.ZIPCODE, 
          t1.DO_NOT_MAIL, 
          t1.RESIDENCE_TYPE, 
          t1.MONTHS_AT_ADDRESS, 
          t1.PRIMARY_PHONE_NUMBER, 
          t1.PRIMARY_PHONE_TYPE, 
          t1.PRIMARY_PHONE_DONT_CONTACT, 
          t1.SECONDARY_PHONE_NUMBER, 
          t1.SECONDARY_PHONE_TYPE, 
          t1.SECONDARY_PHONE_DONT_CONTACT, 
          t1.ALTERNATE_PHONE_NUMBER, 
          t1.ALTERNATE_PHONE_TYPE, 
          t1.ALTERNATE_PHONE_DONT_CONTACT, 
          t1.EMAIL_ID, 
          t1.CUST_BANK_ACNT_TYPE, 
          t1.CUST_BANK_ABA_NUMBER, 
          t1.CUST_BANK_ACNT_NUMBER, 
          t1.STMT_END_BALANCE, 
          t1.IS_DIRECT_DEP, 
          t1.IS_ACTIVE_MILITARY, 
          t1.CONTACT_FIRST_NAME, 
          t1.CONTACT_LAST_NAME, 
          t1.CONTACT_RELATION, 
          t1.CONTACT_PHON_NUM, 
          t1.CONTACT_DONT_CONTACT, 
          t1.REF1_FIRST_NAME, 
          t1.REF1_LAST_NAME, 
          t1.REF1_RELATION, 
          t1.REF1_PHONE_NUM, 
          t1.REF1_DONT_CONTACT, 
          t1.REF2_FIRST_NAME, 
          t1.REF2_LAST_NAME, 
          t1.REF2_RELATION, 
          t1.REF2_PHONE_NUM, 
          t1.REF2_DONT_CONTACT, 
          t1.REF3_FIRST_NAME, 
          t1.REF3_LAST_NAME, 
          t1.REF3_RELATION, 
          t1.REF3_PHONE_NUM, 
          t1.REF3_DONT_CONTACT, 
          t1.REFERRAL_CODE, 
          t1.MARKETING_OUTPUT, 
          t1.DATE_CREATED, 
          t1.CREATED_BY, 
          t1.DATE_UPDATED, 
          t1.UPDATED_BY, 
          t1.IS_TITLE_CUSTOMER, 
          t1.IS_ETL_CUSTOMER, 
          t1.EFFECTIVE_BEGIN_DT, 
          t1.EFFECTIVE_END_DT, 
          t1.ETL_DT, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.ACTIVE_FLG, 
          t1.IS_TTOC_CUSTOMER
      FROM TETL.CUSTOMER t1
      ORDER BY t1.BO_CODE,
               t1.BO_ID,
               t1.DATE_UPDATED;
%RUNQUIT(&job,&sub7);

DATA WORK.TETL_CUST;
	SET WORK.SORT_FOR_MOST_RECENT_ENTRY;
	BY BO_CODE BO_ID;
	IF LAST.BO_CODE;
%RUNQUIT(&job,&sub7);

PROC SQL;
   CREATE TABLE WORK.TETL_DUEDTS AS 
   SELECT t1.LOAN_CODE, 
          /* DUEDT */
            (MAX(t1.INST_DUE_DATE)) FORMAT=DATETIME20. AS DUEDT
      FROM QFUND3.LOAN_SCHEDULE t1
      WHERE t1.ACTIVE_FLG = 'Y'
      GROUP BY t1.LOAN_CODE;
%RUNQUIT(&job,&sub7);

PROC SQL;
   CREATE TABLE QF3_FAI_DAILY_UPDATE AS 
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
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEAL_DT, 
          t1.DEALNBR, 
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
          t1.DEALENDDT, 
          t3.DUEDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUS, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.TETL_PRE_LAYOUT t1
           INNER JOIN WORK.TETL_CUST t2 ON (t1.CUSTNBR = t2.BO_CODE)
           LEFT JOIN WORK.TETL_DUEDTS t3 ON (t1.DEALNBR = t3.LOAN_CODE);
%RUNQUIT(&job,&sub7);

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
		  .					AS TITLE_DEALNBR,
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
          t1.DEFAULTDT, 
          t1.ACHSTATUS AS ACHSTATUSCD, 
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
      FROM QF3_FAI_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub7);

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
%RUNQUIT(&job,&sub7);

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
%RUNQUIT(&job,&sub7);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND3\FAI",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND3\FAI\DIR2\",'G');
%RUNQUIT(&job,&sub7);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF3FAI (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE;
%RUNQUIT(&job,&sub7);

/*UPLOAD QF3FAI*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF3FAI.SAS";

