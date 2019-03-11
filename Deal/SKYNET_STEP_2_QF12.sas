%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub2);

LIBNAME EDW_STAR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=EDW_STAR DEFER=YES;
*QFUND2;
LIBNAME STG ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=STG_QFUND_VS DEFER=YES;

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
%RUNQUIT(&job,&sub2);

PROC SQL;
	CREATE TABLE DUEDT AS
		SELECT LOAN.LOAN_NBR
		       ,MAX(CAL.CAL_DAY_DT) AS DUEDT
			   ,MIN(CAL.CAL_DAY_DT) AS FIRST_DUEDT
		FROM EDW.LOANSCHEDULE LOAN_SCHED
	    INNER JOIN EDW.LOAN LOAN
	        ON (LOAN_SCHED.LOAN_ID = LOAN.LOAN_ID)
	    INNER JOIN EDW.CAL_DAY  CAL
	        ON (LOAN_SCHED.CAL_DAY_ID = CAL.CAL_DAY_ID)
		GROUP BY LOAN.LOAN_NBR
;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.ENDED_DEALS AS 
   SELECT T1.LOAN_SNAP_DT, 
          T1.LOAN_NBR, 
          T1.CUST_NBR, 
          T1.FNCL_STAT_TXT
      FROM EDW_ST.LOANDAILYSNAPSHOT T1
      WHERE T1.FNCL_STAT_TXT = 'REPAID' AND T1.LOAN_SNAP_DT >= &LASTWEEK AND T1.ST_CD NOT = 'CO'
      ORDER BY T1.LOAN_NBR,
               T1.LOAN_SNAP_DT;
%RUNQUIT(&job,&sub2);

DATA WORK.LASTREPAYMENT;
	SET WORK.ENDED_DEALS;
	BY LOAN_NBR;
	IF LAST.LOAN_NBR;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.LAST_WEEK_OF_UPDATES AS 
   SELECT t1.LOAN_SNAP_DT, 
          t1.ST_CD, 
          t1.LOC_NBR, 
          t1.LOAN_NBR, 
          t1.CUST_NBR, 
          t1.PROD_CD, 
          t1.FNCL_STAT_TXT, 
          t1.DAYS_DLNQT_QTY, 
          t1.INSTL_LOAN_CNT, 
          t1.INSTL_PYMNT_FREQ_CD, 
          t1.ORIG_DT, 
          t1.DFLT_DT, 
          t1.WO_AGE_DT, 
          t1.WO_BNKRPT_DT, 
          t1.WO_DCSD_DT, 
/* Haritha - 28Feb2018 : Populating all kinds of writeoff dates into one */
		  case 
			when t1.FNCL_STAT_TXT = 'WRITE OFF (AGE)' then t1.WO_AGE_DT
			when t1.FNCL_STAT_TXT = 'WRITE OFF (BANKRUPT)'	then t1.WO_BNKRPT_DT
			when t1.FNCL_STAT_TXT = 'WRITE OFF (DECEASED)' then t1.WO_DCSD_DT
		  end as WO_DT format = datetime.,
          t1.FRST_PRSNTMT_DT, 
          t1.RCNT_PRSNTMT_DT, 
          t1.FRST_RTN_DT, 
          t1.RCNT_RTN_DT, 
          t1.FRST_REPRSNTMT_DT, 
          t1.RCNT_REPRSNTMT_DT, 
          t1.REPAID_DT, 
          t1.TTL_PAID_AMT, 
          t1.CRRNT_BLNC_AMT, 
          t1.ORIG_PRNC_AMT, 
          t1.PRNC_PAID_AMT, 
          t1.PRNC_BLNC_AMT, 
          t1.INT_CHRG_AMT, 
          t1.INT_PAID_AMT, 
          t1.SDB_FEE_CHRG_AMT, 
          t1.SDB_FEE_PAID_AMT, 
          t1.SDB_FEE_BLNC_AMT, 
          t1.NSF_FEE_CHRG_AMT, 
          t1.NSF_FEE_PAID_AMT, 
          t1.NSF_FEE_WVD_AMT, 
          t1.NSF_FEE_BLNC_AMT, 
          t1.INT_BLNC_AMT, 
          t1.INT_RBT_AMT, 
          t1.MTH_HNDL_FEE_CHRG_AMT, 
          t1.MTH_HNDL_FEE_PAID_AMT, 
          t1.MTH_HNDL_FEE_RBT_AMT, 
          t1.MTH_HNDL_FEE_BLNC_AMT, 
          t1.ACQSN_FEE_CHRG_AMT, 
          t1.ACQSN_FEE_PAID_AMT, 
          t1.ACQSN_FEE_RBT_AMT, 
          t1.ACQSN_FEE_BLNC_AMT, 
          t1.REFI_DATE, 
          t1.PRIOR_REFI_LOAN, 
          t1.SUBSQ_REFI_LOAN, 
          t1.LATE_FEE_CHRG_AMT, 
          t1.LATE_FEE_PAID_AMT, 
          t1.LATE_FEE_WVD_AMT
      FROM EDW_ST.LOANDAILYSNAPSHOT t1
      WHERE t1.LOAN_SNAP_DT >= &lastweek AND t1.ST_CD NOT = 'CO';
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.MOST_RECENT_DEALS AS 
   SELECT t1.LOAN_NBR, 
          /* LOAN_SNAP_DT */
            (MAX(t1.LOAN_SNAP_DT)) FORMAT=DATETIME20. AS LOAN_SNAP_DT
      FROM WORK.LAST_WEEK_OF_UPDATES t1
      GROUP BY t1.LOAN_NBR;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.DAILY_UPDATES AS 
   SELECT /* PRODUCT */
            ("INSTALLMENT") AS PRODUCT, 
          /* PRODUCT_DESC */
            ("") LABEL="PRODUCT_DESC" AS PRODUCT_DESC, 
          /* POS */
            ("QFUND") AS POS, 
          /* INSTANCE */
            (case 
              when t2.ST_CD = "CO" then "QFUND2"
              else "QFUND1"
            end
              ) AS INSTANCE, 
          t3.BRND_CD AS BRANDCD, 
          /* BANKMODEL */
            ("STANDARD") AS BANKMODEL, 
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
          t2.LOC_NBR AS LOCNBR, 
          t3.LOC_NM AS LOCATION_NAME, 
          t3.OPEN_DT AS LOC_OPEN_DT, 
          t3.CLS_DT AS LOC_CLOSE_DT, 
          /* DEAL_DT */
            (datepart(t2.ORIG_DT)) FORMAT=mmddyy10. AS DEAL_DT, 
          t2.ORIG_DT AS DEAL_DTTM, 
          /* BEGINDT */
            (intnx('year',today(),-6,'BEGINNING')) FORMAT=mmddyy10. LABEL="BEGINDT" AS BEGINDT, 
          t2.LOAN_NBR AS DEALNBR, 
          t2.CUST_NBR AS CUSTNBR, 
          t2.ORIG_PRNC_AMT AS ADVAMT, 
          /* FEEAMT */
            (.) AS FEEAMT, 
          t2.NSF_FEE_CHRG_AMT AS NSFFEEAMT, 
          /* OTHERFEEAMT */
            (sum(t2.MTH_HNDL_FEE_CHRG_AMT,t2.ACQSN_FEE_CHRG_AMT,t2.SDB_FEE_CHRG_AMT)) AS OTHERFEEAMT, 
          t2.LATE_FEE_CHRG_AMT AS LATEFEEAMT, 
          /* WAIVEDFEEAMT */
            (SUM(t2.NSF_FEE_WVD_AMT,t2.LATE_FEE_WVD_AMT)) AS WAIVEDFEEAMT, 
          t2.INT_RBT_AMT AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t2.TTL_PAID_AMT AS TOTALPAID, 
          t2.CRRNT_BLNC_AMT AS TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* REFINANCECNT */
            (.) AS REFINANCECNT, 
          /* CASHAGNCNT */
            (.) AS CASHAGNCNT, 
          t2.INT_CHRG_AMT AS INTERESTFEE, 
          t2.FRST_PRSNTMT_DT AS DEPOSITDT, 
          t5.FIRST_DUEDT, 
          t5.DUEDT, 
/* Haritha - 28Feb2018 : Using the newly calculated writeoff column to avoid missing values */

/*          t2.WO_AGE_DT AS WRITEOFFDT, */
		  t2.WO_DT AS WRITEOFFDT,
          /* ACHSTATUSCD */
            ('') AS ACHSTATUSCD, 
          /* DEALSTATUSCD */
            (CASE WHEN t2.FNCL_STAT_TXT  = 'OVERPAID' THEN 'CLO'
                      WHEN t2.FNCL_STAT_TXT = 'DEFAULT' THEN 'DEF'
                      WHEN t2.FNCL_STAT_TXT = 'DEFAULT W/NSF' THEN 'DEF'
                      WHEN t2.FNCL_STAT_TXT = 'NSF' THEN 'OPN'
                      WHEN t2.FNCL_STAT_TXT = 'CURRENT' THEN 'OPN'
                      WHEN t2.FNCL_STAT_TXT = 'CURRENT IN FLIGHT' THEN 'OPN'
                      WHEN t2.FNCL_STAT_TXT = 'WRITE OFF (AGE)' THEN 'WO'
                      WHEN t2.FNCL_STAT_TXT = 'WRITE OFF (BANKRUPT)' THEN 'WO'
                      WHEN t2.FNCL_STAT_TXT = 'WRITE OFF (DECEASED)' THEN 'WO'
                      WHEN t2.FNCL_STAT_TXT = 'VOID/RESCIND' THEN 'V'
                      WHEN t2.FNCL_STAT_TXT = 'REPAID' THEN 'CLO'
            ELSE t2.FNCL_STAT_TXT END) AS DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            (CASE WHEN t4.CLTRL_TYPE_CD = "CHK" THEN "CHECK"
                       WHEN t4.CLTRL_TYPE_CD = "ACH" THEN "ACH"
                       WHEN t4.CLTRL_TYPE_CD = "UNK" THEN "UNKNOWN"
                       ELSE "UNKNOWN"
            END) AS COLLATERAL_TYPE, 
          t2.DFLT_DT AS DEFAULTDT, 
          /* CHECKSTATUSCD */
            (case
              when t2.FNCL_STAT_TXT = 'CURRENT' then 'HLD' 
              when t2.FNCL_STAT_TXT = 'CURRENT IN FLIGHT' then 'DEP'
              when t2.FNCL_STAT_TXT = 'DEFAULT' or t2.FNCL_STAT_TXT = 'DEFAULT W/NSF' or t2.FNCL_STAT_TXT = 'NSF' then 
            'RTN'
              when t2.FNCL_STAT_TXT = 'OVERPAID' or t2.FNCL_STAT_TXT = 'REPAID' then 'BGT'
              when t2.FNCL_STAT_TXT = 'WRITE OFF (AGE)' or t2.FNCL_STAT_TXT = 'WRITE OFF (BANKRUPT)' or 
            t2.FNCL_STAT_TXT = 'WRITE OFF (DECASED)' then 'WO'
            end) AS CHECKSTATUSCD, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          /* ETLDT */
            (''D) FORMAT=MMDDYY10. LABEL="ETLDT" AS ETLDT, 
          t2.PROD_CD AS PRODUCTCD, 
          /* ACHAUTHFLG */
            ('') AS ACHAUTHFLG, 
          /* UPDATEDT */
            (t1.LOAN_SNAP_DT) FORMAT=datetime20. LABEL="UPDATEDT" AS UPDATEDT, 
          /* ENDDT */
            (intnx('day',today(),-1,'BEGINNING')) FORMAT=MMDDYY10. AS ENDDT, 
          t1.LOAN_SNAP_DT
      FROM EDW.D_LOCATION t3
           RIGHT JOIN (WORK.MOST_RECENT_DEALS t1
           INNER JOIN WORK.LAST_WEEK_OF_UPDATES t2 ON (t1.LOAN_SNAP_DT = t2.LOAN_SNAP_DT) AND (t1.LOAN_NBR = 
          t2.LOAN_NBR)) ON (t3.LOC_NBR = t2.LOC_NBR)
           LEFT JOIN WORK.DUEDT t5 ON (t2.LOAN_NBR = t5.LOAN_NBR)
           LEFT JOIN EDW.LOAN t4 ON (t2.LOAN_NBR = t4.LOAN_NBR)
      WHERE (CALCULATED DEAL_DT) BETWEEN (CALCULATED BEGINDT) AND (CALCULATED ENDDT);
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.LAST_WEEK_OF_UPDATES_CO AS 
   SELECT t1.LOAN_SNAP_DT, 
          t1.ST_CD, 
          t1.LOC_NBR, 
          t1.LOAN_NBR, 
          t1.CUST_NBR, 
          t1.PROD_CD, 
          t1.FNCL_STAT_TXT, 
          t1.DAYS_DLNQT_QTY, 
          t1.INSTL_LOAN_CNT, 
          t1.INSTL_PYMNT_FREQ_CD, 
          t1.ORIG_DT, 
          t1.DFLT_DT, 
          t1.WO_AGE_DT, 
          t1.WO_BNKRPT_DT, 
          t1.WO_DCSD_DT, 
/* Haritha - 28Feb2018 : Populating all kinds of writeoff dates into one */
		  case 
			when t1.FNCL_STAT_TXT = 'WRITE OFF (AGE)' then t1.WO_AGE_DT
			when t1.FNCL_STAT_TXT = 'WRITE OFF (BANKRUPT)'	then t1.WO_BNKRPT_DT
			when t1.FNCL_STAT_TXT = 'WRITE OFF (DECEASED)' then t1.WO_DCSD_DT
		  end as WO_DT format = datetime.,
          t1.FRST_PRSNTMT_DT, 
          t1.RCNT_PRSNTMT_DT, 
          t1.FRST_RTN_DT, 
          t1.RCNT_RTN_DT, 
          t1.FRST_REPRSNTMT_DT, 
          t1.RCNT_REPRSNTMT_DT, 
          t1.REPAID_DT, 
          t1.TTL_PAID_AMT, 
          t1.CRRNT_BLNC_AMT, 
          t1.ORIG_PRNC_AMT, 
          t1.PRNC_PAID_AMT, 
          t1.PRNC_BLNC_AMT, 
          t1.INT_CHRG_AMT, 
          t1.INT_PAID_AMT, 
          t1.SDB_FEE_CHRG_AMT, 
          t1.SDB_FEE_PAID_AMT, 
          t1.SDB_FEE_BLNC_AMT, 
          t1.NSF_FEE_CHRG_AMT, 
          t1.NSF_FEE_PAID_AMT, 
          t1.NSF_FEE_WVD_AMT, 
          t1.NSF_FEE_BLNC_AMT, 
          t1.INT_BLNC_AMT, 
          t1.INT_RBT_AMT, 
          t1.MTH_HNDL_FEE_CHRG_AMT, 
          t1.MTH_HNDL_FEE_PAID_AMT, 
          t1.MTH_HNDL_FEE_RBT_AMT, 
          t1.MTH_HNDL_FEE_BLNC_AMT, 
          t1.ACQSN_FEE_CHRG_AMT, 
          t1.ACQSN_FEE_PAID_AMT, 
          t1.ACQSN_FEE_RBT_AMT, 
          t1.ACQSN_FEE_BLNC_AMT, 
          t1.REFI_DATE, 
          t1.PRIOR_REFI_LOAN, 
          t1.SUBSQ_REFI_LOAN, 
          t1.LATE_FEE_CHRG_AMT, 
          t1.LATE_FEE_PAID_AMT, 
          t1.LATE_FEE_WVD_AMT
      FROM EDW_ST.LOANDAILYSNAPSHOT t1
      WHERE t1.LOAN_SNAP_DT >= &lastweek AND t1.ST_CD = 'CO';
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.MOST_RECENT_DEALS_CO AS 
   SELECT t1.LOAN_NBR, 
          /* LOAN_SNAP_DT */
            (MAX(t1.LOAN_SNAP_DT)) FORMAT=DATETIME20. AS LOAN_SNAP_DT
      FROM WORK.LAST_WEEK_OF_UPDATES_CO t1
      GROUP BY t1.LOAN_NBR;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.ENDED_DEALS_CO AS 
   SELECT t1.LOAN_SNAP_DT, 
          t1.LOAN_NBR, 
          t1.CUST_NBR, 
          t1.FNCL_STAT_TXT
      FROM EDW_ST.LOANDAILYSNAPSHOT t1
      WHERE t1.FNCL_STAT_TXT = 'REPAID' AND t1.LOAN_SNAP_DT >= &lastweek AND t1.ST_CD = 'CO'
      ORDER BY t1.LOAN_NBR,
               t1.LOAN_SNAP_DT;
%RUNQUIT(&job,&sub2);

DATA WORK.LASTREPAYMENT_CO;
	SET WORK.ENDED_DEALS_CO;
	BY LOAN_NBR;
	IF LAST.LOAN_NBR;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.QF1_QF2_CUST_SSN AS 
   SELECT DISTINCT t1.CUSTOMER_NBR AS CUSTNBR, 
          /* SOURCESYSTEM */
            (case 
              when t1.SOURCE_SYSTEM = "QF1" then "QFUND1" 
              when t1.SOURCE_SYSTEM = "QF2" then "QFUND2" else "" end
            ) LABEL="SOURCESYSTEM" AS SOURCESYSTEM, 
          t1.SOURCE_SYSTEM, 
          t1.SSN
      FROM EDW_STAR.CUSTOMER_DIM t1
      WHERE t1.EFFECTIVE_END_DT = '31DEC9999:00:00:00'dt AND (CALCULATED SOURCESYSTEM) NOT IS MISSING;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.QF2_DUEDTS AS 
   SELECT /* DEALNBR */
            (INPUT(t1.ILOAN_CODE, BEST32.)) AS DEALNBR, 
          /* DUEDT */
            (MAX(t1.INST_DUE_DATE)) FORMAT=DATETIME20. AS DUEDT, 
          /* FIRST_DUEDT */
            (MIN(t1.INST_DUE_DATE)) FORMAT=DATETIME20. AS FIRST_DUEDT
      FROM STG.TBL_SIL_SCHEDULE t1
      GROUP BY (CALCULATED DEALNBR);
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.DAILY_UPDATES_CO AS 
   SELECT /* PRODUCT */
            ("INSTALLMENT") AS PRODUCT, 
          /* PRODUCT_DESC */
            ("") LABEL="PRODUCT_DESC" AS PRODUCT_DESC, 
          /* POS */
            ("QFUND") AS POS, 
          /* INSTANCE */
            (case 
              when t2.ST_CD = "CO" then "QFUND2"
              else "QFUND1"
            end
              ) AS INSTANCE, 
          t3.BRND_CD AS BRANDCD, 
          /* BANKMODEL */
            ("STANDARD") AS BANKMODEL, 
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
          t2.LOC_NBR AS LOCNBR, 
          t3.LOC_NM AS LOCATION_NAME, 
          t3.OPEN_DT AS LOC_OPEN_DT, 
          t3.CLS_DT AS LOC_CLOSE_DT, 
          /* DEAL_DT */
            (datepart(t2.ORIG_DT)) FORMAT=mmddyy10. AS DEAL_DT, 
          t2.ORIG_DT AS DEAL_DTTM, 
          /* BEGINDT */
            (intnx('year',today(),-6,'BEGINNING')) FORMAT=mmddyy10. LABEL="BEGINDT" AS BEGINDT, 
          t2.LOAN_NBR AS DEALNBR, 
          t2.CUST_NBR AS CUSTNBR, 
          t2.ORIG_PRNC_AMT AS ADVAMT, 
          /* FEEAMT */
            (.) AS FEEAMT, 
          t2.NSF_FEE_CHRG_AMT AS NSFFEEAMT, 
          t2.LATE_FEE_CHRG_AMT AS LATEFEEAMT, 
          /* OTHERFEEAMT */
            (sum(t2.MTH_HNDL_FEE_CHRG_AMT,t2.ACQSN_FEE_CHRG_AMT,t2.SDB_FEE_CHRG_AMT)) AS OTHERFEEAMT, 
          /* WAIVEDFEEAMT */
            (SUM(t2.NSF_FEE_WVD_AMT,t2.LATE_FEE_WVD_AMT)) AS WAIVEDFEEAMT, 
          t2.INT_RBT_AMT AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t2.TTL_PAID_AMT AS TOTALPAID, 
          t2.CRRNT_BLNC_AMT AS TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* REFINANCECNT */
            (.) AS REFINANCECNT, 
          /* CASHAGNCNT */
            (.) AS CASHAGNCNT, 
          t4.DUEDT AS DUEDT, 
          t4.FIRST_DUEDT, 
          t2.INT_CHRG_AMT AS INTERESTFEE, 
          t2.FRST_PRSNTMT_DT AS DEPOSITDT, 
/* Haritha - 28Feb2018 : Using the newly calculated column to avoid missing values */

/*          t2.WO_AGE_DT AS WRITEOFFDT, */
		  t2.WO_DT AS WRITEOFFDT,
          /* ACHSTATUSCD */
            ('') AS ACHSTATUSCD, 
          /* DEALSTATUSCD */
            (CASE WHEN t2.FNCL_STAT_TXT  = 'OVERPAID' THEN 'CLO'
                      WHEN t2.FNCL_STAT_TXT = 'DEFAULT' THEN 'DEF'
                      WHEN t2.FNCL_STAT_TXT = 'DEFAULT W/NSF' THEN 'DEF'
                      WHEN t2.FNCL_STAT_TXT = 'NSF' THEN 'OPN'
                      WHEN t2.FNCL_STAT_TXT = 'CURRENT' THEN 'OPN'
                      WHEN t2.FNCL_STAT_TXT = 'CURRENT IN FLIGHT' THEN 'OPN'
                      WHEN t2.FNCL_STAT_TXT = 'WRITE OFF (AGE)' THEN 'WO'
                      WHEN t2.FNCL_STAT_TXT = 'WRITE OFF (BANKRUPT)' THEN 'WO'
                      WHEN t2.FNCL_STAT_TXT = 'WRITE OFF (DECEASED)' THEN 'WO'
                      WHEN t2.FNCL_STAT_TXT = 'VOID/RESCIND' THEN 'V'
                      WHEN t2.FNCL_STAT_TXT = 'REPAID' THEN 'CLO'
            ELSE t2.FNCL_STAT_TXT END) AS DEALSTATUSCD, 
          t2.DFLT_DT AS DEFAULTDT, 
          /* CHECKSTATUSCD */
            (case
              when t2.FNCL_STAT_TXT = 'CURRENT' then 'HLD' 
              when t2.FNCL_STAT_TXT = 'CURRENT IN FLIGHT' then 'DEP'
              when t2.FNCL_STAT_TXT = 'DEFAULT' or t2.FNCL_STAT_TXT = 'DEFAULT W/NSF' or t2.FNCL_STAT_TXT = 'NSF' then 
            'RTN'
              when t2.FNCL_STAT_TXT = 'OVERPAID' or t2.FNCL_STAT_TXT = 'REPAID' then 'BGT'
              when t2.FNCL_STAT_TXT = 'WRITE OFF (AGE)' or t2.FNCL_STAT_TXT = 'WRITE OFF (BANKRUPT)' or 
            t2.FNCL_STAT_TXT = 'WRITE OFF (DECASED)' then 'WO'
            end) AS CHECKSTATUSCD, 
          /* COLLATERAL_TYPE */
            ('UNKNOWN') AS COLLATERAL_TYPE, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          /* ETLDT */
            (''D) FORMAT=MMDDYY10. LABEL="ETLDT" AS ETLDT, 
          t2.PROD_CD AS PRODUCTCD, 
          /* ACHAUTHFLG */
            ('') AS ACHAUTHFLG, 
          /* UPDATEDT */
            (t1.LOAN_SNAP_DT) FORMAT=DATETIME20. LABEL="UPDATEDT" AS UPDATEDT, 
          /* ENDDT */
            (intnx('day',today(),-1,'BEGINNING')) FORMAT=MMDDYY10. AS ENDDT, 
          t1.LOAN_SNAP_DT
      FROM EDW.D_LOCATION t3
           RIGHT JOIN (WORK.MOST_RECENT_DEALS_CO t1
           INNER JOIN WORK.LAST_WEEK_OF_UPDATES_CO t2 ON (t1.LOAN_SNAP_DT = t2.LOAN_SNAP_DT) AND (t1.LOAN_NBR = 
          t2.LOAN_NBR)) ON (t3.LOC_NBR = t2.LOC_NBR)
           LEFT JOIN WORK.QF2_DUEDTS t4 ON (t2.LOAN_NBR = t4.DEALNBR)
      WHERE (CALCULATED DEAL_DT) BETWEEN (CALCULATED BEGINDT) AND (CALCULATED ENDDT);
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.QFUND1_CUST AS 
   SELECT t1.CUST_ACCT_SRC_SYS_CD AS INSTANCE, 
          t1.CUST_ACCT_SRC_SYS_CUST_NBR AS CUSTNBR, 
          t2.IDV_SCRT_TID AS SSN
      FROM EDW.CUST_ACCT t1
           INNER JOIN EDW.IDV_SCRT t2 ON (t1.CUST_ACCT_CUST_PTY_ID = t2.IDV_SCRT_PTY_ID)
      WHERE t1.CUST_ACCT_SRC_SYS_CD = 'QFUND';
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.QF12_DAILY_UPDATE_PRE AS 
   SELECT t1.PRODUCT, 
          t1.PRODUCT_DESC, 
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
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          /* SSN */
            (CASE WHEN t3.SSN IS NULL THEN t4.SSN ELSE t3.SSN END) AS SSN, 
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
          t1.FIRST_DUEDT, 
          t2.LOAN_SNAP_DT AS DEALENDDT, 
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
      FROM WORK.DAILY_UPDATES t1
           LEFT JOIN WORK.LASTREPAYMENT t2 ON (t1.DEALNBR = t2.LOAN_NBR) AND (t1.CUSTNBR = t2.CUST_NBR)
           LEFT JOIN WORK.QF1_QF2_CUST_SSN t3 ON (t1.CUSTNBR = t3.CUSTNBR) AND (t1.INSTANCE = t3.SOURCESYSTEM)
           LEFT JOIN WORK.QFUND1_CUST t4 ON (t1.CUSTNBR = t4.CUSTNBR)
      ORDER BY t1.INSTANCE,
               t1.DEALNBR;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.QFUND2_CUST AS 
   SELECT t1.CUSTOMERNBR, 
          /* CUSTNBR */
            (input(t1.CUSTOMERNBR,BEST32.)) AS CUSTNBR, 
          t1.SSN
      FROM STG.TBL_CUSTOMER t1;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.QF12_DAILY_UPDATE_CO AS 
   SELECT t1.PRODUCT, 
          t1.PRODUCT_DESC, 
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
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.BEGINDT, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          /* SSN */
            (CASE WHEN t3.SSN IS NULL THEN t4.SSN ELSE t3.SSN END) AS SSN, 
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
          t1.FIRST_DUEDT, 
          t2.LOAN_SNAP_DT AS DEALENDDT, 
          t1.DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT
      FROM WORK.DAILY_UPDATES_CO t1
           LEFT JOIN WORK.LASTREPAYMENT_CO t2 ON (t1.DEALNBR = t2.LOAN_NBR) AND (t1.CUSTNBR = t2.CUST_NBR)
           LEFT JOIN WORK.QF1_QF2_CUST_SSN t3 ON (t1.CUSTNBR = t3.CUSTNBR) AND (t1.INSTANCE = t3.SOURCESYSTEM)
           LEFT JOIN WORK.QFUND2_CUST t4 ON (t1.CUSTNBR = t4.CUSTNBR)
      ORDER BY t1.INSTANCE,
               t1.DEALNBR;
%RUNQUIT(&job,&sub2);

PROC SQL;
CREATE TABLE QF12_DAILY_UPDATE AS 
SELECT * FROM WORK.QF12_DAILY_UPDATE_PRE
 OUTER UNION CORR 
SELECT * FROM WORK.QF12_DAILY_UPDATE_CO
;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE DEAL_SUMMARY_TMP AS 
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
          t1.LOCATION_NAME, 
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
          /* ETLDT */
            (DHMS(t1.ETLDT,00,00,00)) FORMAT=DATETIME20. AS ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          t1.ENDDT,
		  . AS OUTSTANDING_DRAW_AMT,
		  '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM QF12_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub2);

PROC SQL;
   CREATE TABLE WORK.DEAL_SUM_DAILY_UPDATE_PRE AS 
   SELECT T1.PRODUCT, 
          T1.POS, 
          T1.INSTANCE, 
		  'STOREFRONT'					AS CHANNELCD, 
          T1.BRANDCD, 
          T1.BANKMODEL, 
          T1.COUNTRYCD, 
          T1.STATE, 
          T1.CITY, 
          T1.ZIP, 
          T1.ZONENBR, 
          T1.ZONENAME, 
          T1.REGIONNBR, 
          T1.REGIONRDO, 
          T1.DIVISIONNBR, 
          T1.DIVISIONDDO, 
          /* BUSINESS_UNIT */
            (COMPRESS(PUT(T1.BUSINESS_UNIT,BEST9.))) AS BUSINESS_UNIT, 
          T1.LOCNBR, 
          T1.LOC_OPEN_DT, 
          T1.LOC_CLOSE_DT, 
          /* DEAL_DT */
            (DHMS(T1.DEAL_DT,0,0,0)) FORMAT=DATETIME20. AS DEAL_DT, 
          T1.DEAL_DTTM, 
          /* LAST_REPORT_DT */
            (DHMS(TODAY()-1,0,0,0)) FORMAT=DATETIME20. LABEL="LAST_REPORT_DT" AS LAST_REPORT_DT, 
          /* DEALNBR */
            (COMPRESS(PUT(DEALNBR,30.))) AS DEALNBR, 
          /* TITLE_DEALNBR */
            (COMPRESS(PUT((CASE 
               WHEN . = T1.TITLE_DEALNBR THEN 0
               ELSE T1.TITLE_DEALNBR
            END),30.))) AS TITLE_DEALNBR, 
          /* CUSTNBR */
            (COMPRESS(PUT(CUSTNBR,30.))) AS CUSTNBR, 
          T1.SSN,
		  ''	AS OMNINBR, 
          t1.ADVAMT, 
          t1.FEEAMT,
		  .			AS CUSTOMARYFEE, 
          T1.NSFFEEAMT, 
          T1.OTHERFEEAMT, 
          T1.LATEFEEAMT, 
          T1.WAIVEDFEEAMT, 
          T1.REBATEAMT, 
          T1.COUPONAMT, 
          T1.TOTALPAID, 
          T1.TOTALOWED, 
          T1.CONSECUTIVEDEALFLG, 
          T1.CASHAGNCNT, 
          T1.DUEDT, 
          T1.DEALENDDT, 
          T1.DEPOSITDT, 
          T1.WRITEOFFDT, 
          T1.DEFAULTDT, 
          T1.ACHSTATUSCD,
		  '' AS RETURNREASONCD LENGTH=5 FORMAT=$5.,
          T1.DEALSTATUSCD, 
          T1.CHECKSTATUSCD, 
          T1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          T1.ETLDT, 
          T1.PREVDEALNBR, 
          T1.PRODUCTCD, 
          T1.INTERESTFEE, 
          T1.ACHAUTHFLG, 
          T1.UPDATEDT,
		  T1.OUTSTANDING_DRAW_AMT,
		  T1.UNDER_COLLATERALIZED
      FROM WORK.DEAL_SUMMARY_TMP T1;
%RUNQUIT(&job,&sub2);

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
%RUNQUIT(&job,&sub2);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND1_QFUND2",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\QFUND1_QFUND2\DIR2\",'G');
%RUNQUIT(&job,&sub2);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_QF1QF2 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE;
%RUNQUIT(&job,&sub2);

/*UPLOAD QF12*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_QF12.SAS";

