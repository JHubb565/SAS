%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";



%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');
DATA _NULL_;
	CALL SYMPUTX('DAILY_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DAILY_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DAILY",'G');
%RUNQUIT(&job,&sub3);

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub3);

LIBNAME QF_VS ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=STG_QFUND_VS DEFER=YES;

LIBNAME EDW_STAR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=EDW_STAR DEFER=YES;

LIBNAME BIOR ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR DEFER=YES;

LIBNAME EADV_RPT ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EAPROD1
	SCHEMA=EADV_RPT DEFER=YES;


PROC SQL;
   CREATE TABLE WORK.QFUND12_DAILYSUMMARY_TMP1 AS 
   SELECT /* Product */
            ("INSTALLMENT") FORMAT=$18. LABEL="Product" AS Product, 
          /* pos */
            ("QFUND") LABEL="pos" AS pos, 
          /* INSTANCE */
            (case
              when st_cd = 'CO' then 'QFUND2'
              else 'QFUND1'
            end) LABEL="INSTANCE" AS INSTANCE, 
          /* bankmodel */
            ("STANDARD") LABEL="bankmodel" AS bankmodel, 
          t2.BRND_CD AS BRANDCD, 
          t2.CTRY_CD AS COUNTRYCD, 
          t2.ST_PVC_CD AS STATE, 
          t2.ADR_CITY_NM AS CITY, 
          t2.MAIL_CD AS ZIP, 
          t2.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t2.HIER_ZONE_NBR AS ZONENBR, 
          t2.HIER_ZONE_NM AS ZONENAME, 
          t2.HIER_RGN_NBR AS REGIONNBR, 
          t2.HIER_RDO_NM AS REGIONRDO, 
          t2.HIER_DIV_NBR AS DIVISIONNBR, 
          t2.HIER_DDO_NM AS DIVISIONDDO, 
          t1.LOC_NBR LABEL="LOCNBR" AS LOCNBR, 
          t2.LOC_NM AS Location_Name, 
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          /* BusinessDt */
            (datepart(t1.RECAP_DT)) FORMAT=mmddyy10. LABEL="BusinessDt" AS BusinessDt, 
          /* BeginDt */
            (intnx('month',today(),-24,'beginning')) FORMAT=mmddyy10. LABEL="BeginDt" AS BeginDt, 
          /* ADVCNT */
            (SUM(case when compress(t1.FNCL_STAT_TXT) = "VOIDED/RESCINDEDORIGINATIONS" then -t1.ORIG_LOAN_CNT else 
            t1.ORIG_LOAN_CNT end)) AS ADVCNT, 
          /* ADVAMT1 */
            (SUM(t1.ORIG_LOAN_PRNC_BLNC_AMT)) FORMAT=14.2 LABEL="ADVAMTSUM" AS ADVAMT1, 
          /* ADVAMTSUM */
            (SUM(case when t1.FNCL_STAT_TXT = "VOIDED/RESCINDED ORIGINATIONS" then t1.ORIG_LOAN_PRNC_BLNC_AMT else 
            t1.ORIG_LOAN_PRNC_BLNC_AMT end)) AS ADVAMTSUM, 
          /* EARNEDFEES */
            (SUM(SUM(t1.int_paid_amt, t1.sdb_fee_paid_amt, t1.mth_hndl_fee_paid_amt, 
            t1.acqsn_fee_paid_amt,t1.NSF_FEE_PAID_AMT,-t1.INT_RBT_AMT)  * -1)) FORMAT=14.2 LABEL="EARNEDFEES" AS 
            EARNEDFEES, 
          /* NEWCUSTDEALCNT */
            (SUM(t1.ORIG_NEW_CUST_CNT)) FORMAT=7. LABEL="NEWCUSTDEALCNT" AS NEWCUSTDEALCNT, 
          /* TOTADVRECV */
            (SUM(CASE
              WHEN t1.fncl_stat_txt in ("CURRENT", "CURRENT IN FLIGHT", "NSF") then out_prnc_blnc_amt
              ELSE 0
            END)) FORMAT=14.2 LABEL="TOTADVRECV" AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (SUM(CASE
              WHEN t1.fncl_stat_txt in ("CURRENT", "CURRENT IN FLIGHT", "NSF") then t1.OUT_LOAN_BLNC_AMT - 
            out_prnc_blnc_amt
              ELSE 0
            END)) FORMAT=14.2 LABEL="TOTADVFEERECV" AS TOTADVFEERECV, 
          /* TOTDEFAULTRECV */
            (SUM(CASE
              WHEN t1.fncl_stat_txt in ("DEFAULT", "DEFAULT W/NSF") then out_loan_blnc_amt
              ELSE 0
            END)) FORMAT=14.2 LABEL="TOTDEFAULTRECV" AS TOTDEFAULTRECV, 
          /* HELDCNT */
            (SUM(CASE
              WHEN t1.fncl_stat_txt in ("CURRENT", "CURRENT IN FLIGHT", "NSF") then out_loan_cnt
              ELSE 0
            END)) FORMAT=14. LABEL="HELDCNT" AS HELDCNT, 
          /* DEFAULTAMT */
            (SUM(CASE
              WHEN t1.fncl_stat_txt in ("DEFAULT", "DEFAULT W/NSF") then out_loan_blnc_amt
              ELSE 0
            END)) LABEL="DEFAULTAMT" AS DEFAULTAMT, 
          /* DEFAULTCNT */
            (SUM(CASE
              WHEN t1.fncl_stat_txt in ("DEFAULT", "DEFAULT W/NSF") then out_loan_cnt
              ELSE 0
            END)) FORMAT=14. LABEL="DEFAULTCNT" AS DEFAULTCNT, 
          /* WOAMTSUM */
            (SUM(t1.WO_AGE_AMT)) FORMAT=14.2 LABEL="WOAMTSUM" AS WOAMTSUM, 
          /* WOCNT */
            (SUM(t1.WO_AGE_CNT)) FORMAT=11. AS WOCNT, 
          /* WOBAMTSUM */
            (SUM(sum(t1.WO_BNKRPT_AMT,t1.WO_DCSD_AMT))) LABEL="WOBAMTSUM" AS WOBAMTSUM, 
          /* WOBCNT */
            (SUM(t1.WO_BNKRPT_CNT)) FORMAT=7. AS WOBCNT, 
          /* WODCNT */
            (SUM(t1.WO_DCSD_CNT)) FORMAT=7. AS WODCNT, 
          /* WORAMTSUM */
            (SUM((CASE
              WHEN t1.fncl_stat_txt in ("WRITE OFF (AGE)", "WRITE OFF (BANKRUPT)", "WRITE OFF (DECEASED)")
                  THEN 
            sum(t1.CASH_CC_MO_PRTL_PYMNT_AMT,t1.CASH_CC_MO_FULL_PYMNT_AMT,t1.ACH_PYMNT_PRTL_AMT,t1.ACH_PYMNT_FULL_AMT,t1.DPST_PRTL_PYMNT_AMT,t1.DPST_FULL_PYMNT_AMT)
            END) * -1)) FORMAT=14.2 LABEL="WORAMTSUM" AS WORAMTSUM, 
          /* AGNADVSUM */
            (SUM(0)) LABEL="AGNADVSUM" AS AGNADVSUM, 
          /* AGNCNT */
            (SUM(0)) LABEL="AGNCNT" AS AGNCNT, 
          /* Enddt */
            (intnx('day',TODAY(),-1,'beginning')) FORMAT=mmddyy10. LABEL="Enddt" AS Enddt, 
          /* substituterow */
            ('N') AS substituterow
      FROM EDW_ST.DAILYRECAP t1
           INNER JOIN EDW.D_LOCATION t2 ON (t1.LOC_NBR = t2.LOC_NBR)
      WHERE (CALCULATED BusinessDt) BETWEEN (CALCULATED BeginDt) AND (CALCULATED Enddt) AND t2.ST_PVC_CD NOT IS MISSING 
           AND t1.ST_CD = 'CO'
      GROUP BY (CALCULATED Product),
               (CALCULATED pos),
               (CALCULATED INSTANCE),
               (CALCULATED bankmodel),
               t2.BRND_CD,
               t2.CTRY_CD,
               t2.ST_PVC_CD,
               t2.ADR_CITY_NM,
               t2.MAIL_CD,
               t2.BUSN_UNIT_ID,
               t2.HIER_ZONE_NBR,
               t2.HIER_ZONE_NM,
               t2.HIER_RGN_NBR,
               t2.HIER_RDO_NM,
               t2.HIER_DIV_NBR,
               t2.HIER_DDO_NM,
               t1.LOC_NBR,
               t2.LOC_NM,
               t2.OPEN_DT,
               t2.CLS_DT,
               (CALCULATED BusinessDt),
               (CALCULATED BeginDt),
               (CALCULATED Enddt),
               (CALCULATED substituterow)
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.QF2_REFINANCE_CNT AS 
   SELECT /* LOCNBR */
            (INPUT(t1.STORENBR,BEST32.)) AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* REFINANCE_CNT */
            (SUM(t1.ADV_CASH_AGAINS)) AS REFINANCE_CNT
      FROM QF_VS.TBL_DAILY_SUMMARY t1
      WHERE (CALCULATED BUSINESSDT) BETWEEN INTNX('MONTH',TODAY(),-24,'B') AND TODAY()-1
      GROUP BY (CALCULATED LOCNBR),
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub3);

PROC SQL;
	CREATE TABLE PWO_QF2_PRE AS
		SELECT
			 STORE_NUMBER AS LOCNBR
			,CASE WHEN PRODUCT_TYPE = 'CSL' THEN 'INSTALLMENT' ELSE '' END AS PRODUCT
			,DHMS(DATEPART(CREATE_DATE_TIME),00,00,00) AS BUSINESSDT FORMAT DATETIME20.
			,SUM(PWO_AMT) AS BEGIN_PWO_AMT_PRE
			,SUM(CASE WHEN PWO_DATE <= DHMS(INTNX('MONTH',DATEPART(CALCULATED BUSINESSDT),0,'E'),00,00,00)
				 	  THEN PWO_AMT
					  ELSE 0 
				 END) AS CURRENT_PWO_AMT
			,SUM(CASE WHEN PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(CALCULATED BUSINESSDT),1,'B'),00,00,00)
										AND DHMS(INTNX('MONTH',DATEPART(CALCULATED BUSINESSDT),1,'E'),00,00,00)
					  THEN PWO_AMT
					  ELSE 0
				 END) AS NEX_MONTH_PWO_AMT
			,SUM(CASE WHEN PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(CALCULATED BUSINESSDT),2,'B'),00,00,00)
										AND DHMS(INTNX('MONTH',DATEPART(CALCULATED BUSINESSDT),2,'E'),00,00,00)
					  THEN PWO_AMT
					  ELSE 0
				 END) AS NEXT_2_MONTH_PWO_AMT
		FROM QF_VS.TBL_PWO
	GROUP BY 
		 STORE_NUMBER
		,CALCULATED PRODUCT
		,CALCULATED BUSINESSDT
	ORDER BY
		 STORE_NUMBER
		,CALCULATED PRODUCT
		,CALCULATED BUSINESSDT
;
%RUNQUIT(&job,&sub3);

DATA PWO_QF2;
	SET PWO_QF2_PRE;
	BY LOCNBR;
	IF FIRST.LOCNBR OR DAY(DATEPART(BUSINESSDT)) = 1 THEN 
		DO;
/*			%LET BEGIN_AMT = BEGIN_PWO_AMT_PRE;*/
			BEGIN_PWO_AMT = CURRENT_PWO_AMT;
			RETAIN BEGIN_PWO_AMT;
		END;
	BUSINESSDT = DATEPART(BUSINESSDT);
	WHERE DATEPART(BUSINESSDT) BETWEEN INTNX('MONTH',TODAY(),-24,'B') AND TODAY()-1;
	FORMAT BUSINESSDT MMDDYY10.;
DROP BEGIN_PWO_AMT_PRE;
%RUNQUIT(&job,&sub3);

DATA BEGIN_PWO_AMT;
	SET WORK.PWO_QF2;
	MONTH = MONTH(BUSINESSDT);
	YEAR = YEAR(BUSINESSDT);
	WHERE DAY(BUSINESSDT) = 1;
	KEEP LOCNBR BUSINESSDT BEGIN_PWO_AMT MONTH YEAR;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.HOLDOVER_QF2 AS 
   SELECT t1.LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESSDT)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.HOLDOVERAMT, 
          t1.OVERSHORTAMT
      FROM EADV_RPT.RPT_DAILYREPORT t1
           LEFT JOIN EDW.D_LOCATION t2 ON (t1.LOCNBR = t2.LOC_NBR)
      WHERE t2.ST_PVC_CD = 'CO' AND t1.BUSINESSDT >= INTNX('MONTH',TODAY(),-24,'B');
%RUNQUIT(&job,&sub3);

libname edw_st oracle schema=edw_st user=svc_sasuser pw="October132007!!" path=edwprd dbsliceparm=(ALL,4);

proc sql;
create table work.qfund12_originations2years2 as
	select * from edw_st.loandailysnapshot
	where orig_dt >= dhms(intnx('days',today(),-30,'beginning'),0,0,0) and PROD_CD ^= 'IPDL'
;
%RUNQUIT(&job,&sub3);


PROC SQL;
   CREATE TABLE WORK.qfund12_originations2years AS 
   SELECT t1.LOAN_SNAP_DT AS businessdt, 
          t1.ST_CD AS statecd, 
          t1.LOC_NBR AS locnbr, 
          t1.LOAN_NBR AS dealnbr, 
          t1.PROD_CD AS productcd, 
          t1.FNCL_STAT_TXT AS financialstatus, 
          /* OrigDt */
            (datepart(t1.ORIG_DT)) FORMAT=mmddyy10. LABEL="OrigDt" AS OrigDt, 
          /* DefaultDt */
            (datepart(t1.DFLT_DT)) FORMAT=mmddyy10. LABEL="DefaultDt" AS DefaultDt, 
          /* wodt */
            (datepart(t1.WO_AGE_DT)) FORMAT=mmddyy10. LABEL="wodt" AS wodt, 
          /* wobdt */
            (datepart(t1.WO_BNKRPT_DT)) FORMAT=mmddyy10. LABEL="wobdt" AS wobdt, 
          /* woddt */
            (datepart(t1.WO_DCSD_DT)) FORMAT=mmddyy10. LABEL="woddt" AS woddt, 
          /* repaiddt */
            (datepart(t1.REPAID_DT)) FORMAT=mmddyy10. LABEL="repaiddt" AS repaiddt, 
          t1.ORIG_PRNC_AMT, 
          t1.INT_CHRG_AMT, 
          t1.SDB_FEE_CHRG_AMT, 
          t1.NSF_FEE_CHRG_AMT, 
          t1.MTH_HNDL_FEE_CHRG_AMT, 
          t1.ACQSN_FEE_CHRG_AMT
      FROM WORK.QFUND12_ORIGINATIONS2YEARS2 t1
      WHERE (CALCULATED OrigDt) BETWEEN (intnx('month',today(),-36,'beginning')) AND (intnx('day',TODAY(),-1,'beginning'
           )) AND ( t1.DFLT_DT NOT IS MISSING OR t1.WO_AGE_DT NOT IS MISSING OR t1.WO_BNKRPT_DT NOT IS MISSING OR 
           t1.WO_DCSD_DT NOT IS MISSING OR t1.REPAID_DT NOT IS MISSING )
      ORDER BY t1.LOAN_NBR,
               t1.LOAN_SNAP_DT;
%RUNQUIT(&job,&sub3);

PROC SORT DATA=WORK.QFUND12_ORIGINATIONS2YEARS
	OUT=WORK.QFUND12_UNIQUEORIGS(LABEL="Sorted WORK.QFUND12_ORIGINATIONS2YEARS")
	NODUPKEY
	;
	BY statecd dealnbr;

%RUNQUIT(&job,&sub3);



PROC SQL;
   CREATE TABLE WORK.qfund12_uniqueorigs_tmp1 AS 
   SELECT t1.businessdt, 
          t1.statecd, 
          t1.locnbr, 
          t1.dealnbr, 
          t1.productcd, 
          t1.financialstatus, 
          t1.OrigDt, 
          t1.DefaultDt, 
          t1.wodt, 
          t1.wobdt, 
          t1.woddt, 
          t1.repaiddt, 
          /* Duration_Event_Date */
            (min(t1.DefaultDt,t1.wodt,t1.wobdt,t1.woddt,t1.repaiddt)) FORMAT=mmddyy10. LABEL="Duration_Event_Date" AS 
            Duration_Event_Date, 
          t1.ORIG_PRNC_AMT, 
          t1.INT_CHRG_AMT, 
          t1.SDB_FEE_CHRG_AMT, 
          t1.NSF_FEE_CHRG_AMT, 
          t1.MTH_HNDL_FEE_CHRG_AMT, 
          t1.ACQSN_FEE_CHRG_AMT
      FROM WORK.QFUND12_UNIQUEORIGS t1
      WHERE t1.financialstatus NOT = 'VOID/RESCIND';
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.qfund12_UNIQUEORIGS_TMP2 AS 
   SELECT t1.businessdt, 
          t1.statecd, 
          t1.locnbr, 
          t1.dealnbr, 
          t1.productcd, 
          t1.financialstatus, 
          t1.OrigDt, 
          t1.DefaultDt, 
          t1.wodt, 
          t1.wobdt, 
          t1.woddt, 
          t1.repaiddt, 
          t1.Duration_Event_Date, 
          /* Default_Duration_Count */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.DefaultDt AND t1.wobdt ~= t1.DefaultDt 
            AND t1.woddt ~= t1.DefaultDt then 1
              else 0
            end) LABEL="Default_Duration_Count" AS Default_Duration_Count, 
          /* Default_Duration_Days */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.DefaultDt AND t1.wobdt ~= t1.DefaultDt 
            AND t1.woddt ~= t1.DefaultDt then t1.Duration_Event_Date - t1.OrigDt
              else 0
            end) LABEL="Default_Duration_Days" AS Default_Duration_Days, 
          /* WO_Duration_Count */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.wodt then 1
              else 0
            end) LABEL="WO_Duration_Count" AS WO_Duration_Count, 
          /* WO_Duration_Days */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.wodt then t1.Duration_Event_Date - 
            t1.OrigDt
              else 0
            end) LABEL="WO_Duration_Days" AS WO_Duration_Days, 
          /* WOB_Duration_Count */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.wobdt then 1
              else 0
            end) LABEL="WOB_Duration_Count" AS WOB_Duration_Count, 
          /* WOB_Duration_Days */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.wobdt then t1.Duration_Event_Date - 
            t1.OrigDt
              else 0
            end) LABEL="WOB_Duration_Days" AS WOB_Duration_Days, 
          /* WOD_Duration_Count */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.woddt then 1
              else 0
            end) LABEL="WOD_Duration_Count" AS WOD_Duration_Count, 
          /* WOD_Duration_Days */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.woddt then t1.Duration_Event_Date - 
            t1.OrigDt
              else 0
            end) LABEL="WOD_Duration_Days" AS WOD_Duration_Days, 
          /* Repaid_Duration_Count */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.RepaidDt then 1
              else 0
            end) LABEL="Repaid_Duration_Count" AS Repaid_Duration_Count, 
          /* Repaid_Duration_Days */
            (case
              when t1.Duration_Event_Date ~= . AND t1.Duration_Event_Date = t1.RepaidDt then t1.Duration_Event_Date - 
            t1.OrigDt
              else 0
            end) LABEL="Repaid_Duration_Days" AS Repaid_Duration_Days, 
          t1.ORIG_PRNC_AMT, 
          t1.INT_CHRG_AMT, 
          t1.SDB_FEE_CHRG_AMT, 
          t1.NSF_FEE_CHRG_AMT, 
          t1.MTH_HNDL_FEE_CHRG_AMT, 
          t1.ACQSN_FEE_CHRG_AMT
      FROM WORK.QFUND12_UNIQUEORIGS_TMP1 t1;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.QFUND12_AGGREGATE_DURATION AS 
   SELECT DISTINCT t1.statecd, 
          t1.Duration_Event_Date AS businessdt, 
          t1.locnbr, 
          /* Default_Duration_Count */
            (SUM(t1.Default_Duration_Count)) AS Default_Duration_Count, 
          /* Default_Duration_Days */
            (SUM(t1.Default_Duration_Days)) AS Default_Duration_Days, 
          /* WO_Duration_Count */
            (SUM(t1.WO_Duration_Count)) AS WO_Duration_Count, 
          /* WO_Duration_Days */
            (SUM(t1.WO_Duration_Days)) AS WO_Duration_Days, 
          /* WOB_Duration_Count */
            (SUM(t1.WOB_Duration_Count)) AS WOB_Duration_Count, 
          /* WOB_Duration_Days */
            (SUM(t1.WOB_Duration_Days)) AS WOB_Duration_Days, 
          /* WOD_Duration_Count */
            (SUM(t1.WOD_Duration_Count)) AS WOD_Duration_Count, 
          /* WOD_Duration_Days */
            (SUM(t1.WOD_Duration_Days)) AS WOD_Duration_Days, 
          /* Repaid_Duration_Count */
            (SUM(t1.Repaid_Duration_Count)) AS Repaid_Duration_Count, 
          /* Repaid_Duration_Days */
            (SUM(t1.Repaid_Duration_Days)) AS Repaid_Duration_Days, 
          /* ACTUAL_DURATION_ADVAMT */
            (SUM(t1.ORIG_PRNC_AMT)) LABEL="ACTUAL_DURATION_ADVAMT" AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            
            (SUM(sum(t1.INT_CHRG_AMT,t1.SDB_FEE_CHRG_AMT,t1.NSF_FEE_CHRG_AMT,t1.MTH_HNDL_FEE_CHRG_AMT,t1.ACQSN_FEE_CHRG_AMT))) 
            AS ACTUAL_DURATION_FEES
      FROM WORK.QFUND12_UNIQUEORIGS_TMP2 t1
      WHERE t1.Duration_Event_Date NOT IS MISSING
      GROUP BY t1.statecd,
               t1.Duration_Event_Date,
               t1.locnbr
      ORDER BY t1.locnbr,
               t1.Duration_Event_Date;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.originations AS 
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
          t1.ACQSN_FEE_BLNC_AMT
      FROM WORK.QFUND12_ORIGINATIONS2YEARS2 t1
      WHERE t1.LOAN_SNAP_DT = t1.ORIG_DT;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.voided_loans AS 
   SELECT DISTINCT t1.ST_CD, 
          t1.LOAN_NBR
      FROM WORK.QFUND12_ORIGINATIONS2YEARS2 t1
      WHERE t1.FNCL_STAT_TXT = 'VOID/RESCIND';
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.nonvoided_loans AS 
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
          t1.ACQSN_FEE_BLNC_AMT
      FROM WORK.ORIGINATIONS t1
           LEFT JOIN WORK.VOIDED_LOANS t2 ON (t1.LOAN_NBR = t2.LOAN_NBR) AND (t1.ST_CD = t2.ST_CD)
      WHERE t2.LOAN_NBR IS MISSING AND t2.ST_CD IS MISSING;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.advfeeamt AS 
   SELECT /* businessdt */
            (datepart(t1.LOAN_SNAP_DT)) FORMAT=mmddyy10. AS businessdt, 
          t1.LOC_NBR, 
          /* ADVFEEAMT */
            (SUM(sum(t1.INT_CHRG_AMT,t1.SDB_FEE_CHRG_AMT,t1.MTH_HNDL_FEE_CHRG_AMT,t1.ACQSN_FEE_CHRG_AMT))) AS ADVFEEAMT
      FROM WORK.NONVOIDED_LOANS t1
      GROUP BY (CALCULATED businessdt),
               t1.LOC_NBR;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.SAP_CO AS 
   SELECT t1.ZONENAME, 
          t1.ZONENBR, 
          t1.REGIONRDO, 
          t1.REGIONNBR, 
          t1.DIVISIONDDO, 
          t1.DIVISIONNBR, 
          t1.LOCNBR, 
          t1.STATE, 
          t1.LAST_REPORT_DATE, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESSDT)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.PRODUCT, 
          t1.REVENUE_ACTUAL, 
          t1.CUSTOMER_DISCOUNT, 
          t1.BADDEBT_ACTUAL, 
          t1.CENTER_EXPENSE
      FROM BIOR.O_SAP_ALL t1
      WHERE t1.STATE = 'CO' AND t1.PRODUCT = 'INSTALLMENT';
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.QF2_ILP_PNL AS 
   SELECT t1.STORE_NUMBER, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.BAD_DEBT AS GROSS_WRITE_OFF, 
          t1.BADDEBT_PMT AS WOR, 
          t1.PNL_AMT AS GROSS_REVENUE
      FROM EDW.QF_BADDEBT_PNLAMT t1
      WHERE t1.SOURCE_SYSTEM = 'QFUND2' AND t1.PRODUCT_TYPE = 'ILP';
%RUNQUIT(&job,&sub3);

PROC APPEND BASE=SKYNET.QFUND2_DURATION DATA=WORK.QFUND12_AGGREGATE_DURATION;
%RUNQUIT(&job,&sub3);

PROC SORT DATA=SKYNET.QFUND2_DURATION DUPOUT=DUPS NODUPKEY;
BY locnbr businessdt;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.QF1_PD_COMB AS 
   SELECT /* PRODUCT */
            ("INSTALLMENT") LABEL="PRODUCT" AS PRODUCT, 
          t1.ST_CODE AS LOCNBR, 
          /* BUSINESSDT */
            (datepart(t1.AS_OF_DATE)) FORMAT=mmddyy10. LABEL="BUSINESSDT" AS BUSINESSDT, 
          /* begin_dt */
            (intnx('month',today(),-24,'b')) FORMAT=mmddyy10. LABEL="begin_dt" AS begin_dt, 
          /* PASTDUECNT_1 */
            (SUM(t1.PASTDUE_LOAN_CNT_01_09)) FORMAT=11. AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (SUM(t1.PASTDUE_LOAN_AMT_01_09)) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* PASTDUECNT_2 */
            (SUM(t1.PASTDUE_LOAN_CNT_GRT10)) FORMAT=11. AS PASTDUECNT_2, 
          /* PASTDUEAMT_2 */
            (SUM(t1.PASTDUE_LOAN_AMT_GRT10)) FORMAT=12.2 AS PASTDUEAMT_2, 
          /* end_dt */
            (intnx('day',today(),-1,'b')) FORMAT=mmddyy10. LABEL="end_dt" AS end_dt
      FROM QFUND1.PAST_DUE_ILP t1
      WHERE (CALCULATED BUSINESSDT) BETWEEN (CALCULATED begin_dt) AND (CALCULATED end_dt) AND t1.STATE_ID = 'CO'
      GROUP BY (CALCULATED PRODUCT),
               t1.ST_CODE,
               (CALCULATED BUSINESSDT),
               (CALCULATED begin_dt),
               (CALCULATED end_dt)
      ORDER BY t1.ST_CODE,
               BUSINESSDT;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.QF1_PD_COMB_RECODE AS 
   SELECT DISTINCT t1.PRODUCT, 
          t1.LOCNBR, 
          t1.BUSINESSDT, 
          /* PASTDUEAMT_1 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_1 THEN 0
               ELSE t1.PASTDUEAMT_1
            END) FORMAT=12.2 LABEL="PASTDUEAMT_1" AS PASTDUEAMT_1, 
          /* PASTDUECNT_1 */
            (CASE 
               WHEN . = t1.PASTDUECNT_1 THEN 0
               ELSE t1.PASTDUECNT_1
            END) FORMAT=11. LABEL="PASTDUECNT_1" AS PASTDUECNT_1, 
          /* PASTDUEAMT_2 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_2 THEN 0
               ELSE t1.PASTDUEAMT_2
            END) FORMAT=12.2 LABEL="PASTDUEAMT_2" AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (CASE 
               WHEN . = t1.PASTDUECNT_2 THEN 0
               ELSE t1.PASTDUECNT_2
            END) FORMAT=11. LABEL="PASTDUECNT_2" AS PASTDUECNT_2
      FROM WORK.QF1_PD_COMB t1
      ORDER BY t1.LOCNBR,
               t1.PRODUCT,
               t1.BUSINESSDT;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.PNL AS 
   SELECT t1.BUSINESS_DATE, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.STORE_NUMBER, 
          t1.PRODUCT_TYPE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          /* PNL_AMT */
            (-t1.PNL_AMT) AS PNL_AMT
      FROM QF_VS.TBL_PNL_AMT t1;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.QF2_TXNS AS 
   SELECT t1.TRANSACTION_DATE_KEY, 
          t1.TIME_ID, 
          t1.LOCATION_ID, 
          t1.PRODUCT_ID, 
          t1.CUSTOMER_ID, 
          t1.TRANSACTIONTYPE_ID, 
          t1.TRANSACTION_NBR, 
          t1.APPLIED_CD_ID, 
          t1.ETL_DT, 
          t1.VOID_ID, 
          t1.REPORT_DATE_KEY, 
          t1.ORGANIZATION_ID, 
          t1.CREATE_EMPLOYEE_ID, 
          t1.UPDATE_EMPLOYEE_ID, 
          t1.DEAL_ID, 
          t1.AMOUNT, 
          t1.ORIGINAL_TRANSACTION_NBR, 
          t1.CHANGE_TENDER_AMT, 
          t1.OTHER_LOCATION_NBR, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.PRESENTMENT_CNT, 
          t1.AUDIT_ID, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE_KEY)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* BEGINDT */
            (INTNX('MONTH',TODAY(),-24,'B')) FORMAT=MMDDYY10. AS BEGINDT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'B')) FORMAT=MMDDYY10. AS ENDDT
      FROM EDW_STAR.TRANSACTION_FACT t1
      WHERE (CALCULATED BUSINESSDT) BETWEEN (CALCULATED BEGINDT) AND (CALCULATED ENDDT) AND t1.APPLIED_CD_ID = 47;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT AS 
   SELECT t2.LOCATION_NBR AS LOCNBR, 
          t1.BUSINESSDT, 
          /* DEFAULT_AMT */
            (SUM(t1.AMOUNT)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* DEFAULT_CNT */
            (COUNT(DISTINCT(t1.DEAL_ID))) AS DEFAULT_CNT
      FROM WORK.QF2_TXNS t1
           INNER JOIN EDW_STAR.LOCATION_DIM t2 ON (t1.LOCATION_ID = t2.LOCATION_ID)
      GROUP BY t2.LOCATION_NBR,
               t1.BUSINESSDT
      ORDER BY t2.LOCATION_NBR,
               t1.BUSINESSDT;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS_PRE AS 
   SELECT t1.TRANSACTION_DATE_KEY, 
          t1.TIME_ID, 
          t1.LOCATION_ID, 
          t1.PRODUCT_ID, 
          t1.CUSTOMER_ID, 
          t1.TRANSACTIONTYPE_ID, 
          t1.TRANSACTION_NBR, 
          t1.APPLIED_CD_ID, 
          t1.ETL_DT, 
          t1.VOID_ID, 
          t1.REPORT_DATE_KEY, 
          t1.ORGANIZATION_ID, 
          t1.CREATE_EMPLOYEE_ID, 
          t1.UPDATE_EMPLOYEE_ID, 
          t1.DEAL_ID, 
          t1.AMOUNT, 
          t1.ORIGINAL_TRANSACTION_NBR, 
          t1.CHANGE_TENDER_AMT, 
          t1.OTHER_LOCATION_NBR, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.PRESENTMENT_CNT, 
          t1.AUDIT_ID
      FROM EDW_STAR.TRANSACTION_FACT t1
      WHERE t1.TRANSACTIONTYPE_ID = 133;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS AS 
   SELECT t2.LOCATION_NBR AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE_KEY)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_PMT */
            (SUM(-t1.AMOUNT)) AS DEFAULT_PMT
      FROM WORK.DEFAULT_PMTS_PRE t1
           INNER JOIN EDW_STAR.LOCATION_DIM t2 ON (t1.LOCATION_ID = t2.LOCATION_ID)
      WHERE t1.LOCATION_ID NOT = -1
      GROUP BY t2.LOCATION_NBR,
               (CALCULATED BUSINESSDT)
      ORDER BY t2.LOCATION_NBR,
               BUSINESSDT;
%RUNQUIT(&job,&sub3);

DATA _NULL_;
	CALL SYMPUTX('AS_OF_DT',TODAY()-1,'G');
	CALL SYMPUTX('END',TODAY()-1,'G');
%RUNQUIT(&job,&sub3);

%MACRO PASTDUE();
DATA PASTDUE;
	LOCNBR = "     ";
	BUSINESSDT = ""DT;
	PASTDUECNT_1 = .;
	PASTDUEAMT_1 = .;
	FORMAT BUSINESSDT MMDDYY10.;
IF 1 = 0;
RUN;

%DO I=&AS_OF_DT %TO &END;
	PROC SQL;
		CREATE TABLE PASTDUE_&I AS
			SELECT 
				 SD.STORENBR AS LOCNBR LENGTH = 5
				,(&I) AS BUSINESSDT FORMAT MMDDYY10.
				,COUNT(DISTINCT SD.LOAN_CODE) AS PASTDUECNT_1
				,SUM(S.INST_AMT_DUE) AS PASTDUEAMT_1
			FROM QF_VS.TBL_SIL_SCHEDULE S,
				 QF_VS.TBL_SIL_DETAIL SD
			WHERE S.ILOAN_CODE = SD.LOAN_CODE
				  AND S.INST_AMT_DUE > 0
				  AND DATEPART(S.INST_DUE_DATE) <= &I-1
				  AND SD.TRAN_ID IN ('Advance','Cash Again') 
	              AND SD.PRODUCT_ID='CSL'
				  AND S.ILOAN_CODE IN(
				  						SELECT 
									        O.LOAN_CODE
									    FROM QF_VS.TBL_SIL_OPEN O 
									    WHERE 
									        O.AS_OF_DATE=DHMS(&I,00,00,00)
									  )    
			GROUP BY SD.STORENBR
					,CALCULATED BUSINESSDT
			ORDER BY SD.STORENBR
	;
	QUIT;

	PROC APPEND BASE=WORK.PASTDUE DATA=PASTDUE_&I;
	RUN;

	PROC DATASETS LIB=WORK NODETAILS;
	DELETE PASTDUE_&I;
	RUN;

%END;
%MEND;
%PASTDUE


PROC APPEND BASE=SKYNET.QF2_PD DATA=WORK.PASTDUE;
%RUNQUIT(&job,&sub3);

PROC SORT DATA=SKYNET.QF2_PD DUPOUT=DUPS1 NODUPKEY;
BY LOCNBR BUSINESSDT;
%RUNQUIT(&job,&sub3);

DATA WORK.QF2_PASTDUE(DROP=LOCNBR RENAME=(LOCNBR_1=LOCNBR));
	SET SKYNET.QF2_PD;
	LOCNBR_1 = INPUT(LOCNBR,5.);
%RUNQUIT(&job,&sub3);


PROC SQL;
   CREATE TABLE WORK.QFUND2_DAILYSUMMARY AS 
   SELECT t2.Product, 
          t2.pos, 
          t2.INSTANCE, 
          t2.bankmodel, 
          t2.BRANDCD, 
          t2.COUNTRYCD, 
          t2.STATE, 
          t2.CITY, 
          t2.ZIP, 
          t2.BUSINESS_UNIT, 
          t2.ZONENBR, 
          t2.ZONENAME, 
          t2.REGIONNBR, 
          t2.REGIONRDO, 
          t2.DIVISIONNBR, 
          t2.DIVISIONDDO, 
          t2.LOCNBR, 
          t2.Location_Name, 
          t2.LOC_OPEN_DT, 
          t2.LOC_CLOSE_DT, 
          t2.BusinessDt, 
          t2.BeginDt, 
          t2.ADVCNT, 
          t2.ADVAMTSUM, 
          t3.ADVFEEAMT, 
          t2.EARNEDFEES, 
          t2.NEWCUSTDEALCNT, 
          t2.TOTADVRECV, 
          t2.TOTADVFEERECV, 
          t2.TOTDEFAULTRECV, 
          t2.HELDCNT, 
          t2.DEFAULTCNT, 
          t2.DEFAULTAMT, 
          t2.WOAMTSUM, 
          t2.WOCNT, 
          t2.WOBAMTSUM, 
          t2.WOBCNT, 
          t2.WODCNT, 
          t2.WORAMTSUM, 
          t2.AGNADVSUM, 
          t2.AGNCNT, 
          /* Default_Duration_Count */
            (CASE 
               WHEN . = t1.Default_Duration_Count THEN 0
               ELSE t1.Default_Duration_Count
            END) AS Default_Duration_Count, 
          /* Default_Duration_Days */
            (CASE 
               WHEN . = t1.Default_Duration_Days THEN 0
               ELSE t1.Default_Duration_Days
            END) AS Default_Duration_Days, 
          /* WO_Duration_Count */
            (CASE 
               WHEN . = t1.WO_Duration_Count THEN 0
               ELSE t1.WO_Duration_Count
            END) AS WO_Duration_Count, 
          /* WO_Duration_Days */
            (CASE 
               WHEN . = t1.WO_Duration_Days THEN 0
               ELSE t1.WO_Duration_Days
            END) AS WO_Duration_Days, 
          /* WOB_Duration_Count */
            (CASE 
               WHEN . = t1.WOB_Duration_Count THEN 0
               ELSE t1.WOB_Duration_Count
            END) AS WOB_Duration_Count, 
          /* WOB_Duration_Days */
            (CASE 
               WHEN . = t1.WOB_Duration_Days THEN 0
               ELSE t1.WOB_Duration_Days
            END) AS WOB_Duration_Days, 
          /* WOD_Duration_Count */
            (CASE 
               WHEN . = t1.WOD_Duration_Count THEN 0
               ELSE t1.WOD_Duration_Count
            END) AS WOD_Duration_Count, 
          /* WOD_Duration_Days */
            (CASE 
               WHEN . = t1.WOD_Duration_Days THEN 0
               ELSE t1.WOD_Duration_Days
            END) AS WOD_Duration_Days, 
          /* Repaid_Duration_Count */
            (CASE 
               WHEN . = t1.Repaid_Duration_Count THEN 0
               ELSE t1.Repaid_Duration_Count
            END) AS Repaid_Duration_Count, 
          /* Repaid_Duration_Days */
            (CASE 
               WHEN . = t1.Repaid_Duration_Days THEN 0
               ELSE t1.Repaid_Duration_Days
            END) AS Repaid_Duration_Days, 
          /* PASTDUECNT_1 */
            (CASE 
               WHEN . = t8.PASTDUECNT_1 THEN 0
               ELSE t8.PASTDUECNT_1
            END) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (CASE 
               WHEN . = t8.PASTDUEAMT_1 THEN 0
               ELSE t8.PASTDUEAMT_1
            END) AS PASTDUEAMT_1, 
          t9.PNL_AMT, 
          t2.substituterow, 
          /* Actual_Duration_Advamt */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_ADVAMT THEN 0
               ELSE t1.ACTUAL_DURATION_ADVAMT
            END) AS Actual_Duration_Advamt, 
          /* Actual_Duration_Fees */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_FEES THEN 0
               ELSE t1.ACTUAL_DURATION_FEES
            END) AS Actual_Duration_Fees, 
          t5.DEFAULT_AMT, 
          t5.DEFAULT_CNT, 
          t6.DEFAULT_PMT, 
          t7.CURRENT_PWO_AMT, 
          t7.NEX_MONTH_PWO_AMT, 
          t7.NEXT_2_MONTH_PWO_AMT, 
          /* MONTH */
            (MONTH(t2.businessdt)) AS MONTH, 
          /* YEAR */
            (YEAR(t2.businessdt)) AS YEAR
      FROM WORK.PWO_QF2 t7
           RIGHT JOIN (WORK.DEFAULT_PMTS t6
           RIGHT JOIN (WORK.DEFAULT_AMT_CNT t5
           RIGHT JOIN (WORK.QF1_PD_COMB_RECODE t4
           RIGHT JOIN (WORK.ADVFEEAMT t3
           RIGHT JOIN (SKYNET.QFUND2_DURATION t1
           RIGHT JOIN WORK.QFUND12_DAILYSUMMARY_TMP1 t2 ON (t1.locnbr = t2.LOCNBR) AND (t1.businessdt = t2.BusinessDt)) 
          ON (t3.businessdt = t2.BusinessDt) AND (t3.LOC_NBR = t2.LOCNBR)) ON (t4.LOCNBR = t2.LOCNBR) AND 
          (t4.BUSINESSDT = t2.BusinessDt) AND (t4.PRODUCT = t2.Product)) ON (t5.LOCNBR = t2.LOCNBR) AND (t5.BUSINESSDT 
          = t2.BusinessDt)) ON (t6.LOCNBR = t2.LOCNBR) AND (t6.BUSINESSDT = t2.BusinessDt)) ON (t7.LOCNBR = t2.LOCNBR) 
          AND (t7.BUSINESSDT = t2.BusinessDt)
           LEFT JOIN WORK.QF2_PASTDUE t8 ON (t1.businessdt = t8.BUSINESSDT) AND (t1.locnbr = t8.LOCNBR)
           LEFT JOIN WORK.PNL t9 ON (t2.LOCNBR = t9.STORE_NUMBER) AND (t2.BusinessDt = t9.BUSINESSDT);
%RUNQUIT(&job,&sub3);


PROC SQL;
	CREATE TABLE WORK.QFUND2_DAILYSUMMARY_PRE AS
		SELECT 
			T1.*,
			CASE WHEN T2.BEGIN_PWO_AMT = . THEN 0 ELSE T2.BEGIN_PWO_AMT END AS BEGIN_PWO_AMT
		FROM WORK.QFUND2_DAILYSUMMARY T1
		LEFT JOIN 
		WORK.BEGIN_PWO_AMT t2
		ON(T1.LOCNBR = T2.LOCNBR AND
		   T1.YEAR=T2.YEAR AND
		   T1.MONTH=T2.MONTH)
/*		WHERE T1.BUSINESSDT >= TODAY() - 10*/

;
%RUNQUIT(&job,&sub3);


PROC SQL;
   CREATE TABLE QFUND2_DAILYSUMMARY AS 
   SELECT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.bankmodel, 
          t1.BRANDCD, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BusinessDt, 
          t1.BeginDt, 
          t1.ADVCNT, 
          t1.ADVAMTSUM, 
          t1.ADVFEEAMT, 
          t1.EARNEDFEES, 
          t1.NEWCUSTDEALCNT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          t1.TOTDEFAULTRECV, 
          t1.HELDCNT, 
          t1.DEFAULTCNT, 
          t1.DEFAULTAMT, 
          t2.BADDEBT_ACTUAL, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          t1.WOBCNT, 
          t1.WODCNT, 
          t1.WORAMTSUM, 
          t1.AGNADVSUM, 
          t1.AGNCNT, 
          t3.HOLDOVERAMT, 
          t3.OVERSHORTAMT, 
          t1.Default_Duration_Count, 
          t1.Default_Duration_Days, 
          t1.WO_Duration_Count, 
          t1.WO_Duration_Days, 
          t1.WOB_Duration_Count, 
          t1.WOB_Duration_Days, 
          t1.WOD_Duration_Count, 
          t1.WOD_Duration_Days, 
          t1.Repaid_Duration_Count, 
          t1.Repaid_Duration_Days, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.PNL_AMT, 
          t5.GROSS_WRITE_OFF, 
          t5.WOR, 
          t5.GROSS_REVENUE, 
          t1.substituterow, 
          t1.Actual_Duration_Advamt, 
          t1.Actual_Duration_Fees, 
          t1.DEFAULT_AMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_PMT, 
          t4.REFINANCE_CNT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEX_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.MONTH, 
          t1.YEAR, 
          t1.BEGIN_PWO_AMT
      FROM WORK.QFUND2_DAILYSUMMARY_PRE t1
           LEFT JOIN WORK.SAP_CO t2 ON (t1.LOCNBR = t2.LOCNBR) AND (t1.BusinessDt = t2.BUSINESSDT)
           LEFT JOIN WORK.HOLDOVER_QF2 t3 ON (t1.LOCNBR = t3.LOCNBR) AND (t1.BusinessDt = t3.BUSINESSDT)
           LEFT JOIN WORK.QF2_REFINANCE_CNT t4 ON (t1.LOCNBR = t4.LOCNBR) AND (t1.BusinessDt = t4.BUSINESSDT)
           LEFT JOIN WORK.QF2_ILP_PNL t5 ON (t1.BusinessDt = t5.BUSINESSDT) AND (t1.LOCNBR = t5.STORE_NUMBER);
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE QFUND2_NEW_ORIGINATIONS AS 
   SELECT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BusinessDt, 
          /* NEW_ORIGINATIONS */
            (sum(t1.ADVCNT,t1.AGNCNT)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (sum(t1.ADVAMTSUM,t1.AGNADVSUM)) AS NEW_ADV_AMT, 
          t1.ADVFEEAMT AS NEW_ADVFEE_AMT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (t1.HELDCNT) AS COMPLIANT_LOANS_OUTSTANDING, 
          t1.DEFAULTCNT AS DEFAULT_LOANS_OUTSTANDING, 
          t1.TOTDEFAULTRECV, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          /* WOBCNT */
            (sum(t1.WOBCNT,t1.WODCNT)) AS WOBCNT, 
          /* WORAMTSUM */
            (CASE WHEN t1.BusinessDt < '01APR2017'D THEN t1.WORAMTSUM ELSE 0 END) AS WORAMTSUM, 
          t1.DEFAULT_AMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_PMT, 
          t1.BEGIN_PWO_AMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEX_MONTH_PWO_AMT AS NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUECNT_1, 
          t1.REFINANCE_CNT, 
          /* GROSS_REVENUE */
            (CASE WHEN t1.BusinessDt < '01APR2017'D THEN (t1.PNL_AMT) ELSE 0 END ) AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (CASE WHEN t1.BusinessDt < '01APR2017'D THEN (SUM(t1.WOAMTSUM,t1.WOBAMTSUM)) ELSE 0 END) AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (CASE WHEN t1.BusinessDt < '01APR2017'D THEN (t1.BADDEBT_ACTUAL) ELSE 0 END) AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (CASE WHEN t1.BusinessDt < '01APR2017'D THEN (SUM((t1.PNL_AMT),-(t1.BADDEBT_ACTUAL))) ELSE 0 END ) AS 
            NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            
            (sum(t1.Default_Duration_Count,t1.WO_Duration_Count,t1.WOB_Duration_Count,t1.WOD_Duration_Count,t1.Repaid_Duration_Count)) 
            AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            
            (sum(t1.Default_Duration_Days,t1.WO_Duration_Days,t1.WOB_Duration_Days,t1.WOD_Duration_Days,t1.Repaid_Duration_Days)) 
            AS ACTUAL_DURATION_DAYS, 
          t1.HOLDOVERAMT, 
          t1.OVERSHORTAMT, 
          t1.Actual_Duration_Advamt, 
          t1.Actual_Duration_Fees, 
          t1.ADVCNT, 
          t1.AGNCNT, 
          t1.ADVAMTSUM, 
          t1.AGNADVSUM, 
          t1.HELDCNT, 
          /* PRODUCT_DESC */
            (case
              when state in ('IL', 'WI', 'DE', 'CO') then "IPDL"
              else "MULTISTATE INSTALLMENT"
            end) AS PRODUCT_DESC
      FROM QFUND2_DAILYSUMMARY t1;
%RUNQUIT(&job,&sub3);

LIBNAME EDW ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=EDW;

DATA PNL_INTIAL_PULL;
	SET EDW.QF_BADDEBT_PNLAMT;
	BUSINESSDT = DATEPART(BUSINESS_DATE);
	WHERE BUSINESS_DATE >= '01APR2017:00:00:00'DT;
	FORMAT BUSINESSDT MMDDYY10.;
%RUNQUIT(&job,&sub3);

PROC SQL;
	CREATE TABLE WORK.QFUND_PNL AS 
		SELECT 
			STORE_NUMBER AS LOCNBR
		   ,CASE WHEN SOURCE_SYSTEM = 'QFUND5' THEN 'QFUND5-6' ELSE SOURCE_SYSTEM END AS INSTANCE
		   ,PRODUCT_TYPE
		   ,BUSINESSDT
		   ,BAD_DEBT AS GROSS_WRITE_OFF
		   ,-BADDEBT_PMT AS WORAMTSUM
		   ,-PNL_AMT AS GROSS_REVENUE
		   ,SUM(BAD_DEBT,BADDEBT_PMT) AS NET_WRITE_OFF
		   ,SUM(-PNL_AMT,(-SUM(BAD_DEBT,BADDEBT_PMT))) AS NET_REVENUE
		FROM WORK.PNL_INTIAL_PULL
	WHERE COMPRESS(PRODUCT_TYPE) ^= 'MISC'
;
%RUNQUIT(&job,&sub3);

/*-------------*/
/* QFUND 2 ILP */
/*-------------*/
PROC SQL;
	CREATE TABLE QFUND2_ILP AS
		SELECT
		    CASE WHEN COMPRESS(PRODUCT_TYPE) = 'ILP' THEN 'INSTALLMENT' 
			     ELSE PRODUCT_TYPE 
            END AS PRODUCT
		   ,CASE WHEN LOC.ST_PVC_CD IN('IL','WI','DE','CO') THEN 'IPDL'
		         ELSE 'MULTISTATE INSTALLMENT'
		    END AS PRODUCT_DESC
		   ,'QFUND' AS POS
		   ,INSTANCE
		   ,'STANDARD' AS BANKMODEL
		   ,LOC.BRND_CD AS BRANDCD
		   ,LOC.CTRY_CD AS COUNTRYCD
		   ,LOC.ST_PVC_CD AS STATE
		   ,LOC.ADR_CITY_NM AS CITY
		   ,LOC.MAIL_CD AS ZIP
		   ,LOC.BUSN_UNIT_ID AS BUSINESS_UNIT
		   ,LOC.HIER_ZONE_NBR AS ZONENBR
		   ,LOC.HIER_ZONE_NM AS ZONENAME
		   ,LOC.HIER_RGN_NBR AS REGIONNBR
		   ,LOC.HIER_RDO_NM AS REGIONRDO
		   ,LOC.HIER_DIV_NBR AS DIVISIONNBR
		   ,LOC.HIER_DDO_NM AS DIVISIONDDO
		   ,LOCNBR
		   ,LOC.LOC_NM AS LOCATION_NAME
		   ,LOC.OPEN_DT AS LOC_OPEN_DT
		   ,LOC.CLS_DT AS LOC_CLOSE_DT
		   ,BUSINESSDT
		   ,GROSS_WRITE_OFF
		   ,NET_WRITE_OFF
		   ,WORAMTSUM
		   ,NET_REVENUE
		   ,GROSS_REVENUE
		FROM WORK.QFUND_PNL PNL
		LEFT JOIN 
		EDW.D_LOCATION LOC
		ON(PNL.LOCNBR=LOC.LOC_NBR)
	WHERE COMPRESS(PRODUCT_TYPE) = 'ILP' AND INSTANCE = 'QFUND2'
;
%RUNQUIT(&job,&sub3);

PROC SQL;
CREATE TABLE WORK.RU1_LENDINGPRODUCTS_ROLLUP_PRE AS 
	SELECT * FROM QFUND2_NEW_ORIGINATIONS
		OUTER UNION CORR 
	SELECT * FROM QFUND2_ILP
;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE RU1_LENDINGPRODUCTS_ROLLUP AS 
   SELECT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BusinessDt, 
          /* NEW_ORIGINATIONS */
            (SUM(t1.NEW_ORIGINATIONS)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (SUM(t1.NEW_ADV_AMT)) AS NEW_ADV_AMT, 
          /* NEW_ADVFEE_AMT */
            (SUM(t1.NEW_ADVFEE_AMT)) AS NEW_ADVFEE_AMT, 
          /* TOTADVRECV */
            (SUM(t1.TOTADVRECV)) FORMAT=12.2 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (SUM(t1.TOTADVFEERECV)) FORMAT=10.2 AS TOTADVFEERECV, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (SUM(t1.COMPLIANT_LOANS_OUTSTANDING)) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (SUM(t1.DEFAULT_LOANS_OUTSTANDING)) AS DEFAULT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (SUM(t1.TOTDEFAULTRECV)) FORMAT=12.2 AS TOTDEFAULTRECV, 
          /* TOTDEFAULTFEERECV */
            (SUM(0)) FORMAT=10.2 AS TOTDEFAULTFEERECV, 
          /* NSF_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_AMOUNT, 
          /* NSF_PAYMENT_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_PAYMENT_AMOUNT, 
          /* NSF_PREPAYMENT_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_PREPAYMENT_AMOUNT, 
          /* WOCNT */
            (SUM(t1.WOCNT)) AS WOCNT, 
          /* WOAMTSUM */
            (SUM(t1.WOAMTSUM)) FORMAT=14.2 AS WOAMTSUM, 
          /* WOBAMTSUM */
            (SUM(t1.WOBAMTSUM)) FORMAT=10.2 AS WOBAMTSUM, 
          /* WOBCNT */
            (SUM(t1.WOBCNT)) AS WOBCNT, 
          /* WORCNT */
            (SUM(0)) AS WORCNT, 
          /* WORAMTSUM */
            (SUM(t1.WORAMTSUM)) FORMAT=10.2 AS WORAMTSUM, 
          /* CASHAGAIN_COUNT */
            (SUM(0)) AS CASHAGAIN_COUNT, 
          /* BUYBACK_COUNT */
            (SUM(0)) AS BUYBACK_COUNT, 
          /* DEPOSIT_COUNT */
            (SUM(0)) AS DEPOSIT_COUNT, 
          /* BEGIN_PWO_AMT */
            (SUM(t1.BEGIN_PWO_AMT)) AS BEGIN_PWO_AMT, 
          /* CURRENT_PWO_AMT */
            (SUM(t1.CURRENT_PWO_AMT)) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (SUM(t1.NEXT_MONTH_PWO_AMT)) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (SUM(t1.NEXT_2_MONTH_PWO_AMT)) AS NEXT_2_MONTH_PWO_AMT, 
          /* DEFAULT_PMT */
            (SUM(t1.DEFAULT_PMT)) FORMAT=10.2 AS DEFAULT_PMT, 
          /* DEFAULT_CNT */
            (SUM(t1.DEFAULT_CNT)) AS DEFAULT_CNT, 
          /* DEFAULT_AMT */
            (SUM(t1.DEFAULT_AMT)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* GROSS_REVENUE */
            (SUM(t1.GROSS_REVENUE)) FORMAT=10.2 AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (SUM(t1.GROSS_WRITE_OFF)) FORMAT=10.2 AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (SUM(t1.NET_WRITE_OFF)) FORMAT=10.2 AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (SUM(t1.NET_REVENUE)) FORMAT=10.2 AS NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            (SUM(t1.ACTUAL_DURATION_COUNT)) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (SUM(t1.ACTUAL_DURATION_DAYS)) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (SUM(t1.ACTUAL_DURATION_ADVAMT)) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (SUM(t1.ACTUAL_DURATION_FEES)) AS ACTUAL_DURATION_FEES, 
          /* AVGDURATIONDAYS */
            (SUM(0)) AS AVGDURATIONDAYS, 
          /* AVGDURATIONCNT */
            (SUM(0)) AS AVGDURATIONCNT, 
          /* HELDCNT */
            (SUM(t1.HELDCNT)) AS HELDCNT, 
          /* PASTDUECNT_1 */
            (SUM(t1.PASTDUECNT_1)) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (SUM(t1.PASTDUEAMT_1)) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* OVERSHORTAMT */
            (SUM(OVERSHORTAMT)) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (SUM(HOLDOVERAMT)) AS HOLDOVERAMT, 
          /* ADVAMTSUM */
            (SUM(t1.ADVAMTSUM)) FORMAT=14.2 AS ADVAMTSUM, 
          /* AGNADVSUM */
            (SUM(t1.AGNADVSUM)) FORMAT=14.2 AS AGNADVSUM, 
          /* REPMTPLANCNT */
            (SUM(0)) AS REPMTPLANCNT, 
          /* ADVCNT */
            (SUM(t1.ADVCNT)) AS ADVCNT, 
          /* AVGADVAMT */
            (SUM(0)) FORMAT=10.2 AS AVGADVAMT, 
          /* AVGDURATION */
            (SUM(0)) FORMAT=10.2 AS AVGDURATION, 
          /* AVGFEEAMT */
            (SUM(0)) FORMAT=10.2 AS AVGFEEAMT, 
          /* PASTDUEAMT_2 */
            (SUM(0)) FORMAT=12.2 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (SUM(0)) FORMAT=11. AS PASTDUECNT_2, 
          /* REFINANCE_CNT */
            (SUM(t1.REFINANCE_CNT)) AS REFINANCE_CNT, 
          /* AGNCNT */
            (SUM(t1.AGNCNT)) AS AGNCNT, 
          /* POSSESSION_AMT */
            (SUM(0)) FORMAT=21.4 AS POSSESSION_AMT, 
          /* POSSESSION_CNT */
            (SUM(0)) AS POSSESSION_CNT, 
          /* PASTDUEAMT_3 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (SUM(0)) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (SUM(0)) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (SUM(0)) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (SUM(0)) AS PASTDUECNT_6, 
          /* BLACK_BOOK_VALUE */
            (SUM(0)) AS BLACK_BOOK_VALUE, 
          /* SOLD_AMOUNT */
            (SUM(0)) FORMAT=21.4 AS SOLD_AMOUNT, 
          /* agnamtsum */
            (SUM(0)) AS agnamtsum, 
          /* RCC_IN_PROCESS */
            (SUM(0)) AS RCC_IN_PROCESS, 
          /* RCC_INELIGIBLE */
            (SUM(0)) FORMAT=11. AS RCC_INELIGIBLE, 
          /* ADVAMT */
            (SUM(0)) FORMAT=12.2 AS ADVAMT, 
          /* CASHAGAIN_AMOUNT */
            (SUM(0)) FORMAT=12.2 AS CASHAGAIN_AMOUNT, 
          /* SOLD_COUNT */
            (SUM(0)) FORMAT=12.2 AS SOLD_COUNT, 
          /* NET_WRITE_OFF_NEW */
            (SUM(0)) AS NET_WRITE_OFF_NEW, 
          /* GROSS_REVENUE_NEW */
            (SUM(0)) AS GROSS_REVENUE_NEW, 
          /* GROSS_WRITE_OFF_NEW */
            (SUM(0)) FORMAT=12.2 AS GROSS_WRITE_OFF_NEW, 
          /* NET_REVENUE_NEW */
            (SUM(0)) AS NET_REVENUE_NEW, 
          /* WORAMTSUM_OLD */
            (SUM(0)) FORMAT=12.2 AS WORAMTSUM_OLD, 
          /* FIRST_PRESENTMENT_CNT */
            (SUM(0)) AS FIRST_PRESENTMENT_CNT, 
          /* SATISFIED_PAYMENT_CNT */
            (SUM(0)) AS SATISFIED_PAYMENT_CNT, 
          /* DEL_RECV_AMT */
            (SUM(0)) AS DEL_RECV_AMT, 
          /* DEL_RECV_CNT */
            (SUM(0)) AS DEL_RECV_CNT
      FROM WORK.RU1_LENDINGPRODUCTS_ROLLUP_PRE t1
      GROUP BY t1.Product,
               t1.PRODUCT_DESC,
               t1.pos,
               t1.INSTANCE,
               t1.brandcd,
               t1.bankmodel,
               t1.COUNTRYCD,
               t1.STATE,
               t1.CITY,
               t1.ZIP,
               t1.BUSINESS_UNIT,
               t1.ZONENBR,
               t1.ZONENAME,
               t1.REGIONNBR,
               t1.REGIONRDO,
               t1.DIVISIONNBR,
               t1.DIVISIONDDO,
               t1.LOCNBR,
               t1.Location_Name,
               t1.LOC_OPEN_DT,
               t1.LOC_CLOSE_DT,
               t1.BusinessDt;
%RUNQUIT(&job,&sub3);

%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
LIBNAME NORECV "E:\SHARED\CADA\SAS DATA\DATAMART\SCOCHRAN";

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub3);

DATA WORK.A&END_DT;
	X = &ENDINGDT;
	FORMAT X MMDDYY10.;
%RUNQUIT(&job,&sub3);

LIBNAME BIOR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR;

data thursdaydates_tmp1;
	do i = "1JAN2000"d to today();
		businessdt = i;
		dayname = compress(put(businessdt,downame.));
		output;
	end;
	format businessdt mmddyy10.;
%RUNQUIT(&job,&sub3);

proc sql;
	create table thursdaydates_tmp2 as
		select t1.*, t2.holidayname
		  from thursdaydates_tmp1 t1
			LEFT JOIN bior.i_holidays t2 on (t1.businessdt = datepart(t2.holiday_dt))
		 order by t1.businessdt desc;
%RUNQUIT(&job,&sub3);

data thursdaydates_tmp3;
	set thursdaydates_tmp2;
	priordayholiday = lag1(holidayname);
	priordate = lag1(businessdt);
	if dayname = 'Thursday'
		AND businessdt ~= intnx('month',businessdt,0,'end')
		AND holidayname = ''
		THEN ThursdayWeek = 'Y';
	ELSE
		if dayname = 'Wednesday'
			AND (priordayholiday ~= ''
			 OR priordate = intnx('month',businessdt,0,'end'))
			THEN ThursdayWeek = 'Y';
	ELSE
		ThursdayWeek = 'N';
	format priordate mmddyy10.;
%RUNQUIT(&job,&sub3);

data daily_summary_all_tmp2;
	set RU1_LENDINGPRODUCTS_ROLLUP;
		last_report_dt = intnx('day',today(),-1);
		lastthursdayofmonth = intnx('week.5',intnx('month',businessdt,0,'end'),0);
		if lastthursdayofmonth = intnx('month',businessdt,0,'end') 
		   	or lastthursdayofmonth = holiday('veteransusg',year(businessdt))
			or lastthursdayofmonth = holiday('veterans',year(businessdt))
			or lastthursdayofmonth = holiday('thanksgiving',year(businessdt))
			or lastthursdayofmonth = holiday('christmas',year(businessdt)) 
				then lastthursdayofmonth = intnx('day',lastthursdayofmonth,-1);
		format lastthursdayofmonth mmddyy10.;
	if businessdt = lastthursdayofmonth then lastthursday = 'Y';
			else lastthursday = 'N';
	drop lastthursdayofmonth;
	format last_report_dt mmddyy10.;
%RUNQUIT(&job,&sub3);

proc sql;
	create table work.daily_summary_all_tmp3 as
		select t1.*, t2.thursdayweek
          from work.daily_summary_all_tmp2 t1, work.thursdaydates_tmp3 t2
		 where t1.businessdt = t2.businessdt;
%RUNQUIT(&job,&sub3);

proc sort data=daily_summary_all_tmp3;
	by locnbr businessdt;
%RUNQUIT(&job,&sub3);

data last_report_date;
	set daily_summary_all_tmp3;
	by locnbr businessdt;
	loc_last_reported_dt = businessdt;
	if last.locnbr then output;
	keep locnbr loc_last_reported_dt;
	format loc_last_reported_dt mmddyy10.;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.holidays(label="HOLIDAYS") AS 
   SELECT /* holidaydt */
            (datepart(t1.HOLIDAY_DT)) FORMAT=mmddyy10. LABEL="holidaydt" AS holidaydt, 
          t1.HOLIDAYNAME
      FROM BIOR.I_HOLIDAYS t1;
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.DAILY_SUMMARY_ALL_TMP4 AS 
   SELECT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.ZIP, 
          t1.CITY, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BusinessDt, 
          t1.lastthursday, 
          t2.HOLIDAYNAME, 
          t1.ThursdayWeek, 
          t1.LAST_REPORT_DT, 
          t1.NEW_ORIGINATIONS, 
          t1.NEW_ADV_AMT, 
          t1.NEW_ADVFEE_AMT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          t1.COMPLIANT_LOANS_OUTSTANDING, 
          t1.DEFAULT_LOANS_OUTSTANDING, 
          t1.TOTDEFAULTRECV, 
          t1.TOTDEFAULTFEERECV, 
          t1.NSF_AMOUNT, 
          t1.NSF_PAYMENT_AMOUNT, 
          t1.NSF_PREPAYMENT_AMOUNT, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          t1.WOBCNT, 
          t1.WORAMTSUM, 
          t1.WORCNT, 
          t1.CASHAGAIN_COUNT, 
          t1.BUYBACK_COUNT, 
          t1.DEPOSIT_COUNT, 
          t1.GROSS_REVENUE, 
          t1.GROSS_WRITE_OFF, 
          t1.NET_WRITE_OFF, 
          t1.NET_REVENUE, 
          t1.BEGIN_PWO_AMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.RCC_IN_PROCESS, 
          t1.RCC_INELIGIBLE, 
          t1.DEL_RECV_AMT, 
          t1.DEL_RECV_CNT, 
          t1.DEFAULT_PMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_AMT, 
          t1.ACTUAL_DURATION_COUNT, 
          t1.ACTUAL_DURATION_DAYS, 
          t1.ACTUAL_DURATION_ADVAMT, 
          t1.ACTUAL_DURATION_FEES, 
          t1.BLACK_BOOK_VALUE, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUEAMT_2, 
          t1.PASTDUECNT_2, 
          t1.PASTDUEAMT_3, 
          t1.PASTDUECNT_3, 
          t1.PASTDUEAMT_4, 
          t1.PASTDUECNT_4, 
          t1.PASTDUEAMT_5, 
          t1.PASTDUECNT_5, 
          t1.PASTDUEAMT_6, 
          t1.PASTDUECNT_6, 
          t1.REFINANCE_CNT, 
          t1.OVERSHORTAMT, 
          t1.HOLDOVERAMT, 
          t1.FIRST_PRESENTMENT_CNT, 
          t1.SATISFIED_PAYMENT_CNT, 
          t1.POSSESSION_AMT, 
          t1.POSSESSION_CNT, 
          t1.SOLD_AMOUNT, 
          t1.SOLD_COUNT, 
          t1.REPMTPLANCNT AS REPMTPLANCNT1, 
          t1.ADVCNT, 
          t1.AVGADVAMT, 
          t1.AVGDURATION, 
          t1.AVGFEEAMT, 
          t1.ADVAMTSUM, 
          t1.AVGDURATIONDAYS, 
          t1.AVGDURATIONCNT, 
          t1.HELDCNT, 
          t1.REPMTPLANCNT, 
          t1.AGNCNT
      FROM WORK.DAILY_SUMMARY_ALL_TMP3 t1
           LEFT JOIN WORK.HOLIDAYS t2 ON (t1.BusinessDt = t2.holidaydt);
%RUNQUIT(&job,&sub3);

PROC SQL;
   CREATE TABLE WORK.DAILY_SUMMARY_ALL_PreLoad1_pre AS 
   SELECT DISTINCT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BusinessDt, 
          /* LAST_REPORT_DT */
            (CASE WHEN WEEKDAY(TODAY()) = 2 THEN TODAY()-2 ELSE TODAY()-1 END) FORMAT=MMDDYY10. AS LAST_REPORT_DT, 
          t2.loc_last_reported_dt, 
          t3.Latitude, 
          t3.Longitude, 
          t1.HOLIDAYNAME, 
          t1.lastthursday, 
          t1.ThursdayWeek, 
          /* NEW_ADV_AMT */
            (CASE 
               WHEN . = t1.NEW_ADV_AMT THEN 0
               ELSE t1.NEW_ADV_AMT
            END) AS NEW_ADV_AMT, 
          /* NEW_ORIGINATIONS */
            (CASE 
               WHEN . = t1.NEW_ORIGINATIONS THEN 0
               ELSE t1.NEW_ORIGINATIONS
            END) AS NEW_ORIGINATIONS, 
          /* NEW_ADVFEE_AMT */
            (CASE 
               WHEN . = t1.NEW_ADVFEE_AMT THEN 0
               ELSE t1.NEW_ADVFEE_AMT
            END) AS NEW_ADVFEE_AMT, 
          /* TOTADVRECV */
            (CASE 
               WHEN . = t1.TOTADVRECV THEN 0
               ELSE t1.TOTADVRECV
            END) FORMAT=22.2 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (CASE 
               WHEN . = t1.TOTADVFEERECV THEN 0
               ELSE t1.TOTADVFEERECV
            END) FORMAT=10.2 AS TOTADVFEERECV, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (CASE 
               WHEN . = t1.DEFAULT_LOANS_OUTSTANDING THEN 0
               ELSE t1.DEFAULT_LOANS_OUTSTANDING
            END) AS DEFAULT_LOANS_OUTSTANDING, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (CASE 
               WHEN . = t1.COMPLIANT_LOANS_OUTSTANDING THEN 0
               ELSE t1.COMPLIANT_LOANS_OUTSTANDING
            END) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (CASE 
               WHEN . = t1.TOTDEFAULTRECV THEN 0
               ELSE t1.TOTDEFAULTRECV
            END) FORMAT=22.2 AS TOTDEFAULTRECV, 
          /* TOTDEFAULTFEERECV */
            (CASE 
               WHEN . = t1.TOTDEFAULTFEERECV THEN 0
               ELSE t1.TOTDEFAULTFEERECV
            END) FORMAT=10.2 AS TOTDEFAULTFEERECV, 
          /* NSF_AMOUNT */
            (CASE 
               WHEN . = t1.NSF_AMOUNT THEN 0
               ELSE t1.NSF_AMOUNT
            END) FORMAT=10.2 AS NSF_AMOUNT, 
          /* NSF_PAYMENT_AMOUNT */
            (CASE 
               WHEN . = t1.NSF_PAYMENT_AMOUNT THEN 0
               ELSE t1.NSF_PAYMENT_AMOUNT
            END) FORMAT=10.2 AS NSF_PAYMENT_AMOUNT, 
          /* NSF_PREPAYMENT_AMOUNT */
            (CASE 
               WHEN . = t1.NSF_PREPAYMENT_AMOUNT THEN 0
               ELSE t1.NSF_PREPAYMENT_AMOUNT
            END) FORMAT=10.2 AS NSF_PREPAYMENT_AMOUNT, 
          /* WOAMTSUM */
            (CASE 
               WHEN . = t1.WOAMTSUM THEN 0
               ELSE t1.WOAMTSUM
            END) FORMAT=22.2 AS WOAMTSUM, 
          /* WOCNT */
            (CASE 
               WHEN . = t1.WOCNT THEN 0
               ELSE t1.WOCNT
            END) AS WOCNT, 
          /* WOBAMTSUM */
            (CASE 
               WHEN . = t1.WOBAMTSUM THEN 0
               ELSE t1.WOBAMTSUM
            END) FORMAT=10.2 AS WOBAMTSUM, 
          /* WOBCNT */
            (CASE 
               WHEN . = t1.WOBCNT THEN 0
               ELSE t1.WOBCNT
            END) AS WOBCNT, 
          /* WORAMTSUM */
            (CASE 
               WHEN . = t1.WORAMTSUM THEN 0
               ELSE t1.WORAMTSUM
            END) FORMAT=22.2 AS WORAMTSUM, 
          /* WORCNT */
            (CASE 
               WHEN . = t1.WORCNT THEN 0
               ELSE t1.WORCNT
            END) AS WORCNT, 
          /* CASHAGAIN_COUNT */
            (CASE 
               WHEN . = t1.CASHAGAIN_COUNT THEN 0
               ELSE t1.CASHAGAIN_COUNT
            END) AS CASHAGAIN_COUNT, 
          /* BUYBACK_COUNT */
            (CASE 
               WHEN . = t1.BUYBACK_COUNT THEN 0
               ELSE t1.BUYBACK_COUNT
            END) AS BUYBACK_COUNT, 
          /* DEPOSIT_COUNT */
            (CASE 
               WHEN . = t1.DEPOSIT_COUNT THEN 0
               ELSE t1.DEPOSIT_COUNT
            END) AS DEPOSIT_COUNT, 
          /* GROSS_REVENUE */
            (CASE 
               WHEN . = t1.GROSS_REVENUE THEN 0
               ELSE t1.GROSS_REVENUE
            END) FORMAT=22.2 AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (CASE 
               WHEN . = t1.GROSS_WRITE_OFF THEN 0
               ELSE t1.GROSS_WRITE_OFF
            END) FORMAT=22.2 AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (CASE 
               WHEN . = t1.NET_WRITE_OFF THEN 0
               ELSE t1.NET_WRITE_OFF
            END) FORMAT=22.2 AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (CASE 
               WHEN . = t1.NET_REVENUE THEN 0
               ELSE t1.NET_REVENUE
            END) FORMAT=22.2 AS NET_REVENUE, 
          /* BEGIN_PWO_AMT */
            (CASE 
               WHEN . = t1.BEGIN_PWO_AMT THEN 0
               ELSE t1.BEGIN_PWO_AMT
            END) AS BEGIN_PWO_AMT, 
          /* CURRENT_PWO_AMT */
            (CASE 
               WHEN . = t1.CURRENT_PWO_AMT THEN 0
               ELSE t1.CURRENT_PWO_AMT
            END) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (CASE 
               WHEN . = t1.NEXT_MONTH_PWO_AMT THEN 0
               ELSE t1.NEXT_MONTH_PWO_AMT
            END) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (CASE 
               WHEN . = t1.NEXT_2_MONTH_PWO_AMT THEN 0
               ELSE t1.NEXT_2_MONTH_PWO_AMT
            END) AS NEXT_2_MONTH_PWO_AMT, 
          /* RCC_IN_PROCESS */
            (CASE 
               WHEN . = t1.RCC_IN_PROCESS THEN 0
               ELSE t1.RCC_IN_PROCESS
            END) AS RCC_IN_PROCESS, 
          /* RCC_INELIGIBLE */
            (CASE 
               WHEN . = t1.RCC_INELIGIBLE THEN 0
               ELSE t1.RCC_INELIGIBLE
            END) FORMAT=11. AS RCC_INELIGIBLE, 
          /* DEL_RECV_AMT */
            (CASE 
               WHEN . = t1.DEL_RECV_AMT THEN 0
               ELSE t1.DEL_RECV_AMT
            END) AS DEL_RECV_AMT, 
          /* DEL_RECV_CNT */
            (CASE 
               WHEN . = t1.DEL_RECV_CNT THEN 0
               ELSE t1.DEL_RECV_CNT
            END) AS DEL_RECV_CNT, 
          /* DEFAULT_PMT */
            (CASE 
               WHEN . = t1.DEFAULT_PMT THEN 0
               ELSE t1.DEFAULT_PMT
            END) FORMAT=10.2 AS DEFAULT_PMT, 
          /* DEFAULT_CNT */
            (CASE 
               WHEN . = t1.DEFAULT_CNT THEN 0
               ELSE t1.DEFAULT_CNT
            END) AS DEFAULT_CNT, 
          /* DEFAULT_AMT */
            (CASE 
               WHEN . = t1.DEFAULT_AMT THEN 0
               ELSE t1.DEFAULT_AMT
            END) FORMAT=10.2 AS DEFAULT_AMT, 
          /* ACTUAL_DURATION_COUNT */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_COUNT THEN 0
               ELSE t1.ACTUAL_DURATION_COUNT
            END) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_DAYS THEN 0
               ELSE t1.ACTUAL_DURATION_DAYS
            END) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_ADVAMT THEN 0
               ELSE t1.ACTUAL_DURATION_ADVAMT
            END) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_FEES THEN 0
               ELSE t1.ACTUAL_DURATION_FEES
            END) AS ACTUAL_DURATION_FEES, 
          /* AVGDURATIONDAYS */
            (CASE 
               WHEN . = t1.AVGDURATIONDAYS THEN 0
               ELSE t1.AVGDURATIONDAYS
            END) AS AVGDURATIONDAYS, 
          /* AVGDURATIONCNT */
            (CASE 
               WHEN . = t1.AVGDURATIONCNT THEN 0
               ELSE t1.AVGDURATIONCNT
            END) AS AVGDURATIONCNT, 
          /* BLACK_BOOK_VALUE */
            (CASE 
               WHEN . = t1.BLACK_BOOK_VALUE THEN 0
               ELSE t1.BLACK_BOOK_VALUE
            END) AS BLACK_BOOK_VALUE, 
          /* PASTDUECNT_1 */
            (CASE 
               WHEN . = t1.PASTDUECNT_1 THEN 0
               ELSE t1.PASTDUECNT_1
            END) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_1 THEN 0
               ELSE t1.PASTDUEAMT_1
            END) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* PASTDUEAMT_2 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_2 THEN 0
               ELSE t1.PASTDUEAMT_2
            END) FORMAT=12.2 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (CASE 
               WHEN . = t1.PASTDUECNT_2 THEN 0
               ELSE t1.PASTDUECNT_2
            END) FORMAT=11. AS PASTDUECNT_2, 
          /* PASTDUEAMT_3 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_3 THEN 0
               ELSE t1.PASTDUEAMT_3
            END) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (CASE 
               WHEN . = t1.PASTDUECNT_3 THEN 0
               ELSE t1.PASTDUECNT_3
            END) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_4 THEN 0
               ELSE t1.PASTDUEAMT_4
            END) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (CASE 
               WHEN . = t1.PASTDUECNT_4 THEN 0
               ELSE t1.PASTDUECNT_4
            END) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_5 THEN 0
               ELSE t1.PASTDUEAMT_5
            END) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (CASE 
               WHEN . = t1.PASTDUECNT_5 THEN 0
               ELSE t1.PASTDUECNT_5
            END) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_6 THEN 0
               ELSE t1.PASTDUEAMT_6
            END) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (CASE 
               WHEN . = t1.PASTDUECNT_6 THEN 0
               ELSE t1.PASTDUECNT_6
            END) AS PASTDUECNT_6, 
          /* REFINANCE_CNT */
            (CASE 
               WHEN . = t1.REFINANCE_CNT THEN 0
               ELSE t1.REFINANCE_CNT
            END) AS REFINANCE_CNT, 
          /* OVERSHORTAMT */
            (CASE 
               WHEN . = t1.OVERSHORTAMT THEN 0
               ELSE t1.OVERSHORTAMT
            END) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (CASE 
               WHEN . = t1.HOLDOVERAMT THEN 0
               ELSE t1.HOLDOVERAMT
            END) AS HOLDOVERAMT, 
          /* FIRST_PRESENTMENT_CNT */
            (CASE 
               WHEN . = t1.FIRST_PRESENTMENT_CNT THEN 0
               ELSE t1.FIRST_PRESENTMENT_CNT
            END) AS FIRST_PRESENTMENT_CNT, 
          /* SATISFIED_PAYMENT_CNT */
            (CASE 
               WHEN . = t1.SATISFIED_PAYMENT_CNT THEN 0
               ELSE t1.SATISFIED_PAYMENT_CNT
            END) AS SATISFIED_PAYMENT_CNT, 
          /* POSSESSION_AMT */
            (CASE 
               WHEN . = t1.POSSESSION_AMT THEN 0
               ELSE t1.POSSESSION_AMT
            END) FORMAT=21.4 AS POSSESSION_AMT, 
          /* POSSESSION_CNT */
            (CASE 
               WHEN . = t1.POSSESSION_CNT THEN 0
               ELSE t1.POSSESSION_CNT
            END) AS POSSESSION_CNT, 
          /* SOLD_AMOUNT */
            (CASE 
               WHEN . = t1.SOLD_AMOUNT THEN 0
               ELSE t1.SOLD_AMOUNT
            END) FORMAT=21.4 AS SOLD_AMOUNT, 
          /* SOLD_COUNT */
            (CASE 
               WHEN . = t1.SOLD_COUNT THEN 0
               ELSE t1.SOLD_COUNT
            END) FORMAT=12.2 AS SOLD_COUNT, 
          /* ADVCNT */
            (CASE 
               WHEN . = t1.ADVCNT THEN 0
               ELSE t1.ADVCNT
            END) AS ADVCNT, 
          /* AVGADVAMT */
            (CASE 
               WHEN . = t1.AVGADVAMT THEN 0
               ELSE t1.AVGADVAMT
            END) FORMAT=10.2 AS AVGADVAMT, 
          /* AVGDURATION */
            (CASE 
               WHEN . = t1.AVGDURATION THEN 0
               ELSE t1.AVGDURATION
            END) FORMAT=10.2 AS AVGDURATION, 
          /* AVGFEEAMT */
            (CASE 
               WHEN . = t1.AVGFEEAMT THEN 0
               ELSE t1.AVGFEEAMT
            END) FORMAT=10.2 AS AVGFEEAMT, 
          /* ADVAMTSUM */
            (CASE 
               WHEN . = t1.ADVAMTSUM THEN 0
               ELSE t1.ADVAMTSUM
            END) FORMAT=14.2 AS ADVAMTSUM, 
          /* HELDCNT */
            (CASE 
               WHEN . = t1.HELDCNT THEN 0
               ELSE t1.HELDCNT
            END) AS HELDCNT, 
          /* REPMTPLANCNT */
            (CASE 
               WHEN . = t1.REPMTPLANCNT THEN 0
               ELSE t1.REPMTPLANCNT
            END) AS REPMTPLANCNT, 
          /* AGNCNT */
            (CASE 
               WHEN . = t1.AGNCNT THEN 0
               ELSE t1.AGNCNT
            END) AS AGNCNT
      FROM WORK.DAILY_SUMMARY_ALL_TMP4 t1
           INNER JOIN WORK.LAST_REPORT_DATE t2 ON (t1.LOCNBR = t2.LOCNBR)
           LEFT JOIN SKYNET.LOCATION_LATLONG t3 ON (t1.LOCNBR = t3.locnbr)
           LEFT JOIN SKYNET.PS2_LENDING_REVEXP t5 ON (t1.Product = t5.Product) AND (t1.pos = t5.pos) AND (t1.INSTANCE = 
          t5.INSTANCE) AND (t1.LOCNBR = t5.locnbr) AND (t1.BusinessDt = t5.BusinessDt)
      WHERE t1.BusinessDt BETWEEN INTNX('MONTH',TODAY(),-24,'B') AND TODAY()-1
      ORDER BY t1.LOCNBR,
               t1.BusinessDt,
               t1.Product,
               t1.INSTANCE,
               t1.PRODUCT_DESC;
%RUNQUIT(&job,&sub3);

DATA DAILY_SUMMARY_ALL_PRE_QF2;
	SET WORK.DAILY_SUMMARY_ALL_PRELOAD1_PRE;
		IF PRODUCT_DESC ^= "AL ETL" AND SUM(NEW_ADV_AMT,
		   NEW_ORIGINATIONS,
		   NEW_ADVFEE_AMT,
		   TOTADVRECV,
		   TOTADVFEERECV,
		   COMPLIANT_LOANS_OUTSTANDING,
		   DEFAULT_LOANS_OUTSTANDING,
		   TOTDEFAULTRECV,
		   TOTDEFAULTFEERECV,
		   NSF_AMOUNT,
		   NSF_PAYMENT_AMOUNT,
		   NSF_PREPAYMENT_AMOUNT,
		   WOAMTSUM,
		   WOBAMTSUM,
		   WORAMTSUM,
		   CASHAGAIN_COUNT,
		   BUYBACK_COUNT,
		   DEPOSIT_COUNT,
		   GROSS_REVENUE,
		   GROSS_WRITE_OFF,
		   NET_WRITE_OFF,
		   NET_REVENUE,
		   BEGIN_PWO_AMT,
		   CURRENT_PWO_AMT,
		   NEXT_MONTH_PWO_AMT,
		   NEXT_2_MONTH_PWO_AMT,
		   RCC_IN_PROCESS,
		   RCC_INELIGIBLE,
		   DEFAULT_PMT,
		   DEFAULT_CNT,
		   DEFAULT_AMT,
		   ACTUAL_DURATION_COUNT,
		   ACTUAL_DURATION_DAYS,
		   ACTUAL_DURATION_ADVAMT,
		   ACTUAL_DURATION_FEES,
		   BLACK_BOOK_VALUE,
		   PASTDUECNT_1,
	   	   PASTDUEAMT_1,
		   PASTDUEAMT_2,
		   PASTDUECNT_2,
		   PASTDUEAMT_3,
		   PASTDUECNT_3,
		   PASTDUEAMT_4,
		   PASTDUECNT_4,
		   PASTDUEAMT_5,
		   PASTDUECNT_5,
		   PASTDUEAMT_6,
		   PASTDUECNT_6,
		   SOLD_AMOUNT,
		   SOLD_COUNT,		
		   ADVCNT,
		   AVGADVAMT,
		   AVGDURATION,
		   AVGFEEAMT,
		   ADVAMTSUM,
		   ADVAMTSUM,
		   HELDCNT,
		   AGNCNT
		   ) = 0 THEN DELETE;
%RUNQUIT(&job,&sub3);


%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
LIBNAME BIOR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR;

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";

/*WAITFOR CUST LIFECYCLE TO BE READY FOR THE DAY*/

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND2_INSTALL_STATUS = 'WAITING_CL'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub3);

%MACRO WAITFORCUSTLIFE();


	%DO %UNTIL (%EVAL(&COUNT_R. >= 1));	
		PROC SQL;
			CREATE TABLE CUST_LIFE_CHECK_TODAY AS
			SELECT INSTANCE
				  ,MAX(BUSINESS_DATE)	AS BUSINESSDT
			FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE INSTANCE = 'QFUND2'
			GROUP BY INSTANCE
		;
		QUIT;

		DATA _NULL_;
			FORMAT WEEKDAY $20.;
			DAYOFWEEK = WEEKDAY(DATE());
			IF DAYOFWEEK = 1 THEN WEEKDAY = 'SUNDAY';
			ELSE IF DAYOFWEEK = 2 THEN WEEKDAY = 'MONDAY';
			ELSE IF DAYOFWEEK = 3 THEN WEEKDAY = 'TUESDAY';
			ELSE IF DAYOFWEEK = 4 THEN WEEKDAY = 'WEDNESDAY';
			ELSE IF DAYOFWEEK = 5 THEN WEEKDAY = 'THURSDAY';
			ELSE IF DAYOFWEEK = 6 THEN WEEKDAY = 'FRIDAY';
			ELSE IF DAYOFWEEK = 7 THEN WEEKDAY = 'SATURDAY';
			CALL SYMPUTX("DAYOFWEEK",WEEKDAY,'G');
		RUN;

		%IF &DAYOFWEEK. = SUNDAY
			OR &DAYOFWEEK. = TUESDAY
			OR &DAYOFWEEK. = WEDNESDAY
			OR &DAYOFWEEK. = THURSDAY
			OR &DAYOFWEEK. = FRIDAY
			OR &DAYOFWEEK. = SATURDAY %THEN 
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'QFUND2' AND BUSINESSDT >= DHMS(TODAY()-1,00,00,00)
					;
					QUIT;

				%END;
		%ELSE %IF &DAYOFWEEK. = MONDAY %THEN
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'QFUND2' AND BUSINESSDT >= DHMS(TODAY()-2,00,00,00)
					;
					QUIT;
				%END;
					
		%PUT THE COUNT IS EQUAL TO : &COUNT_R;

		%IF %EVAL(&COUNT_R. < 1) %THEN 
			%DO;
				/*SLEEPS FOR 300 SECONDS (5 MINUTES) UNTIL IT FINDS 16 FINISHED TABLES, IT WILL LOOP FOREVER UNTIL THE 16 FINISHED TABLES*/
				DATA SLEEP;
					CALL SLEEP(300,1);
				RUN;
			%END;
	%END;

%MEND ;

%WAITFORCUSTLIFE

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND2_INSTALL_STATUS = 'RUNNING'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub3);*/;


PROC SQL;
   CREATE TABLE WORK.PROD_DESC_CHANGE AS 
   SELECT /* BUSINESS_DATE */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESS_DATE, 
          t1.LOCATION_NBR, 
          t1.INSTANCE, 
          t1.PRODUCT, 
          /* PRODUCT_DESC */
            (CASE WHEN (INSTANCE = 'EAPROD1' AND T1.BUSINESS_DATE >= T2.DEALDATE  AND T2.DEALDATE ^= .) 
												    THEN "TX CSO Cash Advance" 
				  WHEN PRODUCTDESC = 'TX CSO' 		THEN 'EADV PAYDAY'
                  WHEN PRODUCTDESC = 'TEXAS TITLE' THEN 'TX TITLE' ELSE PRODUCTDESC END) AS PRODUCT_DESC, 
          /* NEW_CUST_CNT */
            (SUM(t1.NEW_CUST_CNT)) AS NEW_CUST_CNT, 
          /* REDEEM_CUST_CNT */
            (SUM(t1.REDEEM_CUST_CNT)) AS REDEEM_CUST_CNT, 
          /* NEW_REPEAT_CUST_CNT */
            (SUM(t1.NEW_REPEAT_CUST_CNT)) AS NEW_REPEAT_CUST_CNT, 
          /* REACTIVE_CUST_CNT */
            (SUM(t1.REACTIVE_CUST_CNT)) AS REACTIVE_CUST_CNT, 
          /* ACTIVE_CUST_CNT */
            (SUM(t1.ACTIVE_CUST_CNT)) AS ACTIVE_CUST_CNT, 
          /* INACTIVE_CUST_CNT */
            (SUM(t1.INACTIVE_CUST_CNT)) AS INACTIVE_CUST_CNT
      FROM BIOR.CUST_CATEGORY_DAILY_COUNT T1
	  LEFT JOIN SKYNET.TX_UC_DATEBYLOC AS T2
			ON (T1.LOCATION_NBR = T2.LOCNBR)
	  WHERE T1.INSTANCE = 'QFUND2' AND T1.BUSINESS_DATE >= DHMS(TODAY()-5,00,00,00)
      GROUP BY (CALCULATED BUSINESS_DATE),
               t1.LOCATION_NBR,
               t1.INSTANCE,
               t1.PRODUCT,
               (CALCULATED PRODUCT_DESC)
;
%RUNQUIT(&job,&sub3);


PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_QF2_1 AS 
   SELECT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          /* BUSINESSDT */
            (DHMS(t1.BusinessDt,00,00,00)) FORMAT=DATETIME20. AS BUSINESSDT, 
          /* LAST_REPORT_DT */
            (DHMS(DATE()-1,00,00,00)) FORMAT=DATETIME20. AS LAST_REPORT_DT, 
          /* LOC_LAST_REPORTED_DT */
            (DHMS(t1.loc_last_reported_dt,00,00,00)) FORMAT=DATETIME20. AS LOC_LAST_REPORTED_DT, 
          t1.Latitude, 
          t1.Longitude, 
          t1.HOLIDAYNAME, 
          t1.lastthursday, 
          t1.ThursdayWeek, 
          t1.NEW_ADV_AMT, 
          t1.NEW_ORIGINATIONS, 
          t1.NEW_ADVFEE_AMT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          t1.DEFAULT_LOANS_OUTSTANDING, 
          t1.COMPLIANT_LOANS_OUTSTANDING, 
          t1.TOTDEFAULTRECV, 
          t1.TOTDEFAULTFEERECV, 
          t1.NSF_AMOUNT, 
          t1.NSF_PAYMENT_AMOUNT, 
          t1.NSF_PREPAYMENT_AMOUNT, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          t1.WOBCNT, 
          t1.WORAMTSUM, 
          t1.WORCNT, 
          t1.CASHAGAIN_COUNT, 
          t1.BUYBACK_COUNT, 
          t1.DEPOSIT_COUNT, 
          t1.GROSS_REVENUE, 
          t1.GROSS_WRITE_OFF, 
          t1.NET_WRITE_OFF, 
          t1.NET_REVENUE, 
          t1.BEGIN_PWO_AMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.RCC_IN_PROCESS, 
          t1.RCC_INELIGIBLE, 
          t1.DEL_RECV_AMT, 
          t1.DEL_RECV_CNT, 
          t1.DEFAULT_PMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_AMT, 
          /* NEWCUSTCNTCOMPANY */
            (CASE WHEN SUM(t2.NEW_CUST_CNT,t2.NEW_REPEAT_CUST_CNT) = . THEN 0 ELSE 
            SUM(t2.NEW_CUST_CNT,t2.NEW_REPEAT_CUST_CNT) END) AS NEWCUSTCNTCOMPANY, 
          /* REDEEM_CUSTOMER_CNT */
            (CASE 
               WHEN . = t2.REDEEM_CUST_CNT THEN 0
               ELSE t2.REDEEM_CUST_CNT
            END) AS REDEEM_CUSTOMER_CNT, 
          /* REACTIVE_CUSTOMER_CNT */
            (CASE 
               WHEN . = t2.REACTIVE_CUST_CNT THEN 0
               ELSE t2.REACTIVE_CUST_CNT
            END) AS REACTIVE_CUSTOMER_CNT, 
          /* ACTIVE_CUST_CNT */
            (CASE 
               WHEN . = t2.ACTIVE_CUST_CNT THEN 0
               ELSE t2.ACTIVE_CUST_CNT
            END) AS ACTIVE_CUST_CNT, 
          /* INACTIVE_CUST_CNT */
            (CASE 
               WHEN . = t2.INACTIVE_CUST_CNT THEN 0
               ELSE t2.INACTIVE_CUST_CNT
            END) AS INACTIVE_CUST_CNT, 
          t1.ACTUAL_DURATION_COUNT, 
          t1.ACTUAL_DURATION_DAYS, 
          t1.ACTUAL_DURATION_ADVAMT, 
          t1.ACTUAL_DURATION_FEES, 
          t1.AVGDURATIONDAYS, 
          t1.AVGDURATIONCNT, 
          t1.BLACK_BOOK_VALUE, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUEAMT_2, 
          t1.PASTDUECNT_2, 
          t1.PASTDUEAMT_3, 
          t1.PASTDUECNT_3, 
          t1.PASTDUEAMT_4, 
          t1.PASTDUECNT_4, 
          t1.PASTDUEAMT_5, 
          t1.PASTDUECNT_5, 
          t1.PASTDUEAMT_6, 
          t1.PASTDUECNT_6, 
          t1.REFINANCE_CNT, 
          t1.OVERSHORTAMT, 
          t1.HOLDOVERAMT, 
          t1.FIRST_PRESENTMENT_CNT, 
          t1.SATISFIED_PAYMENT_CNT, 
          t1.POSSESSION_AMT, 
          t1.POSSESSION_CNT, 
          t1.SOLD_AMOUNT, 
          t1.SOLD_COUNT, 
          t1.ADVCNT, 
          t1.REPMTPLANCNT, 
          t1.AVGADVAMT, 
          t1.AVGDURATION, 
          t1.AVGFEEAMT, 
          t1.ADVAMTSUM, 
          t1.HELDCNT, 
          t1.AGNCNT,
		  "STOREFRONT"				AS CHANNELCD
      FROM DAILY_SUMMARY_ALL_PRE_QF2 t1
           LEFT JOIN WORK.PROD_DESC_CHANGE t2 ON (t1.INSTANCE = t2.INSTANCE) AND (t1.PRODUCT_DESC = t2.PRODUCT_DESC) 
          AND (T1.PRODUCT = t2.PRODUCT) AND (T1.BUSINESSDT = T2.BUSINESS_DATE) AND (t1.LOCNBR = t2.LOCATION_NBR);
%RUNQUIT(&job,&sub3);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub3);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND1_QFUND2\QF2",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND1_QFUND2\DIR2\QF2",'G');
%RUNQUIT(&job,&sub3);

PROC SQL;
    INSERT INTO SKY.DAILYSUM_DATAMART_QF2 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM DAILY_SUMMARY_ALL_QF2_1
	WHERE BUSINESSDT >= DHMS(TODAY()-5,00,00,00);
%RUNQUIT(&job,&sub3);

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub3);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
RUN;

%PUT &TIMESTAMP;


/*KICK OFF QF2_DAILY UPLOAD*/
SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
				 '&DAILY_FILE_PATH.\TRANSPOSE QFUND2.SAS'
				 -LOG '&DAILY_LOGPATH.\TRANSPOSE_QFUND2_&TIMESTAMP..LOG'
				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
TASKNAME=TRANSPOSE_QFUND2
STATUS=TRANSPOSE_QFUND2;

/*UPLOAD QF2*/
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_QFUND2.SAS";


PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.O_DAILY_SUMMARY_ALL
		    SET LAST_REPORT_DT = TO_DATE(TO_CHAR(CURRENT_DATE-1, 'MM/DD/YYYY'), 'MM/DD/YYYY')
			WHERE INSTANCE = 'QFUND2' AND PRODUCT = 'INSTALLMENT'
			)
	BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

WAITFOR _ALL_ TRANSPOSE_QFUND2;

/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%abort cancel;
	%END;

%MEND;

%STOPPROGRAM


