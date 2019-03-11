%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";


LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";

LIBNAME ODSFIN ORACLE 
	USER=&USER
	PASSWORD=&PASSWORD
	PATH=PEDWPROD1 
	SCHEMA=SC_ODS_FIN DEFER=YES;

LIBNAME SCDMARTS ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=PEDWPROD1
	SCHEMA=SC_DMARTS DEFER=YES;

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('DAILY_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DAILY_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DAILY",'G');
%RUNQUIT(&job,&sub13);

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub13);

PROC SQL;
   CREATE TABLE WORK.DM_DAILY_SUMMARY_TMP1 AS 
   SELECT t1.FD_BUSINESS_DT, 
          t1.FI_CENTER_ID, 
          t1.FC_CATEGORY_CD, 
          t1.FC_METRIC_CD, 
          /* METRIC */
            (tranwrd(tranwrd(compress(tranwrd(TRANWRD(trim(t1.FC_METRIC_CD), ' ', '_'),'(+/-)','Change'),'/'),'__','_'),
            '-','_')) LABEL="METRIC" AS METRIC, 
          /* FI_METRIC_CNT */
            (SUM(t1.FI_METRIC_CNT)) AS FI_METRIC_CNT, 
          /* FN_METRIC_AMT */
            (SUM(t1.FN_METRIC_AMT)) FORMAT=14.2 AS FN_METRIC_AMT, 
          /* product */
            (case
              when substr(t1.fc_product_cd,3) = 'T' then 'TITLE'
              when substr(t1.fc_product_cd,1,2) = 'CS' and substr(t1.fc_product_cd,3) ~= 'T' then 'PAYDAY'
              when substr(t1.fc_product_cd,1,2) = 'CM' and substr(t1.fc_product_cd,3) ~= 'T' then 'INSTALLMENT'
            end) AS product, 
          /* product_desc */
            (case
              when t1.fc_product_cd = 'CSC' then "Single Pay Check"
              when t1.fc_product_cd = 'CSA' then "Single Pay ACH"
              when t1.fc_product_cd = 'CSU' then "Single Pay Unsecured"
              when t1.fc_product_cd = 'CMC' then "Multi Pay Check"
              when t1.fc_product_cd = 'CMA' then "Multi Pay ACH"
              when t1.fc_product_cd = 'CMU' then "Multi Pay Unsecured"
              when substr(t1.fc_product_cd,3,1) = 'T' then "Title"
              else "Unknown"
            end) AS product_desc, 
          /* pos */
            ("NEXTGEN") AS pos, 
          /* instance */
            ("NG") AS instance
      FROM SCDPEDW.DM_DAILY_SUMMARY t1
      GROUP BY t1.FD_BUSINESS_DT,
               t1.FI_CENTER_ID,
               t1.FC_CATEGORY_CD,
               t1.FC_METRIC_CD,
               (CALCULATED METRIC),
               (CALCULATED product),
               (CALCULATED product_desc),
               (CALCULATED pos),
               (CALCULATED instance);
%RUNQUIT(&job,&sub13);

data dm_daily_summary_tmp2;
	set work.dm_daily_summary_tmp1 (where=(product ~= ''));
%RUNQUIT(&job,&sub13);

proc sort data=work.dm_daily_summary_tmp2 out=work.dm_daily_summary_tmp2_pre;
	by fi_center_id fd_business_dt product product_desc pos instance;
%RUNQUIT(&job,&sub13);

proc sql; 
	create table dm_daily_summary_tmp3 as 
		select  * from dm_daily_summary_tmp2_pre
/*	 (case when FC_CATEGORY_CD = 'Portfolios' and FC_METRIC_CD = 'Default' then 1 else 0 end) NOT = 1*/
	where (case when FC_CATEGORY_CD = 'Payments' and FC_METRIC_CD = 'Current' then 1 else 0 end) NOT = 1
	and   (case when FC_CATEGORY_CD = 'Portfolios' and FC_METRIC_CD = 'WO/CO' then 1 else 0 end) NOT = 1
/*	and   (case when FC_CATEGORY_CD = 'Portfolios' and FC_METRIC_CD = 'Delinquent' then 1 else 0 end) NOT = 1*/
;
%RUNQUIT(&job,&sub13);

DATA DM_DAILY_SUMMARY_TMP2;
	SET WORK.DM_DAILY_SUMMARY_TMP3;
	IF FC_CATEGORY_CD = 'Payments' AND FC_METRIC_CD = 'Delinquent' THEN METRIC = 'Delinquent_Pmt';
	ELSE IF FC_CATEGORY_CD = 'Portfolios' AND FC_METRIC_CD = 'Default' THEN METRIC = 'Default_Recv';
%RUNQUIT(&job,&sub13);

proc transpose data=work.dm_daily_summary_tmp2 out=transposed;
	by fi_center_id fd_business_dt product product_desc pos instance;
	*id fc_metric_cd;
	id metric;
	idlabel	metric;
%RUNQUIT(&job,&sub13);

data dailycnts_pre (keep=advcnt heldcnt defaultcnt nsfcnt repmtplancnt wocnt worcnt pastduecnt_1 first_presentment_cnt satisfied_payment_cnt avgdurationdays avgdurationcnt
						 refinance_cnt defaultoutstanding delinquentcnt locnbr businessdt product product_desc pos instance)
	 dailyamts_pre (keep=advamtsum advfeeamt delinquentamt earnedfees nsfamtsum woamtsum woramtsum totadvrecv default_pmt
					totadvfeerecv totdefaultrecv totdefaultfeerecv pastdueamt_1 locnbr businessdt product product_desc pos instance);
	set transposed;
	locnbr = fi_center_id;
	businessdt = datepart(fd_business_dt);
	format businessdt mmddyy10.;

	if _NAME_ = 'FI_METRIC_CNT' then do;
		advcnt = coalesce(New_Loans,0);
		delinquentcnt = coalesce(Delinquent,0);
		heldcnt = coalesce(Current,0);
		defaultcnt = coalesce(New_Defaults,0);
		defaultoutstanding = coalesce(Default_Recv);
		nsfcnt = coalesce(NSF,0);
		repmtplancnt = coalesce(Repayment_Plans,0);
		refinance_cnt = coalesce(Refinance,0);
		wocnt = coalesce(WOCO,0);
		worcnt = coalesce(Write_Off,0);
		pastduecnt_1 = coalesce(Delinquent_Pmt,0);
		first_presentment_cnt = coalesce(First_Presentment,0);
		satisfied_payment_cnt = coalesce(Satisfied_payments,0);
		avgdurationdays = coalesce(Duration_Days,0);
		avgdurationcnt = coalesce(Duration_Count,0);
		output dailycnts_pre;
	end;

	if _NAME_ = 'FN_METRIC_AMT' then do;
		advamtsum = coalesce(New_Loans,0);
		advfeeamt = coalesce(0,0);
		delinquentamt = coalesce(Delinquent,0);
		earnedfees = coalesce(Gross_Revenue,0);
		default_pmt = coalesce(Default,0);
		nsfamtsum = coalesce(New_Defaults,0);
		woamtsum = coalesce(WOCO,0);
		woramtsum = coalesce(Write_Off,0);
		totadvrecv = coalesce(Current_Loan_Loan_Receivables,0);
		totadvfeerecv = coalesce(Current_Loan_Potential_Revenue,0);
		totdefaultrecv = coalesce(Default_Loan_Loan_Receivables,0);
		totdefaultfeerecv = coalesce(Default_Loan_Potential_Revenue,0);
		pastdueamt_1 = coalesce(Delinquent_Pmt,0);
		output dailyamts_pre;
	end;
	where fd_business_dt <= '01JUL2015:00:00:00'DT;
%RUNQUIT(&job,&sub13);

data dailycnts_post (keep=advcnt heldcnt defaultcnt nsfcnt repmtplancnt wocnt worcnt pastduecnt_1 first_presentment_cnt satisfied_payment_cnt avgdurationdays avgdurationcnt
						  refinance_cnt defaultoutstanding delinquentcnt locnbr businessdt product product_desc pos instance)
	 dailyamts_post (keep=advamtsum advfeeamt delinquentamt earnedfees nsfamtsum woamtsum woramtsum totadvrecv default_pmt
					totadvfeerecv totdefaultrecv totdefaultfeerecv pastdueamt_1 locnbr businessdt product product_desc pos instance);
	set transposed;
	locnbr = fi_center_id;
	businessdt = datepart(fd_business_dt);
	format businessdt mmddyy10.;

	if _NAME_ = 'FI_METRIC_CNT' then do;
		advcnt = coalesce(New_Originations,0);
		heldcnt = coalesce(Current,0);
		defaultcnt = coalesce(New_Defaults,0);
		defaultoutstanding = coalesce(Default_Recv,0);
		delinquentcnt = coalesce(Delinquent,0);
		nsfcnt = coalesce(NSF,0);
		repmtplancnt = coalesce(Repayment_Plans,0);
		refinance_cnt = coalesce(Refinance,0);
		wocnt = coalesce(New_WOCO,0);
		worcnt = coalesce(WOCO,0);
		pastduecnt_1 = coalesce(Delinquent_Pmt,0);
		first_presentment_cnt = coalesce(First_Presentment,0);
		satisfied_payment_cnt = coalesce(Satisfied_payments,0);
		avgdurationdays = coalesce(Duration_Days,0);
		avgdurationcnt = coalesce(Duration_Count,0);
		output dailycnts_post;
	end;

	if _NAME_ = 'FN_METRIC_AMT' then do;
		advamtsum = coalesce(New_Originations,0);
		advfeeamt = coalesce(0,0);
		default_pmt = coalesce(Default,0);
		delinquentamt = coalesce(Delinquent,0);
		earnedfees = coalesce(Gross_Revenue,0);
		nsfamtsum = coalesce(New_Defaults,0);
		woamtsum = coalesce(New_WOCO,0);
		woramtsum = coalesce(WOCO,0);
		totadvrecv = coalesce(Current_Loan_Loan_Receivables,0);
		totadvfeerecv = coalesce(Current_Loan_Potential_Revenue,0);
		totdefaultrecv = coalesce(Default_Loan_Loan_Receivables,0);
		totdefaultfeerecv = coalesce(Default_Loan_Potential_Revenue,0);
		pastdueamt_1 = coalesce(Delinquent_Pmt,0);
		output dailyamts_post;
	end;
	where fd_business_dt > '01JUL2015:00:00:00'DT;
%RUNQUIT(&job,&sub13);

PROC SQL;
CREATE TABLE WORK.DAILYCNTS AS 
SELECT * FROM WORK.DAILYCNTS_PRE
 OUTER UNION CORR 
SELECT * FROM WORK.DAILYCNTS_POST
;
%RUNQUIT(&job,&sub13);

PROC SQL;
CREATE TABLE WORK.DAILYAMTS AS 
SELECT * FROM WORK.DAILYAMTS_PRE
 OUTER UNION CORR 
SELECT * FROM WORK.DAILYAMTS_POST
;
%RUNQUIT(&job,&sub13);

proc sort data=dailycnts;
	by locnbr businessdt;
%RUNQUIT(&job,&sub13);

proc sort data=dailyamts;
	by locnbr businessdt;
%RUNQUIT(&job,&sub13);

data combined_metrics_pre;
	merge dailycnts (in=a)
		  dailyamts (in=b);
	by locnbr businessdt;
	if a or b;
%RUNQUIT(&job,&sub13);

proc sort data=combined_metrics_pre;
	by locnbr businessdt product product_desc pos instance;
%RUNQUIT(&job,&sub13);

PROC SQL;
	CREATE TABLE PWO_NG_PRE AS
		SELECT
			 FI_CENTER_ID 							  	AS LOCNBR
			,DHMS(DATEPART(FD_AS_OF_DATE),00,00,00) AS BUSINESSDT FORMAT DATETIME20.
			,SUM(CASE WHEN CALCULATED BUSINESSDT = DHMS(INTNX('MONTH',DATEPART(FD_AS_OF_DATE),0,'B'),00,00,00)
					  AND FD_PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(FD_AS_OF_DATE),0,'B'),00,00,00)
					  AND DHMS(INTNX('MONTH',DATEPART(FD_AS_OF_DATE),0,'E'),00,00,00)
					  	  THEN FN_PWO_AMT
					  ELSE 0
				 END) 								  	AS BEGIN_PWO_AMT_PRE
			,SUM(CASE WHEN FD_PWO_DATE BETWEEN DHMS(INTNX('MONTH',TODAY(),0,'B'),00,00,00) AND DHMS(INTNX('MONTH',TODAY(),0,'E'),00,00,00)
						THEN FN_PWO_AMT
					  ELSE 0
				 END) 									AS CURRENT_PWO_AMT
			,SUM(CASE WHEN FD_PWO_DATE BETWEEN DHMS(INTNX('MONTH',TODAY(),1,'B'),00,00,00) AND DHMS(INTNX('MONTH',TODAY(),1,'E'),00,00,00)
						THEN FN_PWO_AMT
					  ELSE 0
				 END) 									AS NEXT_MONTH_PWO_AMT
			,SUM(CASE WHEN FD_PWO_DATE BETWEEN DHMS(INTNX('MONTH',TODAY(),2,'B'),00,00,00) AND DHMS(INTNX('MONTH',TODAY(),2,'E'),00,00,00)
						THEN FN_PWO_AMT
					  ELSE 0
				 END) 								 	AS NEXT_2_MONTH_PWO_AMT
			,'NG' 									  	AS INSTANCE
			,CASE WHEN FC_PRODUCT_CD = 'CMA' 
					  THEN 'Multi Pay ACH'
				  WHEN FC_PRODUCT_CD = 'CSC'
				  	  THEN 'Single Pay Check'
				  WHEN FC_PRODUCT_CD = 'CST'
				  	  THEN 'Title'
			 END 									  AS PRODUCT_DESC
			,CASE WHEN FC_PRODUCT_CD = 'CMA' 
					  THEN 'INSTALLMENT'
				  WHEN FC_PRODUCT_CD = 'CSC'
				  	  THEN 'PAYDAY'
				  WHEN FC_PRODUCT_CD = 'CST'
				  	  THEN 'TITLE' 	
			 END 									  AS PRODUCT
		FROM SCDMARTS.DM_PWO
	WHERE CALCULATED BUSINESSDT >= DHMS(INTNX('MONTH',TODAY(),-36,'B'),00,00,00)
	GROUP BY
		 FI_CENTER_ID
		,CALCULATED BUSINESSDT
		,FC_PRODUCT_CD
	ORDER BY
		 FI_CENTER_ID 
	    ,CALCULATED PRODUCT_DESC
		,CALCULATED BUSINESSDT
;
%RUNQUIT(&job,&sub13);

DATA PWO_NG;
	SET PWO_NG_PRE;
	BY LOCNBR PRODUCT_DESC;
	BUSINESSDT = DATEPART(BUSINESSDT);
	IF FIRST.PRODUCT_DESC THEN IND = 'Y';
	ELSE IND = 'N';
	IF DAY(DATEPART(BUSINESSDT)) = 1 OR FIRST.LOCNBR OR IND = 'Y' THEN 
		DO;
			BEGIN_PWO_AMT = BEGIN_PWO_AMT_PRE;
			RETAIN BEGIN_PWO_AMT;
		END;
	FORMAT BUSINESSDT MMDDYY10.;
DROP BEGIN_PWO_AMT_PRE IND;
%RUNQUIT(&job,&sub13);

DATA BEGIN_PWO_AMT;
	SET WORK.PWO_NG;
	MONTH = MONTH(BUSINESSDT);
	YEAR = YEAR(BUSINESSDT);
	WHERE DAY(BUSINESSDT) = 1;
	KEEP LOCNBR BUSINESSDT BEGIN_PWO_AMT MONTH YEAR PRODUCT;
%RUNQUIT(&job,&sub13);

PROC SQL;
   CREATE TABLE WORK.COMBINED_METRICS AS 
   SELECT t1.product, 
          t1.product_desc, 
          t1.pos, 
          t1.instance, 
          t1.locnbr, 
          t1.businessdt, 
          t1.advcnt, 
          t1.delinquentcnt, 
          t1.heldcnt, 
          t1.defaultcnt, 
          t1.defaultoutstanding, 
          t1.nsfcnt, 
          t1.repmtplancnt, 
          t1.refinance_cnt, 
          t1.wocnt, 
          t1.worcnt, 
          t1.pastduecnt_1, 
          t1.first_presentment_cnt, 
          t1.satisfied_payment_cnt, 
          t1.avgdurationcnt, 
          t1.avgdurationdays, 
          t1.advamtsum, 
          t1.advfeeamt, 
          t1.delinquentamt, 
          t1.earnedfees, 
          t1.default_pmt, 
          t1.nsfamtsum, 
          t1.woamtsum, 
          t1.woramtsum, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.totdefaultrecv, 
          t1.totdefaultfeerecv, 
          t1.pastdueamt_1, 
          t2.CURRENT_PWO_AMT, 
          t2.NEXT_MONTH_PWO_AMT, 
          t2.NEXT_2_MONTH_PWO_AMT, 
          t2.BEGIN_PWO_AMT
      FROM WORK.COMBINED_METRICS_PRE t1
           LEFT JOIN WORK.PWO_NG t2 ON (t1.locnbr = t2.LOCNBR) AND (t1.businessdt = t2.BUSINESSDT) AND (t1.product = 
          t2.PRODUCT);
%RUNQUIT(&job,&sub13);


PROC SQL;
   CREATE TABLE WORK.HOLDOVER_OVERSHORTAMT AS 
   SELECT t1.FI_CENTER_ID AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.FD_TRANSACTION_DTTM)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* OVERSHORTAMT */
            (SUM(CASE WHEN FC_TRANSACTION_CD IN ('ADJOVER','ADJSHORT') THEN FN_FINANCIAL_DETAIL_AMT ELSE 0 END)) AS 
            OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (SUM(CASE WHEN FC_TRANSACTION_CD = 'HOLDOVER' THEN FN_FINANCIAL_DETAIL_AMT ELSE 0 END)) AS HOLDOVERAMT
      FROM ODSFIN.MV_FINANCIAL_RECORD t1
      WHERE t1.FC_TRANSACTION_CD IN 
           (
           'ADJOVER',
           'ADJSHORT',
           'HOLDOVER'
           ) AND t1.FC_FINANCIAL_DETAIL_CD = 'CASH'
      GROUP BY t1.FI_CENTER_ID,
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub13);

PROC SQL;
   CREATE TABLE WORK.NG_DAILYSUMMARY AS 
   SELECT t1.product, 
          t1.product_desc, 
          t1.pos, 
          t1.instance, 
          t2.BRND_CD AS BRANDCD, 
          /* bankmodel */
            ("STANDARD") AS bankmodel, 
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
          t1.locnbr, 
          t2.LOC_NM AS LOCATION_NAME, 
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          t1.businessdt, 
          t1.advcnt, 
          t1.delinquentcnt, 
          t1.heldcnt, 
          t1.defaultcnt, 
          t1.defaultoutstanding, 
          t1.nsfcnt, 
          t1.repmtplancnt, 
          t1.refinance_cnt, 
          t1.wocnt, 
          t1.worcnt, 
          t1.pastduecnt_1, 
          /* FIRST_PRESENTMENT_CNT */
            (CASE WHEN PRODUCT = 'PAYDAY' THEN t1.first_presentment_cnt ELSE 0 END) AS FIRST_PRESENTMENT_CNT, 
          /* SATISFIED_PAYMENT_CNT */
            (CASE WHEN t1.PRODUCT = 'PAYDAY' THEN t1.satisfied_payment_cnt ELSE 0 END) AS SATISFIED_PAYMENT_CNT, 
          /* AVGDURATIONCNT */
            (CASE WHEN t1.product = 'PAYDAY' THEN t1.avgdurationcnt ELSE 0 END ) AS AVGDURATIONCNT, 
          /* AVGDURATIONDAYS */
            (CASE WHEN t1.product = 'PAYDAY' THEN t1.avgdurationdays ELSE 0 END) AS AVGDURATIONDAYS, 
          t1.advamtsum, 
          t1.advfeeamt, 
          t1.delinquentamt, 
          t1.earnedfees, 
          t1.nsfamtsum, 
          t1.woamtsum, 
          t1.woramtsum, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.totdefaultrecv, 
          t1.totdefaultfeerecv, 
          t1.pastdueamt_1, 
          t1.default_pmt AS DEFAULT_PMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t5.OVERSHORTAMT, 
          t5.HOLDOVERAMT, 
          /* MONTH */
            (MONTH(T1.BUSINESSDT)) AS MONTH, 
          /* YEAR */
            (YEAR(T1.BUSINESSDT)) AS YEAR
      FROM WORK.COMBINED_METRICS t1
           LEFT JOIN EDW.D_LOCATION t2 ON (t1.locnbr = t2.LOC_NBR)
           LEFT JOIN WORK.HOLDOVER_OVERSHORTAMT t5 ON (t1.locnbr = t5.LOCNBR) AND (t1.businessdt = t5.BUSINESSDT);
%RUNQUIT(&job,&sub13);


PROC SQL;
	CREATE TABLE NG_DAILYSUMMARY AS
		SELECT T1.*,
			   CASE WHEN T2.BEGIN_PWO_AMT = . THEN 0 ELSE T2.BEGIN_PWO_AMT END AS BEGIN_PWO_AMT
		FROM WORK.NG_DAILYSUMMARY T1
		LEFT JOIN 
		WORK.BEGIN_PWO_AMT T2
		ON (T1.LOCNBR = T2.LOCNBR AND
			T1.MONTH = T2.MONTH AND
			T1.YEAR = T2.YEAR AND
			T1.PRODUCT = T2.PRODUCT)
;
%RUNQUIT(&job,&sub13);

PROC SQL;
   CREATE TABLE WORK.NG_NEW_ORIGINATIONS AS 
   SELECT t1.product, 
          t1.product_desc, 
          t1.pos, 
          t1.instance, 
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
          t1.LOCATION_NAME, 
          t1.locnbr, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.businessdt, 
          t1.advcnt AS NEW_ORIGINATIONS, 
          t1.advamtsum AS NEW_ADV_AMT, 
          t1.advfeeamt AS NEW_ADVFEE_AMT, 
          t1.totadvrecv AS TOTADVRECV, 
          t1.heldcnt AS COMPLIANT_LOANS_OUTSTANDING, 
          t1.repmtplancnt AS REPMTPLANCNT, 
          t1.defaultoutstanding AS DEFAULT_LOANS_OUTSTANDING, 
          t1.totadvfeerecv AS TOTADVFEERECV, 
          t1.totdefaultrecv AS TOTDEFAULTRECV, 
		  t1.totdefaultfeerecv AS TOTDEFAULTFEERECV,
          t1.pastduecnt_1 AS PASTDUECNT_1, 
          t1.pastdueamt_1 AS PASTDUEAMT_1, 
          t1.refinance_cnt AS REFINANCE_CNT, 
          t1.OVERSHORTAMT, 
          t1.HOLDOVERAMT, 
          t1.FIRST_PRESENTMENT_CNT, 
          t1.SATISFIED_PAYMENT_CNT, 
          t1.AVGDURATIONCNT, 
          t1.AVGDURATIONDAYS, 
          t1.worcnt AS WORCNT, 
          t1.woramtsum AS WORAMTSUM, 
          t1.wocnt AS WOCNT, 
          t1.woamtsum AS WOAMTSUM, 
          t1.earnedfees AS GROSS_REVENUE, 
          t1.heldcnt AS HELDCNT, 
          /* GROSS_WRITE_OFF */
            (sum(t1.woamtsum)) AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (SUM(t1.woamtsum,-t1.woramtsum)) AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (t1.earnedfees-(SUM(t1.woamtsum,-t1.woramtsum))) AS NET_REVENUE, 
          t1.delinquentamt AS DEL_RECV_AMT, 
          t1.delinquentcnt AS DEL_RECV_CNT, 
          t1.defaultcnt AS DEFAULT_CNT, 
          t1.nsfamtsum AS DEFAULT_AMT, 
          t1.DEFAULT_PMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.BEGIN_PWO_AMT
      FROM NG_DAILYSUMMARY t1
      GROUP BY t1.product,
               t1.product_desc,
               t1.pos,
               t1.instance,
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
               t1.LOCATION_NAME,
               t1.locnbr,
               t1.LOC_OPEN_DT,
               t1.LOC_CLOSE_DT,
               t1.businessdt,
               t1.advcnt,
               t1.advamtsum,
               t1.advfeeamt,
               t1.totadvrecv,
               t1.heldcnt,
               t1.repmtplancnt,
			   t1.defaultoutstanding,
               t1.defaultcnt,
               t1.totadvfeerecv,
               t1.totdefaultrecv,
			   t1.totdefaultfeerecv,
               t1.pastduecnt_1,
               t1.pastdueamt_1,
               t1.refinance_cnt,
               t1.OVERSHORTAMT,
               t1.HOLDOVERAMT,
               t1.FIRST_PRESENTMENT_CNT,
               t1.SATISFIED_PAYMENT_CNT,
               t1.AVGDURATIONCNT,
               t1.AVGDURATIONDAYS,
               t1.worcnt,
               t1.woramtsum,
               t1.wocnt,
               t1.woamtsum,
               t1.earnedfees,
               t1.heldcnt,
               (CALCULATED NET_WRITE_OFF),
               (CALCULATED NET_REVENUE),
               t1.delinquentamt,
               t1.delinquentcnt,
               t1.nsfamtsum,
               t1.DEFAULT_PMT,
               t1.CURRENT_PWO_AMT,
               t1.NEXT_MONTH_PWO_AMT,
               t1.NEXT_2_MONTH_PWO_AMT,
               t1.BEGIN_PWO_AMT;
%RUNQUIT(&job,&sub13);


/*PROC SQL;*/
/*	CREATE TABLE WOR_2018 AS*/
/*	SELECT SUM(CASE WHEN BUSINESSDT >= '01JAN2018'D THEN woramtsum END) AS WOR_2018*/
/*		  ,SUM(CASE WHEN BUSINESSDT >= '01JAN2017'D AND BUSINESSDT <= '23SEP2017'D THEN woramtsum END)	AS WOR_2017*/
/*		  ,SUM(CASE WHEN BUSINESSDT >= '01SEP2018'D THEN woramtsum END)	AS WOR_SEP2018*/
/*	FROM NG_NEW_ORIGINATIONS*/
/*;*/
/*QUIT;*/


PROC SQL;
   CREATE TABLE RU1_LENDINGPRODUCTS_ROLLUP AS 
   SELECT T1.PRODUCT, 
          T1.PRODUCT_DESC, 
          T1.POS, 
          T1.INSTANCE, 
          T1.BRANDCD, 
          T1.BANKMODEL, 
          T1.COUNTRYCD, 
          T1.STATE, 
          T1.CITY, 
          T1.ZIP, 
          T1.BUSINESS_UNIT, 
          T1.ZONENBR, 
          T1.ZONENAME, 
          T1.REGIONNBR, 
          T1.REGIONRDO, 
          T1.DIVISIONNBR, 
          T1.DIVISIONDDO, 
          T1.LOCNBR, 
          T1.LOCATION_NAME, 
          T1.LOC_OPEN_DT, 
          T1.LOC_CLOSE_DT, 
          T1.BUSINESSDT, 
          /* NEW_ORIGINATIONS */
            (SUM(T1.NEW_ORIGINATIONS)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (SUM(T1.NEW_ADV_AMT)) AS NEW_ADV_AMT, 
          /* NEW_ADVFEE_AMT */
            (SUM(T1.NEW_ADVFEE_AMT)) AS NEW_ADVFEE_AMT, 
          /* TOTADVRECV */
            (SUM(T1.TOTADVRECV)) FORMAT=12.2 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (SUM(T1.TOTADVFEERECV)) FORMAT=10.2 AS TOTADVFEERECV, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (SUM(T1.COMPLIANT_LOANS_OUTSTANDING)) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (SUM(T1.DEFAULT_LOANS_OUTSTANDING)) AS DEFAULT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (SUM(T1.TOTDEFAULTRECV)) FORMAT=12.2 AS TOTDEFAULTRECV, 
          /* TOTDEFAULTFEERECV */
            (SUM(T1.TOTDEFAULTFEERECV)) FORMAT=10.2 AS TOTDEFAULTFEERECV, 
          /* NSF_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_AMOUNT, 
          /* NSF_PAYMENT_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_PAYMENT_AMOUNT, 
          /* NSF_PREPAYMENT_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_PREPAYMENT_AMOUNT, 
          /* WOCNT */
            (SUM(T1.WOCNT)) AS WOCNT, 
          /* WOAMTSUM */
            (SUM(WOAMTSUM)) FORMAT=14.2 AS WOAMTSUM, 
          /* WOBAMTSUM */
            (SUM(0)) FORMAT=10.2 AS WOBAMTSUM, 
          /* WOBCNT */
            (SUM(0)) AS WOBCNT, 
          /* WORCNT */
            (SUM(T1.WORCNT)) AS WORCNT, 
          /* WORAMTSUM */
            (SUM(woramtsum)) FORMAT=10.2 AS WORAMTSUM, 
          /* CASHAGAIN_COUNT */
            (SUM(0)) AS CASHAGAIN_COUNT, 
          /* BUYBACK_COUNT */
            (SUM(0)) AS BUYBACK_COUNT, 
          /* DEPOSIT_COUNT */
            (SUM(0)) AS DEPOSIT_COUNT, 
          /* BEGIN_PWO_AMT */
            (SUM(T1.BEGIN_PWO_AMT)) AS BEGIN_PWO_AMT, 
          /* CURRENT_PWO_AMT */
            (SUM(T1.CURRENT_PWO_AMT)) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (SUM(T1.NEXT_MONTH_PWO_AMT)) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (SUM(T1.NEXT_2_MONTH_PWO_AMT)) AS NEXT_2_MONTH_PWO_AMT, 
          /* DEFAULT_PMT */
            (SUM(T1.DEFAULT_PMT)) FORMAT=10.2 AS DEFAULT_PMT, 
          /* DEFAULT_CNT */
            (SUM(T1.DEFAULT_CNT)) AS DEFAULT_CNT, 
          /* DEFAULT_AMT */
            (SUM(T1.DEFAULT_AMT)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* GROSS_REVENUE */
            (SUM(T1.GROSS_REVENUE)) FORMAT=10.2 AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (SUM(T1.GROSS_WRITE_OFF)) FORMAT=10.2 AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (SUM(T1.NET_WRITE_OFF)) FORMAT=10.2 AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (SUM(T1.NET_REVENUE)) FORMAT=10.2 AS NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            (SUM(0)) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (SUM(0)) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (SUM(0)) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (SUM(0)) AS ACTUAL_DURATION_FEES, 
          /* AVGDURATIONDAYS */
            (SUM(AVGDURATIONDAYS)) AS AVGDURATIONDAYS, 
          /* AVGDURATIONCNT */
            (SUM(AVGDURATIONCNT)) AS AVGDURATIONCNT, 
          /* HELDCNT */
            (SUM(T1.HELDCNT)) AS HELDCNT, 
          /* PASTDUECNT_1 */
            (SUM(T1.PASTDUECNT_1)) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (SUM(T1.PASTDUEAMT_1)) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* OVERSHORTAMT */
            (SUM(T1.OVERSHORTAMT)) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (SUM(T1.HOLDOVERAMT)) AS HOLDOVERAMT, 
          /* ADVAMTSUM */
            (SUM(0)) FORMAT=14.2 AS ADVAMTSUM, 
          /* AGNADVSUM */
            (SUM(0)) FORMAT=14.2 AS AGNADVSUM, 
          /* REPMTPLANCNT */
            (SUM(REPMTPLANCNT)) AS REPMTPLANCNT, 
          /* ADVCNT */
            (SUM(0)) AS ADVCNT, 
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
            (SUM(T1.REFINANCE_CNT)) AS REFINANCE_CNT, 
          /* AGNCNT */
            (SUM(0)) AS AGNCNT, 
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
          /* AGNAMTSUM */
            (SUM(0)) AS AGNAMTSUM, 
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
            (SUM(t1.FIRST_PRESENTMENT_CNT)) AS FIRST_PRESENTMENT_CNT, 
          /* SATISFIED_PAYMENT_CNT */
            (SUM(t1.SATISFIED_PAYMENT_CNT)) AS SATISFIED_PAYMENT_CNT, 
          /* DEL_RECV_AMT */
            (SUM(DEL_RECV_AMT)) AS DEL_RECV_AMT, 
          /* DEL_RECV_CNT */
            (SUM(DEFAULT_CNT)) AS DEL_RECV_CNT
      FROM WORK.NG_NEW_ORIGINATIONS t1
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
%RUNQUIT(&job,&sub13);

%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
LIBNAME NORECV "E:\SHARED\CADA\SAS DATA\DATAMART\SCOCHRAN";

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub13);

DATA WORK.A&END_DT;
	X = &ENDINGDT;
	FORMAT X MMDDYY10.;
%RUNQUIT(&job,&sub13);

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
%RUNQUIT(&job,&sub13);

proc sql;
	create table thursdaydates_tmp2 as
		select t1.*, t2.holidayname
		  from thursdaydates_tmp1 t1
			LEFT JOIN bior.i_holidays t2 on (t1.businessdt = datepart(t2.holiday_dt))
		 order by t1.businessdt desc;
%RUNQUIT(&job,&sub13);

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
%RUNQUIT(&job,&sub13);

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
%RUNQUIT(&job,&sub13);

proc sql;
	create table work.daily_summary_all_tmp3 as
		select t1.*, t2.thursdayweek
          from work.daily_summary_all_tmp2 t1, work.thursdaydates_tmp3 t2
		 where t1.businessdt = t2.businessdt;
%RUNQUIT(&job,&sub13);

proc sort data=daily_summary_all_tmp3;
	by locnbr businessdt;
%RUNQUIT(&job,&sub13);

data last_report_date;
	set daily_summary_all_tmp3;
	by locnbr businessdt;
	loc_last_reported_dt = businessdt;
	if last.locnbr then output;
	keep locnbr loc_last_reported_dt;
	format loc_last_reported_dt mmddyy10.;
%RUNQUIT(&job,&sub13);

PROC SQL;
   CREATE TABLE WORK.holidays(label="HOLIDAYS") AS 
   SELECT /* holidaydt */
            (datepart(t1.HOLIDAY_DT)) FORMAT=mmddyy10. LABEL="holidaydt" AS holidaydt, 
          t1.HOLIDAYNAME
      FROM BIOR.I_HOLIDAYS t1;
%RUNQUIT(&job,&sub13);

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
%RUNQUIT(&job,&sub13);

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
%RUNQUIT(&job,&sub13);

DATA DAILY_SUMMARY_ALL_PRE;
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
%RUNQUIT(&job,&sub13);


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
			SET NG_STATUS = 'WAITING_CL'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub13);

%MACRO WAITFORCUSTLIFE();


	%DO %UNTIL (%EVAL(&COUNT_R. >= 1));	
		PROC SQL;
			CREATE TABLE CUST_LIFE_CHECK_TODAY AS
			SELECT INSTANCE
				  ,MAX(BUSINESS_DATE)	AS BUSINESSDT
			FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE INSTANCE = 'NG'
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
						WHERE INSTANCE = 'NG' AND BUSINESSDT >= DHMS(TODAY()-1,00,00,00)
					;
					QUIT;

				%END;
		%ELSE %IF &DAYOFWEEK. = MONDAY %THEN
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'NG' AND BUSINESSDT >= DHMS(TODAY()-2,00,00,00)
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

%MEND;

%WAITFORCUSTLIFE

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET NG_STATUS = 'RUNNING'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub13);

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
	  WHERE T1.INSTANCE = 'NG' AND T1.BUSINESS_DATE >= DHMS(TODAY()-5,00,00,00)
      GROUP BY (CALCULATED BUSINESS_DATE),
               t1.LOCATION_NBR,
               t1.INSTANCE,
               t1.PRODUCT,
               (CALCULATED PRODUCT_DESC)
;
%RUNQUIT(&job,&sub13);

PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_NG_1 AS 
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
      FROM DAILY_SUMMARY_ALL_PRE t1
           LEFT JOIN WORK.PROD_DESC_CHANGE t2 ON (t1.INSTANCE = t2.INSTANCE) AND (t1.PRODUCT_DESC = t2.PRODUCT_DESC) 
          AND (T1.PRODUCT = t2.PRODUCT) AND (T1.BUSINESSDT = T2.BUSINESS_DATE) AND (t1.LOCNBR = t2.LOCATION_NBR);
%RUNQUIT(&job,&sub13);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub13);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\NG",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\NG\DIR2\",'G');
%RUNQUIT(&job,&sub13);

PROC SQL;
    INSERT INTO SKY.DAILYSUM_DATAMART_NG (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM DAILY_SUMMARY_ALL_NG_1
	WHERE BUSINESSDT >= DHMS(TODAY()-5,00,00,00);
%RUNQUIT(&job,&sub13);

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub13);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
RUN;

%PUT &TIMESTAMP;

/*KICK OFF NG_DAILY UPLOAD*/
SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
				 '&DAILY_FILE_PATH.\TRANSPOSE NG.SAS'
				 -LOG '&DAILY_LOGPATH.\TRANSPOSE_NG_&TIMESTAMP..LOG'
				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
TASKNAME=TRANSPOSE_NG
STATUS=TRANSPOSE_NG;

/*UPLOAD NG*/
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_NG.SAS";


PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.O_DAILY_SUMMARY_ALL
		    SET LAST_REPORT_DT = TO_DATE(TO_CHAR(CURRENT_DATE-1, 'MM/DD/YYYY'), 'MM/DD/YYYY')
			WHERE INSTANCE = 'NG'
			)
	BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

WAITFOR _ALL_ TRANSPOSE_NG;


/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%abort cancel;
	%END;

%MEND;

%STOPPROGRAM