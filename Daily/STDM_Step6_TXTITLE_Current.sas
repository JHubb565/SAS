%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";


%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub5);

DATA _NULL_;
	CALL SYMPUTX('DAILY_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DAILY_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DAILY",'G');
%RUNQUIT(&job,&sub5);

LIBNAME STATIC "//AACADASAS/CADA/SAS DATA/USER/SCOCHRAN/SKYNETCOMPARISON";

LIBNAME QFUND3 ORACLE
	PATH=EDWPRD  
	SCHEMA=QFUND3  
	USER=&USER  
	PW=&PASSWORD DEFER=YES;

PROC SQL;
   CREATE TABLE WORK.QFUND3_DAILYSUMMARY_tmp1(label="QFUND3_DAILYSUMMARY_tmp1") AS 
   SELECT /* Product */
            ("TITLE") LABEL="Product" AS Product, 
          /* pos */
            ("QFUND") LABEL="pos" AS pos, 
          /* INSTANCE */
            ('QFUND3') LABEL="INSTANCE" AS INSTANCE, 
          /* bankmodel */
            ("CSO") LABEL="bankmodel" AS bankmodel, 
          t3.BRND_CD AS BRANDCD, 
          t3.CTRY_CD AS COUNTRYCD, 
          t3.ST_PVC_CD AS STATE, 
          t3.ADR_CITY_NM AS CITY, 
          t3.MAIL_CD AS ZIP, 
          t3.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t3.HIER_ZONE_NBR AS ZONENBR, 
          t3.HIER_ZONE_NM AS ZONENAME, 
          t3.HIER_RGN_NBR AS REGIONNBR, 
          t3.HIER_RDO_NM AS REGIONRDO, 
          t3.HIER_DIV_NBR AS DIVISIONNBR, 
          t3.HIER_DDO_NM AS DIVISIONDDO, 
          t1.ST_CODE AS LOCNBR, 
          t3.LOC_NM AS Location_Name, 
          t3.OPEN_DT AS LOC_OPEN_DATE, 
          t3.CLS_DT AS LOC_CLOSE_DATE, 
          /* businessdt */
            (datepart(t1.TRAN_DATE)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* begindt */
            (intnx('month',today(),-24,'beginning')) FORMAT=mmddyy10. LABEL="begindt" AS begindt, 
          t1.ADV_CNT AS advcnt, 
          t1.ADV_AMT AS advamtsum, 
          t1.NEWCUST_CNT AS newcustdealcnt, 
          /* totadvrecv */
            (sum(t1.TOT_ADVREC_AMT, t1.PAST_DUE_AMT)) LABEL="totadvrecv" AS totadvrecv, 
          t1.TOT_ADVFEEREC_AMT AS totadvfeerecv, 
          t1.EARNED_FEES AS EARNEDFEES, 
          /* heldcnt */
            (sum(t1.HLD_CNT, t1.PAST_DUE_CNT)) LABEL="heldcnt" AS heldcnt, 
          t1.PAST_DUE_CNT AS PASTDUECNT_1, 
          t1.PAST_DUE_AMT AS PASTDUEAMT_1, 
          t1.BLACKBOOK_APPROVED_VALUE, 
          t1.SOLD_CNT, 
          t1.SOLD_AMT, 
          t1.IN_REPOSSESSION_CNT AS POSSESSION_CNT, 
          t1.IN_REPOSSESSION_AMT AS POSSESSION_AMT, 
          t1.WO_AMT AS WOAMTSUM, 
          t1.WO_CNT AS WOCNT, 
          t1.WOB_AMT AS WOBAMTSUM, 
          t1.WOB_CNT AS WOBCNT, 
          /* WORAMTSUM */
            (t1.WOR_AMT * -1) LABEL="WORAMTSUM" AS WORAMTSUM, 
          /* Enddt */
            (intnx('day',TODAY(),-1,'beginning')) FORMAT=mmddyy10. LABEL="Enddt" AS Enddt
      FROM TXTITLE.TITLE_DAILY_SUMMARY t1
           INNER JOIN EDW.D_LOCATION t3 ON (t1.ST_CODE = t3.LOC_NBR)
      WHERE (CALCULATED businessdt) BETWEEN (CALCULATED begindt) AND (CALCULATED Enddt) AND t3.ST_PVC_CD NOT IS MISSING 
           AND t1.PRODUCT_TYPE = 'TLP';
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.REFI_CNT AS 
   SELECT t1.ST_CODE, 
          /* BUSINESSDT */
            (DATEPART(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.PRODUCT_TYPE, 
          /* REFINANCE_CNT */
            (SUM(t1.AGN_CNT)) FORMAT=10.2 AS REFINANCE_CNT
      FROM EDW.TITLE_DAILY_SUMMARY t1
      WHERE t1.PRODUCT_TYPE = 'TLP'
      GROUP BY t1.ST_CODE,
               (CALCULATED BUSINESSDT),
               t1.PRODUCT_TYPE;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.qfund3_defaults_tmp1 AS 
   SELECT t1.LOCNBR, 
          t1.CUSTNBR, 
          t1.LOAN_CODE, 
          t1.TRANDTE AS DefaultDt, 
          t1.PRODUCT_ID, 
          t1.TRAN_ID, 
          /* businessdt */
            (datepart(t1.TRANDTE)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* DEFAULT_AMT */
            (SUM(t1.PRINCIPAL,t1.CSO_FEE,t1.LATE_FEE)) AS DEFAULT_AMT
      FROM TXTITLE.TITLE_TRANS_SUMMARY t1
      WHERE t1.TRAN_ID = 'DEF' AND t1.VOID_ID = 'N'
      ORDER BY t1.LOAN_CODE,
               t1.TRANDTE DESC;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT AS 
   SELECT t1.LOCNBR, 
          t1.businessdt AS BUSINESSDT, 
          /* DEFAULT_AMT */
            (SUM(t1.DEFAULT_AMT)) AS DEFAULT_AMT, 
          /* DEFAULT_CNT */
            (COUNT(t1.CUSTNBR)) AS DEFAULT_CNT
      FROM WORK.QFUND3_DEFAULTS_TMP1 t1
      GROUP BY t1.LOCNBR,
               t1.businessdt;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_writeoffloans(label="txtitle_writeoffloans") AS 
   SELECT t1.LOAN_CODE, 
          t1.TRANDTE AS writeoffdt
      FROM TXTITLE.TITLE_TRANS_SUMMARY t1
      WHERE t1.TRAN_ID IN 
           (
           'WO',
           'WOB',
           'WOD'
           );
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.qfund3_defaults_tmp2 AS 
   SELECT t2.LOCNBR, 
          t2.CUSTNBR, 
          t2.LOAN_CODE, 
          t2.DefaultDt, 
          t2.PRODUCT_ID, 
          t2.TRAN_ID, 
          t2.businessdt, 
          /* writeoffdt */
            (CASE 
               WHEN . = t1.writeoffdt THEN dhms('31DEC9999'd,0,0,0)
               ELSE t1.writeoffdt
            END) FORMAT=DATETIME20. LABEL="writeoffdt" AS writeoffdt
      FROM WORK.TXTITLE_WRITEOFFLOANS t1
           RIGHT JOIN WORK.QFUND3_DEFAULTS_TMP1 t2 ON (t1.LOAN_CODE = t2.LOAN_CODE);
%RUNQUIT(&job,&sub5);

/* -------------------------------------------------------------------
   Run the SORT procedure
   ------------------------------------------------------------------- */
PROC SORT DATA=WORK.QFUND3_DEFAULTS_TMP2
	OUT=WORK.QFUND3_DEFAULTS(LABEL="Sorted WORK.QFUND3_DEFAULTS_TMP2")
	NODUPKEY
	;
	BY LOAN_CODE;

%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.qfund3_defaults_transactions AS 
   SELECT t1.LOCNBR, 
          t2.CUSTNBR, 
          t2.LOAN_CODE, 
          t2.TRANDTE, 
          /* tran_dt */
            (datepart(t2.TRANDTE)) FORMAT=mmddyy10. LABEL="tran_dt" AS tran_dt, 
          t2.PRODUCT_ID, 
          t2.LOAN_TRAN_CODE, 
          t2.LOAN_DUE_DATE, 
          t2.LOAN_STATUS, 
          t2.TRAN_ID, 
          t2.VOID_ID, 
          t2.PRINCIPAL, 
          t2.CSO_FEE, 
          t2.LATE_FEE, 
          t2.WRITEOFF_AMT, 
          t2.WRITEOFF_FEE, 
          t1.businessdt
      FROM WORK.QFUND3_DEFAULTS t1, TXTITLE.TITLE_TRANS_SUMMARY t2
      WHERE (t1.LOAN_CODE = t2.LOAN_CODE AND t1.CUSTNBR = t2.CUSTNBR AND t1.DefaultDt <= t2.TRANDTE) AND (t2.TRAN_ID NOT 
           = 'LAT' AND t2.TRANDTE <= t1.writeoffdt)
      ORDER BY t2.LOAN_CODE,
               tran_dt,
               t2.LOAN_TRAN_CODE;
%RUNQUIT(&job,&sub5);


PROC SQL;
   CREATE TABLE WORK.txtitle_summarize_transactions(label="txtitle_summarize_transactions") AS 
   SELECT t1.LOCNBR, 
          t1.LOAN_CODE, 
          t1.tran_dt, 
          t1.TRAN_ID, 
          /* balance_adjustment */
            (SUM(sum(t1.PRINCIPAL,t1.CSO_FEE,t1.LATE_FEE))) LABEL="balance_adjustment" AS balance_adjustment
      FROM WORK.QFUND3_DEFAULTS_TRANSACTIONS t1
      GROUP BY t1.LOCNBR,
               t1.LOAN_CODE,
               t1.tran_dt,
               t1.TRAN_ID
      ORDER BY t1.LOCNBR,
               t1.LOAN_CODE,
               t1.tran_dt;
%RUNQUIT(&job,&sub5);

data work.txtitle_runningtotals_tmp1;
	set Work.TXTITLE_SUMMARIZE_TRANSACTIONS;
	by locnbr loan_code tran_dt;
	if first.loan_code then balance = 0;
	balance + balance_adjustment;
	if tran_id in ('WO', 'WOB', 'WOD') then balance_adjustment = balance * -1;
	drop balance;
%RUNQUIT(&job,&sub5);

proc sort data=work.txtitle_runningtotals_tmp1;
	by locnbr tran_dt;
%RUNQUIT(&job,&sub5);

data work.txtitle_runningtotals_tmp2;
	set work.txtitle_runningtotals_tmp1 (drop=tran_id loan_code);
	by locnbr tran_dt;
	if first.locnbr then balance = 0;
	balance + balance_adjustment;
	balance = round(balance,0.01);
	if last.tran_dt then output;
%RUNQUIT(&job,&sub5);

proc sort data=work.txtitle_runningtotals_tmp2;
	by locnbr tran_dt;
%RUNQUIT(&job,&sub5);


PROC SQL;
   CREATE TABLE WORK.txtitle_defaultenddate AS 
   SELECT t2.LOCNBR, 
          t1.LOAN_CODE, 
          t1.DefaultDt, 
          /* DEFAULTENDDT */
            (MIN(min(t2.DEAL_END_DATE,t1.WRITEOFFDT))) FORMAT=datetime20. LABEL="DEFAULTENDDT" AS DEFAULTENDDT
      FROM WORK.QFUND3_DEFAULTS t1
           INNER JOIN TXTITLE.TITLE_LOAN_SUMMARY t2 ON (t1.LOAN_CODE = t2.LOAN_CODE)
      GROUP BY t2.LOCNBR,
               t1.LOAN_CODE,
               t1.DefaultDt
      ORDER BY t1.LOAN_CODE;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_defaultmorethanoneday AS 
   SELECT t1.LOCNBR, 
          t1.LOAN_CODE, 
          t1.DefaultDt, 
          t1.DEFAULTENDDT
      FROM WORK.TXTITLE_DEFAULTENDDATE t1
      WHERE t1.DEFAULTENDDT > t1.DefaultDt;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_countdefaultsbyday(label="txtitle_countdefaultsbyday") AS 
   SELECT t1.LOCNBR, 
          t2.businessdt, 
          /* DEFAULTCNT */
            (case
              when dhms(t2.businessdt,0,0,0) >= t1.DefaultDt AND dhms(t2.businessdt,0,0,0) < t1.DEFAULTENDDT then 1
              else 0
            end) LABEL="DEFAULTCNT" AS DEFAULTCNT
      FROM WORK.TXTITLE_DEFAULTMORETHANONEDAY t1
           INNER JOIN WORK.QFUND3_DAILYSUMMARY_TMP1 t2 ON (t1.LOCNBR = t2.LOCNBR)
      ORDER BY t1.LOCNBR,
               t2.businessdt;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_defaultcountsbylocdate(label="txtitle_defaultcountsbylocdate") AS 
   SELECT t1.LOCNBR, 
          t1.businessdt, 
          /* defaultcnt */
            (SUM(t1.DEFAULTCNT)) LABEL="defaultcnt" AS defaultcnt
      FROM WORK.TXTITLE_COUNTDEFAULTSBYDAY t1
      GROUP BY t1.LOCNBR,
               t1.businessdt;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_dailysummary_tmp2(label="txtitle_dailysummary_tmp2") AS 
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
          t2.LOC_OPEN_DATE, 
          t2.LOC_CLOSE_DATE, 
          t2.businessdt, 
          t2.begindt, 
          t2.advcnt, 
          t2.advamtsum, 
          t2.newcustdealcnt, 
          t2.totadvrecv, 
          t2.totadvfeerecv, 
          t2.heldcnt, 
          /* PASTDUECNT_1 */
            (CASE 
               WHEN . = t2.PASTDUECNT_1 THEN 0
               ELSE t2.PASTDUECNT_1
            END) FORMAT=10.2 LABEL="PASTDUECNT_1" AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (CASE 
               WHEN . = t2.PASTDUEAMT_1 THEN 0
               ELSE t2.PASTDUEAMT_1
            END) FORMAT=10.2 LABEL="PASTDUEAMT_1" AS PASTDUEAMT_1, 
          t2.BLACKBOOK_APPROVED_VALUE, 
          t2.SOLD_CNT, 
          t2.SOLD_AMT, 
          t2.POSSESSION_CNT, 
          t2.POSSESSION_AMT, 
          t2.EARNEDFEES, 
          t1.balance AS totdefaultrecv, 
          /* defaultcnt */
            (CASE 
               WHEN . = t3.defaultcnt THEN 0
               ELSE t3.defaultcnt
            END) LABEL="defaultcnt" AS defaultcnt, 
          t2.WOAMTSUM, 
          t2.WOCNT, 
          t2.WOBAMTSUM, 
          t2.WOBCNT, 
          t2.WORAMTSUM
      FROM WORK.TXTITLE_RUNNINGTOTALS_TMP2 t1
           RIGHT JOIN WORK.QFUND3_DAILYSUMMARY_TMP1 t2 ON (t1.tran_dt = t2.businessdt) AND (t1.locnbr = t2.LOCNBR)
           LEFT JOIN WORK.TXTITLE_DEFAULTCOUNTSBYLOCDATE t3 ON (t2.LOCNBR = t3.LOCNBR) AND (t2.businessdt = 
          t3.businessdt)
      ORDER BY t2.LOCNBR,
               t2.businessdt;
%RUNQUIT(&job,&sub5);

data work.qfund3_DAILYSUMMARY_tmp3;
	set Work.TXTITLE_DAILYSUMMARY_TMP2;
	retain priordefrecv;
	by locnbr businessdt;

	if first.locnbr then do;
		if totdefaultrecv = . then totdefaultrecv = 0;
		priordefrecv = totdefaultrecv;
	end;
	else do;
		if totdefaultrecv = . then totdefaultrecv = priordefrecv;
			else priordefrecv = totdefaultrecv;
	end;

	if woamtsum = . then woamtsum = 0;
	if wobamtsum = . then wobamtsum = 0;

	drop priordefrecv;
%RUNQUIT(&job,&sub5);



/*#########################################################*/
/*------------------------  START  ------------------------*/
/*#########################################################*/

/* END DATE VARIABLE */
data _null_;
   call symputx('end_dt',put(TODAY()-1,date9.), 'g');
%RUNQUIT(&job,&sub5);
%put &end_dt;

/* START DATE VARIABLE */
data _null_;
   start_dt1 = intnx('month',today(),-24,'beginning');
   call symputx('start_dt',put(start_dt1,date9.), 'g');
%RUNQUIT(&job,&sub5);
%put &start_dt;


/* GET DEFAULT DATES FOR LOANS */
PROC SQL;
   CREATE TABLE WORK.qfund3_defaults_tmp1 AS 
   SELECT t1.LOCNBR, 
          t1.CUSTNBR, 
          t1.LOAN_CODE, 
          (datepart(t1.TRANDTE)) FORMAT=date9. AS DefaultDt, 
          t1.PRODUCT_ID, 
          t1.TRAN_ID, 
          /* businessdt */
            (datepart(t1.TRANDTE)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* DEFAULT_AMT */
            (SUM(t1.PRINCIPAL,t1.CSO_FEE,t1.LATE_FEE)) AS DEFAULT_AMT
      FROM TXTITLE.TITLE_TRANS_SUMMARY t1
      WHERE t1.TRAN_ID = 'DEF' AND t1.VOID_ID = 'N'
      ORDER BY t1.LOAN_CODE,
               t1.TRANDTE;
%RUNQUIT(&job,&sub5);

/* PICK FIRST DEFAULT DATE */
data qfund3_defaults_first;
	set qfund3_defaults_tmp1;
	by loan_code DefaultDt;
	if first.loan_code;
%RUNQUIT(&job,&sub5);




/* PICK DATA 2 YEARS AGO FROM TODAY
   ZERO WRITEOFF AMOUNT JUST IN CASE */
data test_time_series;
	set txtitle.title_loan_summary;
	where updatedte >= "&start_dt"d
	;
	update_dt = datepart(updatedte);
	format update_dt date9.;
	if loan_status = "WO"
		then total_owed = 0;
	;
%RUNQUIT(&job,&sub5);

/* REMOVE LOANS THAT HAVE BEEN VOIDED */
proc sql;
	create table test_time_series_99 as
		select *
		from test_time_series
		where loan_code not in (select distinct loan_code
								from test_time_series
								where LOAN_STATUS = "V"
								)
	;
%RUNQUIT(&job,&sub5);


/* JOIN ON DEFAULT DATE TO
   CORRESPONDING LOANS     */
proc sql;
	create table test_time_series_98 as
		select t1.*
			  ,t2.DefaultDt
		from test_time_series_99 t1
			left join qfund3_defaults_first t2
				on t1.loan_code = t2.loan_code
		order by t1.loan_code
				,t1.update_dt
	;
%RUNQUIT(&job,&sub5);

/* SET ALL TOTAL OWED AMOUNTS TO ZERO
   IF UPDATEDTE DATE IS LESS
   THAN DEFAULT DATE                  */
data test_time_series_97;
	set test_time_series_98;
	if update_dt < defaultdt
		then total_owed = 0
	;
	if defaultdt ^= .;
%RUNQUIT(&job,&sub5);

/* SUBTRACT 1 DAY FROM UPDATE DATE
   SINCE IT IS LAGGED BY 1 DAY     */
data test_time_series_96;
	set test_time_series_97;
	update_dt = update_dt - 1;
%RUNQUIT(&job,&sub5);

proc sql;
	create table test_time_series_95 as
		select locnbr
			  ,loan_code
			  ,sum(total_owed) as total
		from test_time_series_96
		group by locnbr
				,loan_code
	;
%RUNQUIT(&job,&sub5);

proc sql;
	create table test_time_series_94 as
		select *
		from test_time_series_96
		where loan_code not in (select distinct loan_code
								from test_time_series_95
								where total = 0
								)
	;
%RUNQUIT(&job,&sub5);

/* SORT DATA */
proc sort data=test_time_series_94;
	by locnbr loan_code update_dt;
%RUNQUIT(&job,&sub5);



/* CREATE DAILY RECORD FOR PAST 2 YEARS
   FOR EACH LOAN WITH THE TOTAL_OWED AT
   THAT TIME, IF NO CHANGE, KEEP PREVIOUS
   TOTAL_OWED AMOUNT RETAINED UNTIL CHANGE */
proc timeseries data=test_time_series_94 out=mseries;
   by locnbr loan_code;
   id update_dt interval=day
                accumulate=TOTAL
                setmiss=prev
/*                start='01feb2016'd*/
/*                end  ='12feb2018'd;*/
                start="&start_dt"d
                end="&end_dt"d;
   var total_owed;
%RUNQUIT(&job,&sub5);


/* IF ANY MISSING TOTAL_OWED AMOUNTS REMAIN
   SET TO ZERO                              */
data mseries_2;
	set mseries;
	if total_owed = .
		then total_owed = 0
	;
%RUNQUIT(&job,&sub5);

/* SUMMARIZE DATA BY LOCNBR AND BUSINESSDT.
   SUM OF TOTAL_OWED BY LOCNBR AND DAY      */
proc sql;
	create table total_default_amt as
		select locnbr
			  ,update_dt as businessdt
			  ,sum(total_owed) as totdefaultrecv
		from mseries_2
		group by locnbr
				,update_dt
	;
%RUNQUIT(&job,&sub5);
			

/*#########################################################*/
/*-------------------------  END  -------------------------*/
/*#########################################################*/

PROC SQL;
   CREATE TABLE WORK.TXTITLE_ORIGS AS 
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
      WHERE t1.PRODUCT_TYPE = 'TLP'
      ORDER BY t1.CUSTNBR,
               t1.LOAN_CODE,
               t1.TRANDTE,
               t1.LOAN_TRAN_CODE;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.csofees AS 
   SELECT /* businessdt */
            (datepart(t1.trandte)) FORMAT=mmddyy10. AS businessdt, 
          t1.LOCNBR, 
          /* CSO_FEE */
            (SUM(t1.CSO_FEE)) FORMAT=10.2 AS CSO_FEE
      FROM WORK.TXTITLE_ORIGS t1
      WHERE t1.TRAN_ID IN 
           (
           'ADV',
           'AGN'
           )
      GROUP BY (CALCULATED businessdt),
               t1.LOCNBR;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS_PRE AS 
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
      WHERE t1.TRAN_ID = 'DFP';
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMT AS 
   SELECT t1.LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANDTE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_PMT */
            (SUM(-SUM(t1.PRINCIPAL,t1.CSO_FEE,t1.LATE_FEE))) AS DEFAULT_PMT
      FROM WORK.DEFAULT_PMTS_PRE t1
      GROUP BY t1.LOCNBR,
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_originations2years AS 
   SELECT t1.LOAN_CODE, 
          t1.LOCNBR, 
          t1.LOAN_DATE, 
          t1.LOAN_STATUS, 
          t1.ADV_AMT, 
          t1.FEE_AMT, 
          t1.LATE_FEE_AMT, 
          t1.DUE_DATE, 
          t1.DEAL_END_DATE, 
          t1.WRITEOFF_DATE, 
          t1.UPDATEDTE
      FROM TXTITLE.TITLE_LOAN_SUMMARY t1
      WHERE (datepart(t1.LOAN_DATE)) BETWEEN (intnx('month',today(),-36,'beginning')) AND (intnx('day',TODAY(),-1,
           'beginning'))
      ORDER BY t1.LOAN_CODE,
               t1.UPDATEDTE DESC;
%RUNQUIT(&job,&sub5);

/* -------------------------------------------------------------------
   Run the SORT procedure
   ------------------------------------------------------------------- */
PROC SORT DATA=WORK.TXTITLE_ORIGINATIONS2YEARS
	OUT=WORK.txtitle_uniqueorigs(LABEL="Sorted WORK.TXTITLE_ORIGINATIONS2YEARS")
	NODUPKEY
	;
	BY LOAN_CODE;

%RUNQUIT(&job,&sub5);


PROC SQL;
   CREATE TABLE WORK.txtitle_uniqueorigs_tmp2 AS 
   SELECT t1.LOAN_CODE, 
          t1.LOCNBR, 
          t1.LOAN_DATE, 
          t1.LOAN_STATUS, 
          t1.ADV_AMT, 
          t1.FEE_AMT, 
          t1.LATE_FEE_AMT, 
          t1.DUE_DATE, 
          t2.DefaultDt, 
          t1.WRITEOFF_DATE, 
          t1.DEAL_END_DATE, 
          t1.UPDATEDTE, 
          /* Duration_Event_Date */
            (min(t2.DefaultDt,t1.DEAL_END_DATE,t1.WRITEOFF_DATE)) FORMAT=datetime20. AS Duration_Event_Date
      FROM WORK.TXTITLE_UNIQUEORIGS t1
           LEFT JOIN WORK.QFUND3_DEFAULTS t2 ON (t1.LOAN_CODE = t2.LOAN_CODE)
      WHERE t1.LOAN_STATUS NOT = 'V';
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_durations AS 
   SELECT t1.LOAN_CODE, 
          t1.LOCNBR, 
          t1.LOAN_DATE, 
          t1.LOAN_STATUS, 
          t1.ADV_AMT, 
          t1.FEE_AMT, 
          t1.LATE_FEE_AMT, 
          t1.DUE_DATE, 
          t1.DefaultDt, 
          t1.WRITEOFF_DATE, 
          t1.DEAL_END_DATE, 
          t1.UPDATEDTE, 
          t1.Duration_Event_Date, 
          /* Default_Duration_Count */
            (case
              when t1.duration_event_date ~= . AND t1.duration_event_date = t1.defaultdt AND t1.defaultdt ~= 
            t1.writeoff_date then 1
              else 0
            end) AS Default_Duration_Count, 
          /* Default_Duration_Days */
            (case
              when t1.duration_event_date ~= . AND t1.duration_event_date = t1.defaultdt AND t1.defaultdt ~= 
            t1.writeoff_date then datepart(t1.duration_event_date) - datepart(t1.loan_date)
              else 0
            end) AS Default_Duration_Days, 
          /* WO_Duration_Count */
            (case
              when t1.duration_event_date ~= . AND t1.duration_event_date = t1.writeoff_date then 1
              else 0
            end) AS WO_Duration_Count, 
          /* WO_Duration_Days */
            (case
              when t1.duration_event_date ~= . AND t1.duration_event_date = t1.writeoff_date then 
            datepart(t1.duration_event_date) - datepart(t1.loan_date)
              else 0
            end) AS WO_Duration_Days, 
          /* Repaid_Duration_Count */
            (case
              when t1.duration_event_date ~= . AND t1.duration_event_date = t1.deal_end_date AND t1.defaultdt = . AND 
            t1.writeoff_date = . then 1
              else 0
            end) AS Repaid_Duration_Count, 
          /* Repaid_Duration_Days */
            (case
              when t1.duration_event_date ~= . AND t1.duration_event_date = t1.deal_end_date AND t1.defaultdt = . AND 
            t1.writeoff_date = . then datepart(t1.duration_event_date) - datepart(t1.loan_date)
              else 0
            end) AS Repaid_Duration_Days, 
          /* sched_duration_days */
            (datepart(t1.DUE_DATE) - datepart(t1.LOAN_DATE)) AS sched_duration_days
      FROM WORK.TXTITLE_UNIQUEORIGS_TMP2 t1;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.txtitle_sched_duration AS 
   SELECT t1.LOCNBR, 
          /* businessdt */
            (datepart(t1.loan_date)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* sched_duration_days */
            (SUM(t1.sched_duration_days)) AS sched_duration_days, 
          /* sched_adv_amt */
            (SUM(t1.ADV_AMT)) AS sched_adv_amt, 
          /* sched_fee_amt */
            (SUM(t1.FEE_AMT)) AS sched_fee_amt
      FROM WORK.TXTITLE_DURATIONS t1
      GROUP BY t1.LOCNBR,
               (CALCULATED businessdt);
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.TXTITLE_ACTUAL_DURATION AS 
   SELECT t1.LOCNBR, 
          /* businessdt */
            (datepart(t1.Duration_Event_Date)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* actual_adv_amt */
            (SUM(t1.ADV_AMT)) AS actual_adv_amt, 
          /* actual_fee_amt */
            (SUM(sum(t1.FEE_AMT,t1.LATE_FEE_AMT))) AS actual_fee_amt, 
          /* Default_Duration_Count */
            (SUM(t1.Default_Duration_Count)) AS Default_Duration_Count, 
          /* Default_Duration_Days */
            (SUM(t1.Default_Duration_Days)) AS Default_Duration_Days, 
          /* WO_Duration_Count */
            (SUM(t1.WO_Duration_Count)) AS WO_Duration_Count, 
          /* WO_Duration_Days */
            (SUM(t1.WO_Duration_Days)) AS WO_Duration_Days, 
          /* Repaid_Duration_Count */
            (SUM(t1.Repaid_Duration_Count)) AS Repaid_Duration_Count, 
          /* Repaid_Duration_Days */
            (SUM(t1.Repaid_Duration_Days)) AS Repaid_Duration_Days
      FROM WORK.TXTITLE_DURATIONS t1
      WHERE (CALCULATED businessdt) NOT IS MISSING
      GROUP BY t1.LOCNBR,
               (CALCULATED businessdt);
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.advfeeamt_tmp1 AS 
   SELECT /* businessdt */
            (datepart(t1.LOAN_DATE)) FORMAT=mmddyy10. AS businessdt, 
          t1.LOCNBR, 
          /* ADVFEEAMT */
            (SUM(t1.FEE_AMT)) FORMAT=16.2 AS ADVFEEAMT
      FROM WORK.TXTITLE_UNIQUEORIGS t1
      WHERE t1.LOAN_STATUS NOT = 'V'
      GROUP BY (CALCULATED businessdt),
               t1.LOCNBR;
%RUNQUIT(&job,&sub5);

PROC SQL;
CREATE TABLE WORK.combined_fees AS 
SELECT * FROM WORK.ADVFEEAMT_TMP1
 OUTER UNION CORR 
SELECT * FROM WORK.CSOFEES
;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.advfeeamt AS 
   SELECT t1.businessdt, 
          t1.LOCNBR, 
          /* ADVFEEAMT */
            (SUM(sum(t1.ADVFEEAMT,t1.CSO_FEE))) AS ADVFEEAMT
      FROM WORK.COMBINED_FEES t1
      GROUP BY t1.businessdt,
               t1.LOCNBR
      ORDER BY t1.LOCNBR,
               t1.businessdt;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.QF3_TXTITLE_PNL AS 
   SELECT t1.STORE_NUMBER AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.BAD_DEBT AS GROSS_WRITE_OFF, 
          t1.BADDEBT_PMT AS WOR, 
          t1.PNL_AMT AS GROSS_REVENUE
      FROM EDW.QF_BADDEBT_PNLAMT t1
      WHERE t1.SOURCE_SYSTEM = 'QFUND3' AND t1.PRODUCT_TYPE = 'TLP';
%RUNQUIT(&job,&sub5);

PROC SQL;
	CREATE TABLE PWO_QF3_CURR AS
		SELECT 
			 STORE_NUMBER AS LOCNBR
			,DHMS(DATEPART(ETL_DT),00,00,00) AS BUSINESSDT FORMAT DATETIME20.
			,SUM(CASE WHEN DHMS(DATEPART(ETL_DT),00,00,00) = DHMS(INTNX('MONTH',DATEPART(ETL_DT),0,'B'),00,00,00)
					  AND PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(ETL_DT),0,'B'),00,00,00) 
					  AND DHMS(INTNX('MONTH',DATEPART(ETL_DT),0,'E'),00,00,00)
					   	  THEN PWO_AMT
					  ELSE 0
				 END) AS BEGIN_PWO_AMT_PRE
			,'QFUND3' AS INSTANCE
			,CASE WHEN PRODUCT_TYPE = 'ETL' 
					  THEN 'TX TETL'
				  WHEN PRODUCT_TYPE = 'TLP'
				  	  THEN 'TX TITLE'
				  WHEN PRODUCT_TYPE = 'TTOC'
				  	  THEN 'TX TTOC'
			 END AS PRODUCT_DESC
		FROM QFUND3.PWO
	WHERE CALCULATED BUSINESSDT >= DHMS(INTNX('MONTH',TODAY(),-36,'B'),00,00,00)
		  AND PRODUCT_TYPE = 'TLP'
	GROUP BY 
		 STORE_NUMBER
		,CALCULATED BUSINESSDT
		,PRODUCT_TYPE
	ORDER BY 
		 STORE_NUMBER
	    ,CALCULATED PRODUCT_DESC
		,CALCULATED BUSINESSDT
;
%RUNQUIT(&job,&sub5);

PROC SQL;
	CREATE TABLE PWO_QF3_NON_CURR AS
		SELECT 
			 STORE_NUMBER AS LOCNBR
			,DHMS(DATEPART(ETL_DT)-1,00,00,00) AS BUSINESSDT FORMAT DATETIME20.
			,SUM(CASE WHEN PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(ETL_DT)-1,0,'B'),00,00,00) 
					  AND DHMS(INTNX('MONTH',DATEPART(ETL_DT)-1,0,'E'),00,00,00)
					  	  THEN PWO_AMT
					  ELSE 0
				 END) AS CURRENT_PWO_AMT
			,SUM(CASE WHEN PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(ETL_DT)-1,1,'B'),00,00,00) 
					  AND DHMS(INTNX('MONTH',DATEPART(ETL_DT)-1,1,'E'),00,00,00) 
					  	  THEN PWO_AMT
					  ELSE 0
				 END) AS NEXT_MONTH_PWO_AMT
			,SUM(CASE WHEN PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(ETL_DT),2,'B'),00,00,00) 
					  AND DHMS(INTNX('MONTH',DATEPART(ETL_DT),2,'E'),00,00,00) 
					  	  THEN PWO_AMT
					  ELSE 0 
				 END) AS NEXT_2_MONTH_PWO_AMT
			,'QFUND3' AS INSTANCE
			,CASE WHEN PRODUCT_TYPE = 'ETL' 
					  THEN 'TX TETL'
				  WHEN PRODUCT_TYPE = 'TLP'
				  	  THEN 'TX TITLE'
				  WHEN PRODUCT_TYPE = 'TTOC'
				  	  THEN 'TX TTOC'
			 END AS PRODUCT_DESC
		FROM QFUND3.PWO
	WHERE CALCULATED BUSINESSDT >= DHMS(INTNX('MONTH',TODAY(),-36,'B'),00,00,00)
		  AND PRODUCT_TYPE = 'TLP'
	GROUP BY 
		 STORE_NUMBER
		,CALCULATED BUSINESSDT
		,PRODUCT_TYPE
	ORDER BY 
		 STORE_NUMBER
	    ,CALCULATED PRODUCT_DESC
		,CALCULATED BUSINESSDT
;
%RUNQUIT(&job,&sub5);

PROC SQL;
CREATE TABLE WORK.PWO_QF3_PRE1 AS 
SELECT * FROM WORK.PWO_QF3_CURR
 OUTER UNION CORR 
SELECT * FROM WORK.PWO_QF3_NON_CURR
;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.PWO_QF3_PRE AS 
   SELECT t1.LOCNBR, 
          t1.BUSINESSDT, 
          t1.INSTANCE, 
          t1.PRODUCT_DESC, 
          /* BEGIN_PWO_AMT_PRE */
            (SUM(t1.BEGIN_PWO_AMT_PRE)) AS BEGIN_PWO_AMT_PRE, 
          /* CURRENT_PWO_AMT */
            (SUM(t1.CURRENT_PWO_AMT)) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (SUM(t1.NEXT_MONTH_PWO_AMT)) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (SUM(t1.NEXT_2_MONTH_PWO_AMT)) AS NEXT_2_MONTH_PWO_AMT
      FROM WORK.PWO_QF3_PRE1 t1
      GROUP BY t1.LOCNBR,
               t1.BUSINESSDT,
               t1.INSTANCE,
               t1.PRODUCT_DESC;
%RUNQUIT(&job,&sub5);

DATA PWO_QF3;
	SET PWO_QF3_PRE;
	BY LOCNBR;
/*	IF FIRST.PRODUCT_DESC THEN IND = 'Y';*/
/*	ELSE IND = 'N';*/
	IF DAY(DATEPART(BUSINESSDT)) = 1 OR FIRST.LOCNBR THEN 
		DO;
/*			%LET BEGIN_AMT = BEGIN_PWO_AMT_PRE;*/
			BEGIN_PWO_AMT = CURRENT_PWO_AMT;
			RETAIN BEGIN_PWO_AMT;
		END;
	BUSINESSDT = DATEPART(BUSINESSDT);
	FORMAT BUSINESSDT MMDDYY10.;
DROP BEGIN_PWO_AMT_PRE;
%RUNQUIT(&job,&sub5);

DATA BEGIN_PWO_AMT;
	SET WORK.PWO_QF3;
	MONTH = MONTH(BUSINESSDT);
	YEAR = YEAR(BUSINESSDT);
	WHERE DAY(BUSINESSDT) = 1;
	KEEP LOCNBR BUSINESSDT BEGIN_PWO_AMT MONTH YEAR;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.QFUND3_DAILYSUMMARY AS 
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
          t1.Location_Name, 
          t1.LOCNBR, 
          t1.LOC_OPEN_DATE, 
          t1.LOC_CLOSE_DATE, 
          t1.businessdt, 
          t1.begindt, 
          t1.advcnt, 
          t1.advamtsum, 
          t4.ADVFEEAMT, 
          t1.newcustdealcnt, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.heldcnt, 
          /* PASTDUECNT_1 */
            (CASE 
               WHEN . = t1.PASTDUECNT_1 THEN 0
               ELSE t1.PASTDUECNT_1
            END) FORMAT=10.2 LABEL="PASTDUECNT_1" AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_1 THEN 0
               ELSE t1.PASTDUEAMT_1
            END) FORMAT=10.2 LABEL="PASTDUEAMT_1" AS PASTDUEAMT_1, 
          t8.REFINANCE_CNT, 
          /* BLACK_BOOK_VALUE */
            (CASE 
               WHEN . = t1.BLACKBOOK_APPROVED_VALUE THEN 0
               ELSE t1.BLACKBOOK_APPROVED_VALUE
            END) FORMAT=10.2 AS BLACK_BOOK_VALUE, 
          /* SOLD_COUNT */
            (CASE 
               WHEN . = t1.SOLD_CNT THEN 0
               ELSE t1.SOLD_CNT
            END) FORMAT=10.2 AS SOLD_COUNT, 
          /* SOLD_AMOUNT */
            (CASE 
               WHEN . = t1.SOLD_AMT THEN 0
               ELSE t1.SOLD_AMT
            END) FORMAT=10.2 AS SOLD_AMOUNT, 
          t1.POSSESSION_CNT, 
          t1.POSSESSION_AMT, 
          t1.EARNEDFEES, 

/*          t1.totdefaultrecv, */
          t10.totdefaultrecv, 

          t1.defaultcnt, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          t1.WOBCNT, 
          t1.WORAMTSUM, 
          /* sched_duration_days */
            (CASE 
               WHEN . = t3.sched_duration_days THEN 0
               ELSE t3.sched_duration_days
            END) AS sched_duration_days, 
          /* sched_adv_amt */
            (CASE 
               WHEN . = t3.sched_adv_amt THEN 0
               ELSE t3.sched_adv_amt
            END) AS sched_adv_amt, 
          /* sched_fee_amt */
            (CASE 
               WHEN . = t3.sched_fee_amt THEN 0
               ELSE t3.sched_fee_amt
            END) AS sched_fee_amt, 
          /* actual_adv_amt */
            (CASE 
               WHEN . = t2.actual_adv_amt THEN 0
               ELSE t2.actual_adv_amt
            END) AS actual_adv_amt, 
          /* actual_fee_amt */
            (CASE 
               WHEN . = t2.actual_fee_amt THEN 0
               ELSE t2.actual_fee_amt
            END) AS actual_fee_amt, 
          /* Default_Duration_Count */
            (CASE 
               WHEN . = t2.Default_Duration_Count THEN 0
               ELSE t2.Default_Duration_Count
            END) AS Default_Duration_Count, 
          /* Default_Duration_Days */
            (CASE 
               WHEN . = t2.Default_Duration_Days THEN 0
               ELSE t2.Default_Duration_Days
            END) AS Default_Duration_Days, 
          /* WO_Duration_Count */
            (CASE 
               WHEN . = t2.WO_Duration_Count THEN 0
               ELSE t2.WO_Duration_Count
            END) AS WO_Duration_Count, 
          /* WO_Duration_Days */
            (CASE 
               WHEN . = t2.WO_Duration_Days THEN 0
               ELSE t2.WO_Duration_Days
            END) AS WO_Duration_Days, 
          /* Repaid_Duration_Count */
            (CASE 
               WHEN . = t2.Repaid_Duration_Count THEN 0
               ELSE t2.Repaid_Duration_Count
            END) AS Repaid_Duration_Count, 
          /* Repaid_Duration_Days */
            (CASE 
               WHEN . = t2.Repaid_Duration_Days THEN 0
               ELSE t2.Repaid_Duration_Days
            END) AS Repaid_Duration_Days, 
          /* substituterow */
            ('N') AS substituterow, 
          t7.CURRENT_PWO_AMT, 
          t7.NEXT_MONTH_PWO_AMT, 
          t7.NEXT_2_MONTH_PWO_AMT, 
          t5.DEFAULT_AMT, 
          t5.DEFAULT_CNT, 
          t6.DEFAULT_PMT, 
          t9.GROSS_WRITE_OFF, 
          t9.WOR, 
          t9.GROSS_REVENUE, 
          /* MONTH */
            (MONTH(t1.businessdt)) AS MONTH, 
          /* YEAR */
            (YEAR(t1.businessdt)) AS YEAR
      FROM WORK.QFUND3_DAILYSUMMARY_TMP3 t1
           LEFT JOIN WORK.TXTITLE_ACTUAL_DURATION t2 ON (t1.LOCNBR = t2.LOCNBR) AND (t1.businessdt = t2.businessdt)
           LEFT JOIN WORK.TXTITLE_SCHED_DURATION t3 ON (t1.LOCNBR = t3.LOCNBR) AND (t1.businessdt = t3.businessdt)
           LEFT JOIN WORK.ADVFEEAMT t4 ON (t1.businessdt = t4.businessdt) AND (t1.LOCNBR = t4.LOCNBR)
           LEFT JOIN WORK.DEFAULT_AMT_CNT t5 ON (t1.LOCNBR = t5.LOCNBR) AND (t1.businessdt = t5.BUSINESSDT)
           LEFT JOIN WORK.DEFAULT_PMT t6 ON (t1.LOCNBR = t6.LOCNBR) AND (t1.businessdt = t6.BUSINESSDT)
           LEFT JOIN WORK.PWO_QF3 t7 ON (t1.LOCNBR = t7.LOCNBR) AND (t1.businessdt = t7.BUSINESSDT)
           LEFT JOIN WORK.REFI_CNT t8 ON (t1.businessdt = t8.BUSINESSDT) AND (t1.LOCNBR = t8.ST_CODE)
           LEFT JOIN WORK.QF3_TXTITLE_PNL t9 ON (t1.LOCNBR = t9.LOCNBR) AND (t1.businessdt = t9.BUSINESSDT)
	   LEFT JOIN WORK.TOTAL_DEFAULT_AMT t10 on (t1.LOCNBR = t10.LOCNBR) AND (t1.businessdt = t10.BUSINESSDT)
      ORDER BY t1.LOCNBR,
               t1.businessdt;
%RUNQUIT(&job,&sub5);

data QFUND3_DAILYSUMMARY;
	set QFUND3_DAILYSUMMARY;
	if totdefaultrecv = .
		then totdefaultrecv = 0;
%RUNQUIT(&job,&sub5);

proc sort data=QFUND3_DAILYSUMMARY;
	by locnbr businessdt;
%RUNQUIT(&job,&sub5);

PROC SQL;
	CREATE TABLE QFUND3_DAILYSUMMARY AS
		SELECT T1.*
			  ,CASE WHEN T2.BEGIN_PWO_AMT = . THEN 0 ELSE T2.BEGIN_PWO_AMT END AS BEGIN_PWO_AMT
		FROM WORK.QFUND3_DAILYSUMMARY T1
		LEFT JOIN 
		WORK.BEGIN_PWO_AMT T2
		ON (T1.LOCNBR=T2.LOCNBR AND 
		    T1.MONTH=T2.MONTH AND
			T1.YEAR=T2.YEAR)
		WHERE T1.BUSINESSDT >= TODAY() - 10
;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE QFUND3_NEW_ORIGINATIONS AS 
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
          t1.LOC_OPEN_DATE AS LOC_OPEN_DT, 
          t1.LOC_CLOSE_DATE AS LOC_CLOSE_DT, 
          t1.businessdt, 
          /* NEW_ORIGINATIONS */
           t1.advcnt AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
           t1.advamtsum AS NEW_ADV_AMT, 
          t1.ADVFEEAMT AS NEW_ADVFEE_AMT, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (t1.heldcnt) AS COMPLIANT_LOANS_OUTSTANDING, 
          t1.defaultcnt AS DEFAULT_LOANS_OUTSTANDING, 
          t1.totdefaultrecv, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          t1.WOBCNT, 
          /* WORAMTSUM */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN t1.WORAMTSUM ELSE 0 END) AS WORAMTSUM, 
          /* GROSS_REVENUE */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN (t1.EARNEDFEES) ELSE 0 END) AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN (sum(t1.WOAMTSUM,t1.WOBAMTSUM)) ELSE 0 END) AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN ((sum(t1.WOAMTSUM,t1.WOBAMTSUM)) - t1.WORAMTSUM) ELSE 0 END ) 
            AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN ((t1.EARNEDFEES)-((sum(t1.WOAMTSUM,t1.WOBAMTSUM)) - 
            t1.WORAMTSUM)) ELSE 0 END) AS NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            (sum(t1.Default_Duration_Count,t1.WO_Duration_Count,t1.Repaid_Duration_Count)) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (sum(t1.Default_Duration_Days,t1.WO_Duration_Days,t1.Repaid_Duration_Days)) AS ACTUAL_DURATION_DAYS, 
          t1.BEGIN_PWO_AMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.DEFAULT_AMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_PMT, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.REFINANCE_CNT, 
          t1.BLACK_BOOK_VALUE, 
          t1.SOLD_COUNT, 
          t1.SOLD_AMOUNT, 
          t1.POSSESSION_CNT, 
          t1.POSSESSION_AMT, 
          t1.actual_adv_amt AS actual_duration_advamt, 
          t1.actual_fee_amt AS actual_duration_fees, 
          t1.heldcnt, 
          /* PRODUCT_DESC */
            ("TX TITLE") AS PRODUCT_DESC
      FROM QFUND3_DAILYSUMMARY t1;
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);

/*-------------*/
/* QFUND 3 TLP */
/*-------------*/
PROC SQL;
	CREATE TABLE QFUND3_TLP AS
		SELECT
		    CASE WHEN COMPRESS(PRODUCT_TYPE) = 'TLP' THEN 'TITLE' 
			     ELSE PRODUCT_TYPE 
            END AS PRODUCT
		   ,'TX TITLE' AS PRODUCT_DESC
		   ,'QFUND' AS POS
		   ,INSTANCE
		   ,'CSO' AS BANKMODEL
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
	WHERE COMPRESS(PRODUCT_TYPE) = 'TLP' AND INSTANCE = 'QFUND3'
;
%RUNQUIT(&job,&sub5);

PROC SQL;
CREATE TABLE WORK.RU1_LENDINGPRODUCTS_ROLLUP_PRE AS 
	SELECT * FROM QFUND3_NEW_ORIGINATIONS
		OUTER UNION CORR 
	SELECT * FROM QFUND3_TLP
;
%RUNQUIT(&job,&sub5);

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
            (SUM(0)) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (SUM(0)) AS HOLDOVERAMT, 
          /* ADVAMTSUM */
            (SUM(0)) FORMAT=14.2 AS ADVAMTSUM, 
          /* AGNADVSUM */
            (SUM(0)) FORMAT=14.2 AS AGNADVSUM, 
          /* REPMTPLANCNT */
            (SUM(0)) AS REPMTPLANCNT, 
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
            (SUM(t1.REFINANCE_CNT)) AS REFINANCE_CNT, 
          /* AGNCNT */
            (SUM(0)) AS AGNCNT, 
          /* POSSESSION_AMT */
            (SUM(t1.POSSESSION_AMT)) FORMAT=21.4 AS POSSESSION_AMT, 
          /* POSSESSION_CNT */
            (SUM(t1.POSSESSION_CNT)) AS POSSESSION_CNT, 
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
            (SUM(t1.BLACK_BOOK_VALUE)) AS BLACK_BOOK_VALUE, 
          /* SOLD_AMOUNT */
            (SUM(t1.SOLD_AMOUNT)) FORMAT=21.4 AS SOLD_AMOUNT, 
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
            (SUM(t1.SOLD_COUNT)) FORMAT=12.2 AS SOLD_COUNT, 
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
      FROM RU1_LENDINGPRODUCTS_ROLLUP_PRE t1
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
%RUNQUIT(&job,&sub5);

%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
LIBNAME NORECV "E:\SHARED\CADA\SAS DATA\DATAMART\SCOCHRAN";

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub5);

DATA WORK.A&END_DT;
	X = &ENDINGDT;
	FORMAT X MMDDYY10.;
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);

proc sql;
	create table thursdaydates_tmp2 as
		select t1.*, t2.holidayname
		  from thursdaydates_tmp1 t1
			LEFT JOIN bior.i_holidays t2 on (t1.businessdt = datepart(t2.holiday_dt))
		 order by t1.businessdt desc;
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);

proc sql;
	create table work.daily_summary_all_tmp3 as
		select t1.*, t2.thursdayweek
          from work.daily_summary_all_tmp2 t1, work.thursdaydates_tmp3 t2
		 where t1.businessdt = t2.businessdt;
%RUNQUIT(&job,&sub5);

proc sort data=daily_summary_all_tmp3;
	by locnbr businessdt;
%RUNQUIT(&job,&sub5);

data last_report_date;
	set daily_summary_all_tmp3;
	by locnbr businessdt;
	loc_last_reported_dt = businessdt;
	if last.locnbr then output;
	keep locnbr loc_last_reported_dt;
	format loc_last_reported_dt mmddyy10.;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE WORK.holidays(label="HOLIDAYS") AS 
   SELECT /* holidaydt */
            (datepart(t1.HOLIDAY_DT)) FORMAT=mmddyy10. LABEL="holidaydt" AS holidaydt, 
          t1.HOLIDAYNAME
      FROM BIOR.I_HOLIDAYS t1;
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_PreLoad1_pre AS 
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
%RUNQUIT(&job,&sub5);

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
%RUNQUIT(&job,&sub5);


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
			SET QFUND3_TXTITLE_STATUS = 'WAITING_CL'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub5);

%MACRO WAITFORCUSTLIFE();


	%DO %UNTIL (%EVAL(&COUNT_R. >= 1));	
		PROC SQL;
			CREATE TABLE CUST_LIFE_CHECK_TODAY AS
			SELECT INSTANCE
				  ,MAX(BUSINESS_DATE)	AS BUSINESSDT
			FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE INSTANCE = 'QFUND3'
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
						WHERE INSTANCE = 'QFUND3' AND BUSINESSDT >= DHMS(TODAY()-1,00,00,00)
					;
					QUIT;

				%END;
		%ELSE %IF &DAYOFWEEK. = MONDAY %THEN
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'QFUND3' AND BUSINESSDT >= DHMS(TODAY()-2,00,00,00)
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
			SET QFUND3_TXTITLE_STATUS = 'RUNNING'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub5);

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
	  WHERE T1.INSTANCE = 'QFUND3' AND T1.BUSINESS_DATE >= DHMS(TODAY()-5,00,00,00)
      GROUP BY (CALCULATED BUSINESS_DATE),
               t1.LOCATION_NBR,
               t1.INSTANCE,
               t1.PRODUCT,
               (CALCULATED PRODUCT_DESC)
;
%RUNQUIT(&job,&sub5);

PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_QF3_1 AS 
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
%RUNQUIT(&job,&sub5);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub5);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND3",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND3\Dir2",'G');
%RUNQUIT(&job,&sub5);

PROC SQL;
    INSERT INTO SKY.DAILYSUM_DATAMART_QF3 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM DAILY_SUMMARY_ALL_QF3_1
	WHERE BUSINESSDT >= DHMS(TODAY()-5,00,00,00);
%RUNQUIT(&job,&sub5);

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub5);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
RUN;

%PUT &TIMESTAMP;


/*KICK OFF QF1_DAILY UPLOAD*/
SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
				 '&DAILY_FILE_PATH.\TRANSPOSE QFUND3_TXTITLE.SAS'
				 -LOG '&DAILY_LOGPATH.\TRANSPOSE_EADV_&TIMESTAMP..LOG'
				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
TASKNAME=TRANSPOSE_QF1
STATUS=TRANSPOSE_QF1;

/*UPLOAD QF3 TXTITLE*/
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_QFUND3_TXTITLE.SAS";

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.O_DAILY_SUMMARY_ALL
		    SET LAST_REPORT_DT = TO_DATE(TO_CHAR(CURRENT_DATE-1, 'MM/DD/YYYY'), 'MM/DD/YYYY')
			WHERE INSTANCE = 'QFUND3' AND PRODUCT_DESC = 'TX TITLE'
			)
	BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

WAITFOR _ALL_ TRANSPOSE_QF1;


/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%abort cancel;
	%END;

%MEND;

%STOPPROGRAM

