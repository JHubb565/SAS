%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";


%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub12);

DATA _NULL_;
	CALL SYMPUTX('DAILY_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DAILY_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DAILY",'G');
%RUNQUIT(&job,&sub12);

LIBNAME STATIC "//AACADASAS/CADA/SAS DATA/USER/SCOCHRAN/SKYNETCOMPARISON";

LIBNAME OHCSO ORACLE
	PATH=EDWPRD
	SCHEMA=EDW
	USER=&USER
	PASSWORD=&PASSWORD DEFER=YES;

LIBNAME QFUND5 ORACLE
	PATH=EDWPRD
	SCHEMA=QFUND5
	USER=&USER
	PASSWORD=&PASSWORD DEFER=YES;

PROC SQL;
   CREATE TABLE WORK.QF5_PDL_PNL AS 
   SELECT t1.STORE_NUMBER AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          t1.BAD_DEBT AS GROSS_WRITE_OFF, 
          t1.BADDEBT_PMT AS WOR, 
          t1.PNL_AMT AS GROSS_REVENUE, 
          /* PRODUCT_TYPE */
            (COMPRESS(t1.PRODUCT_TYPE)) AS PRODUCT_TYPE
      FROM EDW.QF_BADDEBT_PNLAMT t1
      WHERE t1.SOURCE_SYSTEM = 'QFUND5' AND (CALCULATED PRODUCT_TYPE) = 'PDL';
%RUNQUIT(&job,&sub12);

DATA _NULL_;
CALL SYMPUTX('THIS',PUT(DHMS(INTNX('MONTH',TODAY(),-24,'B'),0,0,0),DATETIME20.),'G');
CALL SYMPUTX('THAT',PUT(DHMS(INTNX('DAY',TODAY(),-1,'B'),0,0,0),DATETIME20.),'G');
RUN;

%PUT THIS= &THIS.;
%PUT THAT= &THAT.;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='EDWPRD');
    CREATE TABLE WORK.RCC_INELIGIBLE0 AS
	SELECT *
	FROM CONNECTION TO ORACLE
	(
	    SELECT *
	    FROM EDW.CSO_DAILY_CENTER_SUMMARY
	    WHERE TRAN_DATE >= TRUNC(CURRENT_DATE)-10
	         AND DESCRIPTION ='RCC Ineligible'
	          AND PRODUCT_TYPE = 'PDL'
	)
	;
	DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub12);

PROC SQL;
        CREATE TABLE WORK.RCC_INELIGIBLE AS
            SELECT ST_CODE AS LOCNBR,
                   DATEPART(TRAN_DATE) AS BUSINESSDT FORMAT=MMDDYY10.,
                   SUM(TODAY_COUNT) AS RCC_INELIGIBLE
            FROM RCC_INELIGIBLE0
        WHERE TRAN_DATE BETWEEN "&THIS."DT AND "&THAT."DT
             AND DESCRIPTION ='RCC Ineligible'
              AND PRODUCT_TYPE = 'PDL'
        GROUP BY ST_CODE, DATEPART(TRAN_DATE);
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.EARNED_FEES AS 
   SELECT t1.TRAN_DATE, 
          /* TRANDT */
            (datepart(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS TRANDT, 
          t1.ST_CODE, 
          /* begindt */
            (intnx('month',today(),-24,'beginning')) AS begindt, 
          t1.STATE_CODE, 
          t1.DESCRIPTION, 
          t1.PRODUCT_TYPE, 
          /* SUM_of_TODAY_AMOUNT */
            (SUM(t1.TODAY_AMOUNT)) FORMAT=22.2 AS SUM_of_TODAY_AMOUNT, 
          /* enddt */
            (intnx('day',today(),-1,'beginning')) FORMAT=mmddyy10. AS enddt
      FROM EDW.CSO_DAILY_CENTER_SUMMARY t1
      WHERE t1.DESCRIPTION = 'Earned CSO Fees' AND (CALCULATED TRANDT) BETWEEN (CALCULATED begindt) AND (CALCULATED 
           enddt) AND t1.PRODUCT_TYPE = 'PDL'
      GROUP BY t1.TRAN_DATE,
               (CALCULATED TRANDT),
               t1.ST_CODE,
               (CALCULATED begindt),
               t1.STATE_CODE,
               t1.DESCRIPTION,
               t1.PRODUCT_TYPE,
               (CALCULATED enddt);
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.WOAMT AS 
   SELECT t1.ST_CODE AS LOCNBR, 
          t1.TRAN_DATE, 
          /* BUSINESSDT */
            (DATEPART(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* WOAMTSUM */
            (SUM(CASE WHEN t1.DESCRIPTION = 'Actual Write Off' THEN TODAY_AMOUNT ELSE 0 END)) AS WOAMTSUM, 
          /* WOCNT */
            (SUM(CASE WHEN t1.DESCRIPTION = 'Actual Write Off' THEN TODAY_COUNT ELSE 0 END)) AS WOCNT, 
          /* WOBAMTSUM */
            (SUM(CASE WHEN t1.DESCRIPTION = 'Actual Write Off Bankruptcy/Deceased' THEN TODAY_AMOUNT ELSE 0 END)) AS 
            WOBAMTSUM, 
          /* WOBCNT */
            (SUM(CASE WHEN t1.DESCRIPTION = 'Actual Write Off Bankruptcy/Deceased' THEN TODAY_COUNT ELSE 0 END)) AS 
            WOBCNT, 
          /* WORAMTSUM */
            (SUM(CASE WHEN t1.DESCRIPTION = 'Actual Write Off Recovery' THEN t1.TODAY_AMOUNT ELSE 0 END)) AS WORAMTSUM
      FROM EDW.CSO_DAILY_CENTER_SUMMARY t1
      WHERE t1.DESCRIPTION IN 
           (
           'Actual Write Off',
           'Actual Write Off Bankruptcy/Deceased',
           'Actual Write Off Recovery'
           ) AND t1.TRAN_DATE BETWEEN DHMS(INTNX('MONTH',TODAY(),-24,'B'),00,00,00) AND DHMS(INTNX('DAY',TODAY(),-1,'B'
           ),00,00,00) AND t1.PRODUCT_TYPE = 'PDL'
      GROUP BY t1.ST_CODE,
               t1.TRAN_DATE,
               (CALCULATED BUSINESSDT)
      ORDER BY t1.ST_CODE,
               t1.TRAN_DATE;
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.CSO_LOAN_TRANSACTION_PRE AS 
   SELECT t1.LOAN_ID, 
          t1.LOAN_CODE, 
          t1.LOAN_TRAN_CODE, 
          t1.LOAN_STATUS_ID, 
          t1.TRAN_ID, 
          t1.TRAN_DATE, 
          /* TRAN_DT */
            (DATEPART(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS TRAN_DT, 
          t1.TRANSACTION_AMOUNT, 
          t1.TENDER_TYPE, 
          t1.ABA_CODE, 
          t1.BANK_ACNT_NUM, 
          t1.REFERENCE_NUMBER, 
          t1.PAYMENT_SOURCE, 
          t1.INST_NUM, 
          t1.PAY_PRINCIPAL, 
          t1.PAY_INTEREST, 
          t1.CRF_FEE, 
          t1.CSO_FEE, 
          t1.NSF_FEE, 
          t1.WO_PRINCIPAL, 
          t1.WO_INTEREST, 
          t1.WO_CSO_FEE, 
          t1.WO_FEE, 
          t1.WO_CRF_FEE, 
          t1.WAIVE_AMT, 
          t1.TOTAL_PAID, 
          t1.TOTAL_DUE, 
          t1.BALANCE_PRINCIPAL, 
          t1.BALANCE_CSO_FEE, 
          t1.UNPAID_INF_FEE, 
          t1.UNPAID_CRF_FEE, 
          t1.VOID_ID, 
          t1.ORIG_TRAN_CODE, 
          t1.REV_TRAN_CODE, 
          t1.RAL_TRAN_CODE, 
          t1.REPRESENTMENT_COUNT, 
          t1.REPRESENTMENT_AMT, 
          t1.RTN_REASON_ID, 
          t1.CHECK_STATUS_ID, 
          t1.IS_NCP_TRANSACTION, 
          t1.ST_CODE, 
          t1.ORIG_ST_CODE, 
          t1.DATE_CREATED, 
          t1.CREATED_BY, 
          t1.CHANGE_TENDER_AMT, 
          t1.ETL_DT, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.VEHICLE_STATUS, 
          t1.CALLOFF_FEE, 
          t1.REPO_FEE, 
          t1.SALE_FEE, 
          t1.REPO_COMPANY, 
          t1.CALLOFF_COMPANY, 
          t1.SALVAGE_COMPANY, 
          t1.AUCTION_COMPANY, 
          t1.PRODUCT_TYPE, 
          t1.COUPON_AMT
      FROM EDW.CSO_LOAN_TRANSACTION t1
      WHERE t1.TRAN_DATE >= INTNX('MONTH',TODAY(),-24,'BEGINNING') AND t1.PRODUCT_TYPE = 'PDL';
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.CSO_FEE_OLD AS 
   SELECT t1.ST_CODE, 
          /* TRAN_DTTM */
            (dhms(t1.TRAN_DT,0,0,0)) FORMAT=datetime20. AS TRAN_DTTM, 
          /* SUM_OF_CSO_FEE */
            (SUM(-(t1.CSO_FEE))) AS SUM_OF_CSO_FEE
      FROM WORK.CSO_LOAN_TRANSACTION_PRE t1
      WHERE t1.TRAN_DT > '30Nov2013'd AND t1.TRAN_ID IN 
           (
           'BUY',
           'PAY',
           'DP',
           'DFP',
           'NSF',
           'PAYIL'
           ) AND t1.TRAN_DT < '1Jun2015'd
      GROUP BY t1.ST_CODE,
               (CALCULATED TRAN_DTTM)
	  ORDER BY T1.ST_CODE, (CALCULATED TRAN_DTTM);
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.QFUND_WRITEOFFS AS 
   SELECT t1.ST_CODE AS LOCNBR, 
          t1.LOAN_CODE, 
          /* TRANDT */
            (DATEPART(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS TRANDT, 
          t1.TRAN_DATE, 
          /* WOAMTSUM */
            (case when
              t1.tran_id = 'WO' then t1.TOTAL_DUE
              else 0
            end) AS WOAMTSUM, 
          /* WOBAMTSUM */
            (case when
              t1.tran_id in ('WOB','WOD') then t1.TOTAL_DUE
              else 0
            end) AS WOBAMTSUM, 
          /* WO_CSO_FEE */
            (case when
              t1.tran_id = 'WO' then t1.WO_CSO_FEE
              else 0
            end) AS WO_CSO_FEE, 
          /* WOB_CSO_FEE */
            (case when
              t1.tran_id in ('WOB','WOD') then t1.WO_CSO_FEE
              else 0
            end) AS WOB_CSO_FEE
      FROM EDW.CSO_LOAN_TRANSACTION t1
      WHERE t1.PRODUCT_TYPE = 'PDL';
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.QFUND56_WRITEOFFS_BY_DAY AS 
   SELECT t1.LOCNBR, 
          /* TRANDT */
            (DHMS(t1.TRANDT,0,0,0)) FORMAT=DATETIME20. AS TRANDT, 
          /* WOAMTSUM */
            (SUM(t1.WOAMTSUM)) AS WOAMTSUM, 
          /* WOBAMTSUM */
            (SUM(t1.WOBAMTSUM)) AS WOBAMTSUM, 
          /* WO_CSO_FEE */
            (SUM(t1.WO_CSO_FEE)) AS WO_CSO_FEE, 
          /* WOB_CSO_FEE */
            (SUM(t1.WOB_CSO_FEE)) AS WOB_CSO_FEE
      FROM WORK.QFUND_WRITEOFFS t1
      GROUP BY t1.LOCNBR,
               (CALCULATED TRANDT);
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.QFUND56_PAYDAY_DAILYSUMMARY_PRE AS 
   SELECT /* Product */
            ("PAYDAY") LABEL="Product" AS Product, 
          /* pos */
            ("QFUND") LABEL="pos" AS pos, 
          /* INSTANCE */
            ('QFUND5-6') LABEL="INSTANCE" AS INSTANCE, 
          /* bankmodel */
            ("CSO") LABEL="bankmodel" AS bankmodel, 
          t3.BRND_CD AS BRANDCD, 
          t3.CTRY_CD AS COUNTRYCD, 
          t3.ADR_CITY_NM AS CITY, 
          t3.ST_PVC_CD AS STATE, 
          t3.MAIL_CD AS ZIP, 
          t3.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t3.HIER_ZONE_NBR AS ZONENBR, 
          t3.HIER_ZONE_NM AS ZONENAME, 
          t3.HIER_RGN_NBR AS REGIONNBR, 
          t3.HIER_RDO_NM AS REGIONRDO, 
          t3.HIER_DIV_NBR AS DIVISIONNBR, 
          t3.HIER_DDO_NM AS DIVISIONDDO, 
          t3.LOC_NM AS Location_Name, 
          t1.LOCATION_NBR AS LOCNBR, 
          t3.OPEN_DT AS LOC_OPEN_DATE, 
          t3.CLS_DT AS LOC_CLOSE_DATE, 
          /* businessdt */
            (datepart(t1.BUSINESS_DT)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* begindt */
            (intnx('month',today(),-24,'beginning')) FORMAT=mmddyy10. LABEL="begindt" AS begindt, 
          t1.NEW_LOANS_CNT AS advcnt, 
          /* advamtsum */
            (t1.NEW_ADVANCE_AMOUNT) LABEL="advamtsum" AS advamtsum, 
          t1.CSO_FEE AS ADVFEEAMT, 
          t1.NEW_CUSTOMERS_CNT AS newcustdealcnt, 
          t1.OPEN_LOANS_PROCEEDS_AMT_SUM AS totadvrecv, 
/*		  t1.OPEN_LOANS_AMT_SUM AS totadvrecv, */
          /* totadvfeerecv */
            (sum(t1.OPEN_LOANS_CSO_FEE_AMT_SUM,t1.OPEN_LOANS_CRF_FEE_AMT_SUM)) AS totadvfeerecv, 
          t2.OUTSTANDING_DEFAULT_CNT AS defaultcnt, 
          t1.PAST_DUE_CNT AS PASTDUECNT_1, 
          t1.PAST_DUE_AMT_SUM AS PASTDUEAMT_1, 
          t2.OUTSTANDING_DEFAULT_AMT AS totdefaultrecv, 
          t1.REFINANCE_CNT, 
          /* EARNEDFEES */
            (CASE WHEN T1.BUSINESS_DT < '01JUN2015:00:00:00'DT then t5.SUM_OF_CSO_FEE else (CASE 
               WHEN . = t4.SUM_of_TODAY_AMOUNT THEN 0
               ELSE t4.SUM_of_TODAY_AMOUNT
            END) end) AS EARNEDFEES, 
          t1.OPEN_LOANS_CNT AS heldcnt, 
          /* WOAMTSUM */
            (CASE WHEN (datepart(t1.BUSINESS_DT)) >= '01JUN2015'D THEN t2.WRITEOFF_AMT ELSE 
            (SUM(t2.WRITEOFF_AMT,-t6.WO_CSO_FEE)) END) AS WOAMTSUM, 
          t2.WRITEOFF_CNT AS WOCNT, 
          /* WOBAMTSUM */
            (case when t1.BUSINESS_DT >= '01JUN2015'd THEN (sum(t2.WRITEOFF_BANKRUPT_AMT,t2.WRITEOFF_DECEASED_AMT)) 
            ELSE (sum((sum(t2.WRITEOFF_BANKRUPT_AMT,t2.WRITEOFF_DECEASED_AMT)),-t6.WOB_CSO_FEE)) END) AS WOBAMTSUM, 
          t2.WRITEOFF_BANKRUPT_CNT AS WOBCNT, 
          t2.WRITEOFF_DECEASED_CNT AS WODCNT, 
          t2.WRITEOFF_RECOVERY_AMT AS woramtsum, 
          /* Enddt */
            (intnx('day',TODAY(),-1,'beginning')) FORMAT=mmddyy10. LABEL="Enddt" AS Enddt
      FROM QFUND5.NCP_DAILYSUMMARY t1
           LEFT JOIN EDW.D_LOCATION t3 ON (t1.LOCATION_NBR = t3.LOC_NBR)
           LEFT JOIN OHCSO.DAILY_FINANCE_SUMMARY t2 ON (t1.BUSINESS_DT = t2.BUSINESS_DT) AND (t1.LOCATION_NBR = 
          t2.STORE_NBR) AND (t1.PRODUCT_TYPE = t2.PRODUCT_CD)
           LEFT JOIN WORK.EARNED_FEES t4 ON (t1.BUSINESS_DT = t4.TRAN_DATE) AND (t1.LOCATION_NBR = t4.ST_CODE)
           LEFT JOIN WORK.CSO_FEE_OLD t5 ON (t1.LOCATION_NBR = t5.ST_CODE) AND (t1.BUSINESS_DT = t5.TRAN_DTTM)
           LEFT JOIN WORK.QFUND56_WRITEOFFS_BY_DAY t6 ON (t1.LOCATION_NBR = t6.LOCNBR) AND (t1.BUSINESS_DT = t6.TRANDT)
      WHERE (CALCULATED businessdt) BETWEEN (CALCULATED begindt) AND (CALCULATED Enddt) AND t3.ST_PVC_CD NOT IS MISSING 
           AND t1.PRODUCT_TYPE = 'PDL';
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT AS 
   SELECT t1.ST_CODE AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_AMT */
            (SUM(t1.TRANSACTION_AMOUNT)) FORMAT=12.2 AS DEFAULT_AMT, 
          /* DEFAULT_CNT */
            (COUNT(t1.LOAN_CODE)) AS DEFAULT_CNT
      FROM EDW.CSO_LOAN_TRANSACTION t1
      WHERE t1.PRODUCT_TYPE = 'PDL' AND t1.TRAN_ID = 'DEF'
      GROUP BY t1.ST_CODE,
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS AS 
   SELECT t1.ST_CODE AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRAN_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_PMT */
            (SUM(-t1.TRANSACTION_AMOUNT)) AS DEFAULT_PMT
      FROM EDW.CSO_LOAN_TRANSACTION t1
      WHERE t1.TRAN_ID = 'DFP' AND t1.PRODUCT_TYPE = 'PDL'
      GROUP BY t1.ST_CODE,
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub12);

PROC SQL;
	CREATE TABLE PWO_QF5_CURR AS 
		SELECT 
			 STORE_NUMBER AS LOCNBR
			,DHMS(DATEPART(ETL_DT),00,00,00) AS BUSINESSDT FORMAT DATETIME20.
			,SUM(CASE WHEN DHMS(DATEPART(ETL_DT),00,00,00) = DHMS(INTNX('MONTH',DATEPART(PWO_DATE),0,'B'),00,00,00)				  
					  AND PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(ETL_DT),0,'B'),00,00,00)
					  AND DHMS(INTNX('MONTH',DATEPART(ETL_DT),0,'E'),00,00,00)
					  	  THEN PWO_AMT
					  ELSE 0
				 END) AS BEGIN_PWO_AMT_PRE
			,'QFUND5-6' AS INSTANCE
			,CASE WHEN PRODUCT_TYPE = 'ILP' 
					  THEN 'OH CSO INSTALLMENT'
				  WHEN PRODUCT_TYPE = 'TLP'
				  	  THEN 'OH CSO TITLE'
				  WHEN PRODUCT_TYPE = 'OHCSO'
				  	  THEN 'CSO CASH ADVANCE'
				  WHEN PRODUCT_TYPE = 'PDL'
				  	  THEN 'CSO CASH ADVANCE'
			 END AS PRODUCT_DESC
		FROM EDW.CSO_PWO
	WHERE CALCULATED BUSINESSDT >= DHMS(INTNX('MONTH',TODAY(),-36,'B'),00,00,00)
		  AND PRODUCT_TYPE = 'PDL'
	GROUP BY
		 STORE_NUMBER
		,CALCULATED BUSINESSDT
		,PRODUCT_TYPE
	ORDER BY 
		 STORE_NUMBER
		,CALCULATED PRODUCT_DESC
		,CALCULATED BUSINESSDT
;
%RUNQUIT(&job,&sub12);

PROC SQL;
	CREATE TABLE PWO_QF5_NON_CURR AS 
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
			,SUM(CASE WHEN PWO_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(ETL_DT)-1,2,'B'),00,00,00)
					  AND DHMS(INTNX('MONTH',DATEPART(ETL_DT)-1,2,'E'),00,00,00)
					  	  THEN PWO_AMT
					  ELSE 0
				 END) AS NEXT_2_MONTH_PWO_AMT
			,'QFUND5-6' AS INSTANCE
			,CASE WHEN PRODUCT_TYPE = 'ILP' 
					  THEN 'OH CSO INSTALLMENT'
				  WHEN PRODUCT_TYPE = 'TLP'
				  	  THEN 'OH CSO TITLE'
				  WHEN PRODUCT_TYPE = 'OHCSO'
				  	  THEN 'CSO CASH ADVANCE'
				  WHEN PRODUCT_TYPE = 'PDL'
				  	  THEN 'CSO CASH ADVANCE'
			 END AS PRODUCT_DESC
		FROM EDW.CSO_PWO
	WHERE CALCULATED BUSINESSDT >= DHMS(INTNX('MONTH',TODAY(),-36,'B'),00,00,00)
		  AND PRODUCT_TYPE = 'PDL'
	GROUP BY
		 STORE_NUMBER
		,CALCULATED BUSINESSDT
		,PRODUCT_TYPE
	ORDER BY 
		 STORE_NUMBER
		,CALCULATED PRODUCT_DESC
		,CALCULATED BUSINESSDT
;
%RUNQUIT(&job,&sub12);

PROC SQL;
	CREATE TABLE WORK.PWO_QF5_PRE1 AS
		SELECT * FROM WORK.PWO_QF5_CURR
		OUTER UNION CORR
		SELECT * FROM WORK.PWO_QF5_NON_CURR
;
%RUNQUIT(&job,&sub12);

PROC SQL;
	CREATE TABLE WORK.PWO_QF5_PRE AS
		SELECT 
			LOCNBR,
			BUSINESSDT as BUSINESS_DT,
			INSTANCE,
			PRODUCT_DESC,
			SUM(BEGIN_PWO_AMT_PRE) AS BEGIN_PWO_AMT_PRE,
			SUM(CURRENT_PWO_AMT) AS CURRENT_PWO_AMT,
			SUM(NEXT_MONTH_PWO_AMT) AS NEXT_MONTH_PWO_AMT,
			SUM(NEXT_2_MONTH_PWO_AMT) AS NEXT_2_MONTH_PWO_AMT
		FROM WORK.PWO_QF5_PRE1
	GROUP BY LOCNBR,
			 BUSINESSDT,
			 INSTANCE,
			 PRODUCT_DESC
;
%RUNQUIT(&job,&sub12);
			
DATA PWO_QF5;
	SET WORK.PWO_QF5_PRE;
	BY LOCNBR;
	IF FIRST.LOCNBR OR DAY(DATEPART(BUSINESS_DT)) = 1 THEN
		DO;
			BEGIN_PWO_AMT = CURRENT_PWO_AMT;
			RETAIN BEGIN_PWO_AMT;
		END;
	BUSINESSDT = DATEPART(BUSINESS_DT);
	FORMAT BUSINESSDT MMDDYY10.;
	DROP BEGIN_PWO_AMT_PRE BUSINESS_DT;
%RUNQUIT(&job,&sub12);

DATA BEGIN_PWO_AMT;
	SET WORK.PWO_QF5;
	MONTH = MONTH(BUSINESSDT);
	YEAR = YEAR(BUSINESSDT);
	WHERE DAY(BUSINESSDT) = 1;
	KEEP LOCNBR BUSINESSDT BEGIN_PWO_AMT MONTH YEAR;
%RUNQUIT(&job,&sub12);

%MACRO RCC(BEGINDT_1/*Start Date*/,ENDDT_1/*End Date*/);
DATA _NULL_;
	CALL SYMPUTX('BEGINDT',&BEGINDT_1,'G');
	CALL SYMPUTX('ENDDT',&ENDDT_1,'G');
RUN;

DATA RCC;
LOCNBR= .;
BUSINESSDT= ''D;
RCC= .;
FORMAT BUSINESSDT MMDDYY10.;
IF 1 = 0;
RUN;

%DO I=&BEGINDT %TO &ENDDT;
PROC SQL;
	CREATE TABLE RCC_&I AS 
		SELECT ST_CODE AS LOCNBR
		      ,&I AS BUSINESSDT FORMAT MMDDYY10.
			  ,COUNT(DISTINCT LOAN_CODE) AS RCC
		FROM EDW.CSO_LOAN_SUMMARY
	WHERE LOAN_IN_RCC = 'Y'
	 	  AND PRODUCT_TYPE = 'PDL'
	      AND DATEPART(EFFECTIVE_BEGIN_DT) <= &I
		  AND DATEPART(EFFECTIVE_END_DT) >= &I
	GROUP BY ST_CODE
	        ,CALCULATED BUSINESSDT;
QUIT;

PROC APPEND BASE=WORK.RCC DATA=RCC_&I;
RUN;
%END;
%MEND;

%RCC(TODAY()-1,today()-1)

PROC APPEND BASE=SKYNET.QF5_PDL_RCC DATA=WORK.RCC;
%RUNQUIT(&job,&sub12);

PROC SORT DATA=SKYNET.QF5_PDL_RCC DUPOUT=DUPS NODUPKEY;
BY LOCNBR BUSINESSDT;
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE QFUND56_PAYDAY_DAILYSUMMARY AS 
   SELECT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.bankmodel, 
          t1.BRANDCD, 
          t1.COUNTRYCD, 
          t1.CITY, 
          t1.STATE, 
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
          t1.advamtsum, 
          t1.advcnt, 
          t1.ADVFEEAMT, 
          t1.newcustdealcnt, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.defaultcnt, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.totdefaultrecv, 
          t1.EARNEDFEES, 
          t1.REFINANCE_CNT, 
          t1.heldcnt, 
          /* WOAMTSUM */
            (CASE WHEN t1.businessdt < '01JUN2015'D THEN t1.WOAMTSUM ELSE t7.WOAMTSUM END ) AS WOAMTSUM, 
          /* WOCNT */
            (CASE WHEN t1.businessdt < '01JUN2015'D THEN t1.WOCNT ELSE t7.WOCNT END ) AS WOCNT, 
          /* WOBAMTSUM */
            (CASE WHEN t1.businessdt < '01JUN2015'D THEN t1.WOBAMTSUM ELSE t7.WOBAMTSUM END ) AS WOBAMTSUM, 
          /* WOBCNT */
            (CASE WHEN t1.businessdt < '01JUN2015'D THEN t1.WOBCNT ELSE t7.WOBCNT END) AS WOBCNT, 
          /* WORAMTSUM */
            (CASE WHEN t1.businessdt < '01JUN2015'D THEN t1.woramtsum ELSE t7.WORAMTSUM END) AS WORAMTSUM, 
          t2.DEFAULT_AMT, 
          t2.DEFAULT_CNT, 
          t3.DEFAULT_PMT, 
          t4.CURRENT_PWO_AMT, 
          t4.NEXT_MONTH_PWO_AMT, 
          t4.NEXT_2_MONTH_PWO_AMT, 
          t5.RCC AS RCC_IN_PROCESS, 
          t6.RCC_INELIGIBLE, 
          t8.GROSS_WRITE_OFF, 
          t8.WOR, 
          t8.GROSS_REVENUE, 
          t1.Enddt, 
          /* MONTH */
            (MONTH(T1.BUSINESSDT)) AS MONTH, 
          /* YEAR */
            (YEAR(T1.BUSINESSDT)) AS YEAR
      FROM WORK.QFUND56_PAYDAY_DAILYSUMMARY_PRE t1
           LEFT JOIN WORK.DEFAULT_AMT_CNT t2 ON (t1.LOCNBR = t2.LOCNBR) AND (t1.businessdt = t2.BUSINESSDT)
           LEFT JOIN WORK.DEFAULT_PMTS t3 ON (t1.LOCNBR = t3.LOCNBR) AND (t1.businessdt = t3.BUSINESSDT)
           LEFT JOIN WORK.PWO_QF5 t4 ON (t1.LOCNBR = t4.LOCNBR) AND (t1.businessdt = t4.BUSINESSDT)
           LEFT JOIN SKYNET.QF5_PDL_RCC t5 ON (t1.LOCNBR = t5.LOCNBR) AND (t1.businessdt = t5.BUSINESSDT)
           LEFT JOIN WORK.RCC_INELIGIBLE t6 ON (t1.LOCNBR = t6.LOCNBR) AND (t1.businessdt = t6.BUSINESSDT)
           LEFT JOIN WORK.WOAMT t7 ON (t1.LOCNBR = t7.LOCNBR) AND (t1.businessdt = t7.BUSINESSDT)
           LEFT JOIN WORK.QF5_PDL_PNL t8 ON (t1.LOCNBR = t8.LOCNBR) AND (t1.businessdt = t8.BUSINESSDT);
%RUNQUIT(&job,&sub12);

PROC SQL;
	CREATE TABLE QFUND56_PAYDAY_DAILYSUMMARY AS
		SELECT T1.*,
			   CASE WHEN T2.BEGIN_PWO_AMT = . THEN 0 ELSE T2.BEGIN_PWO_AMT END AS BEGIN_PWO_AMT
		FROM WORK.QFUND56_PAYDAY_DAILYSUMMARY T1
		LEFT JOIN 
		WORK.BEGIN_PWO_AMT T2
		ON (T1.LOCNBR = T2.LOCNBR AND
			T1.MONTH = T2.MONTH AND
			T1.YEAR = T2.YEAR)
		WHERE T1.BUSINESSDT >= TODAY() - 10
;
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE QFUND56_PAYDAY_NEW_ORIGINATIONS AS 
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
          /* NEW_ORIGINATIONS */
            (t1.advcnt) AS NEW_ORIGINATIONS, 
          t1.businessdt, 
          /* NEW_ADV_AMT */
            (t1.advamtsum) AS NEW_ADV_AMT, 
          t1.ADVFEEAMT AS NEW_ADVFEE_AMT, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (t1.heldcnt) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* GROSS_REVENUE */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN (t1.EARNEDFEES) ELSE 0 END) AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN (SUM(t1.WOAMTSUM,t1.WOBAMTSUM)) ELSE 0 END) AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN ((SUM(t1.WOAMTSUM,t1.WOBAMTSUM)) - t1.WORAMTSUM) ELSE 0 END) 
            AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN ((t1.EARNEDFEES) - ((SUM(t1.WOAMTSUM,t1.WOBAMTSUM)) - 
            t1.WORAMTSUM)) ELSE 0 END ) AS NET_REVENUE, 
          /* PRODUCT_DESC */
            (case
              when product = "TITLE" then "OH CSO TITLE"
              when product = "INSTALLMENT" then "OH CSO INSTALLMENT"
              when product = "PAYDAY" then "CSO CASH ADVANCE"
            end) AS PRODUCT_DESC, 
          t1.DEFAULT_AMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_PMT, 
          t1.BEGIN_PWO_AMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.RCC_IN_PROCESS, 
          t1.RCC_INELIGIBLE, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.REFINANCE_CNT, 
          t1.defaultcnt AS DEFAULT_LOANS_OUTSTANDING, 
          t1.totdefaultrecv AS TOTDEFAULTRECV, 
          t1.woamtsum, 
          t1.WOCNT, 
          t1.wobamtsum, 
          t1.WOBCNT, 
          /* WORAMTSUM */
            (CASE WHEN T1.BUSINESSDT < '01APR2017'D THEN t1.WORAMTSUM ELSE 0 END) AS WORAMTSUM, 
          t1.heldcnt
      FROM QFUND56_PAYDAY_DAILYSUMMARY t1;
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

/*-------------*/
/* QFUND 5 PDL */
/*-------------*/
PROC SQL;
	CREATE TABLE QFUND5_PDL AS
		SELECT
		    CASE WHEN COMPRESS(PRODUCT_TYPE) = 'PDL' THEN 'PAYDAY' 
			     ELSE PRODUCT_TYPE 
            END AS PRODUCT
		   ,'CSO CASH ADVANCE' AS PRODUCT_DESC
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
	WHERE COMPRESS(PRODUCT_TYPE) = 'PDL' AND INSTANCE = 'QFUND5-6'
;
%RUNQUIT(&job,&sub12);

PROC SQL;
CREATE TABLE WORK.RU1_LENDINGPRODUCTS_ROLLUP_PRE AS 
	SELECT * FROM QFUND56_PAYDAY_NEW_ORIGINATIONS
		OUTER UNION CORR 
	SELECT * FROM QFUND5_PDL
;
%RUNQUIT(&job,&sub12);

/* SUM ALL METRICS TO COLLAPSE DUPLICATE CENTER/DAY FROM PNL */
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
            (SUM(0)) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (SUM(0)) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (SUM(0)) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (SUM(0)) AS ACTUAL_DURATION_FEES, 
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
            (SUM(t1.RCC_IN_PROCESS)) AS RCC_IN_PROCESS, 
          /* RCC_INELIGIBLE */
            (SUM(t1.RCC_INELIGIBLE)) FORMAT=11. AS RCC_INELIGIBLE, 
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
%RUNQUIT(&job,&sub12);

%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
LIBNAME NORECV "E:\SHARED\CADA\SAS DATA\DATAMART\SCOCHRAN";

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub12);

DATA WORK.A&END_DT;
	X = &ENDINGDT;
	FORMAT X MMDDYY10.;
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

proc sql;
	create table thursdaydates_tmp2 as
		select t1.*, t2.holidayname
		  from thursdaydates_tmp1 t1
			LEFT JOIN bior.i_holidays t2 on (t1.businessdt = datepart(t2.holiday_dt))
		 order by t1.businessdt desc;
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

proc sql;
	create table work.daily_summary_all_tmp3 as
		select t1.*, t2.thursdayweek
          from work.daily_summary_all_tmp2 t1, work.thursdaydates_tmp3 t2
		 where t1.businessdt = t2.businessdt;
%RUNQUIT(&job,&sub12);

proc sort data=daily_summary_all_tmp3;
	by locnbr businessdt;
%RUNQUIT(&job,&sub12);

data last_report_date;
	set daily_summary_all_tmp3;
	by locnbr businessdt;
	loc_last_reported_dt = businessdt;
	if last.locnbr then output;
	keep locnbr loc_last_reported_dt;
	format loc_last_reported_dt mmddyy10.;
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE WORK.holidays(label="HOLIDAYS") AS 
   SELECT /* holidaydt */
            (datepart(t1.HOLIDAY_DT)) FORMAT=mmddyy10. LABEL="holidaydt" AS holidaydt, 
          t1.HOLIDAYNAME
      FROM BIOR.I_HOLIDAYS t1;
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);

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
%RUNQUIT(&job,&sub12);


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
			SET QFUND5_PAYDAY_STATUS = 'WAITING_CL'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub12);

%MACRO WAITFORCUSTLIFE();


	%DO %UNTIL (%EVAL(&COUNT_R. >= 1));	
		PROC SQL;
			CREATE TABLE CUST_LIFE_CHECK_TODAY AS
			SELECT INSTANCE
				  ,MAX(BUSINESS_DATE)	AS BUSINESSDT
			FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE INSTANCE = 'QFUND5-6'
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
						WHERE INSTANCE = 'QFUND5-6' AND BUSINESSDT >= DHMS(TODAY()-1,00,00,00)
					;
					QUIT;

				%END;
		%ELSE %IF &DAYOFWEEK. = MONDAY %THEN
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'QFUND5-6' AND BUSINESSDT >= DHMS(TODAY()-2,00,00,00)
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
			SET QFUND5_PAYDAY_STATUS = 'RUNNING'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub12);

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
	  WHERE T1.INSTANCE = 'QFUND5-6' AND T1.BUSINESS_DATE >= DHMS(TODAY()-5,00,00,00)
      GROUP BY (CALCULATED BUSINESS_DATE),
               t1.LOCATION_NBR,
               t1.INSTANCE,
               t1.PRODUCT,
               (CALCULATED PRODUCT_DESC)
;
%RUNQUIT(&job,&sub12);

PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_QF5P_1 AS 
   SELECT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          "QFUND5-6"				AS INSTANCE, 
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
%RUNQUIT(&job,&sub12);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub12);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND5\Payday",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND5\Payday\Dir2",'G');
%RUNQUIT(&job,&sub12);

PROC SQL;
    INSERT INTO SKY.DAILYSUM_DATAMART_QF5P (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM DAILY_SUMMARY_ALL_QF5P_1
	WHERE BUSINESSDT >= DHMS(TODAY()-5,00,00,00);
%RUNQUIT(&job,&sub12);

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub12);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
RUN;

%PUT &TIMESTAMP;


/*KICK OFF QF5_DAILY UPLOAD*/
SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
				 '&DAILY_FILE_PATH.\TRANSPOSE QFUND5_PAYDAY.SAS'
				 -LOG '&DAILY_LOGPATH.\TRANSPOSE_QFUND5_PAYDAY_&TIMESTAMP..LOG'
				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
TASKNAME=TRANSPOSE_QF5_PAYDAY
STATUS=TRANSPOSE_QF5_PAYDAY_;

/*UPLOAD QF5PDL*/
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_QFUND5_P.SAS";



PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.O_DAILY_SUMMARY_ALL
		    SET LAST_REPORT_DT = TO_DATE(TO_CHAR(CURRENT_DATE-1, 'MM/DD/YYYY'), 'MM/DD/YYYY')
			WHERE INSTANCE = 'QFUND5-6' AND PRODUCT = 'PAYDAY'
			)
	BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

WAITFOR _ALL_ TRANSPOSE_QF5_PAYDAY;


/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%abort cancel;
	%END;

%MEND;

%STOPPROGRAM