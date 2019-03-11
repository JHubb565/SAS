%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM\";

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub31);

OPTIONS MLOGIC MPRINT SYMBOLGEN;

LIBNAME BIOR ORACLE 
	USER=&USER 
	PASSWORD=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR DEFER=YES;

LIBNAME TMP_TBLS ORACLE 
	USER=&USER 
	PASSWORD=&PASSWORD
	PATH=BIOR
	SCHEMA=TEMPTABLES DEFER=YES;

LIBNAME BIORDEV ORACLE
	USER = &USER
	PASSWORD = &PASSWORD
	PATH=BIOR
	SCHEMA=BIORDEV DEFER=YES;

LIBNAME OMNICORE postgres 
	server=CLTEDBPROD1 
	port=5444 
	user=&EDBUSER. 
	password=&EDBPASSWORD. 
	db='coredb' 
	schema='sc_core';

LIBNAME OMNIFODS postgres 
	server=CLTEDBPROD1 
	port=5444 
	user=&EDBUSER.
	password=&EDBPASSWORD.
	db='coredb' 
	schema='sc_ods_fin';

LIBNAME OMNIREF postgres 
	server=CLTEDBPROD1 
	port=5444 
	user=&EDBUSER.
	password=&EDBPASSWORD.
	db='coredb' 
	schema='sc_reference_data';

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
%RUNQUIT(&job,&sub31);

%PUT &LASTWEEK;

DATA _NULL_;
%LET _SYSTEM = 'QFUND_X';
RUN;

/*PULLS Information from SC_CORE.PRODUCT_MASTER*/
PROC SQL;
CREATE TABLE WORK.QFX_BASE AS
	SELECT
		T1.PRODUCT_CATEGORY_CD AS PRODUCT,
		T1.SOURCE_SYSTEM_CD AS POS,
		T1.SOURCE_SYSTEM_CD AS INSTANCE,
		T1.SOURCE_PRODUCT_ID AS DEALNBR,
		T3.SSN_ENCRYPTED AS SSN,
		COMPRESS(PUT(T2.OMNI_CUSTOMER_ID,30.)) AS OMNINBR,
		T1.CENTER_NBR,
		T1.STATE_CD AS STATE,
		DATEPART(T1.ORIGINATION_DTTM) AS DEAL_DT FORMAT = DATE9.,
		T1.ORIGINATION_DTTM AS DEAL_DTTM FORMAT = DATETIME.,
		T1.SOURCE_CUSTOMER_ID AS CUSTNBR,
		T1.PRINCIPAL_AMT AS ADVAMT,
		T1.FINANCE_CHG_AMT AS FEEAMT,
		T1.FEE_NSF_AMT AS NSFFEEAMT,
		T1.FEE_LATE_AMT AS LATEFEEAMT,
		T1.ADDITIONAL_CHG_AMT AS OTHERFEEAMT,
		T1.FEE_WAIVED_AMT AS WAIVEDFEEAMT,
		DATEPART(T1.DUE_DTTM) AS DUEDT FORMAT = DATE9.,
		DATEPART(T1.SATISFIED_DTTM) AS DEALENDDT FORMAT = DATE9.,
		DATEPART(T1.WRITEOFF_DTTM) AS WRITEOFFDT FORMAT = DATE9.,
		T1.PRODUCT_STATUS_CD AS DEALSTATUSCD,
		T1.ETL_CREATED_DTTM AS ETLDT FORMAT = datetime.,
		T1.PRODUCT_CATEGORY_CD AS PRODUCTCD,
		T1.SOURCE_MODIFIED_DTTM AS UPDATEDT
	FROM OMNICORE.PRODUCT_MASTER T1
		LEFT JOIN OMNICORE.CUSTOMER_MASTER T2 ON (T1.SOURCE_SYSTEM_CD = T2.SOURCE_SYSTEM_CD AND T1.SOURCE_CUSTOMER_ID = T2.SOURCE_CUSTOMER_ID)
		LEFT JOIN OMNICORE.CUSTOMER_SSN T3 ON (T1.SOURCE_SYSTEM_CD = T3.SOURCE_SYSTEM_CD AND T1.SOURCE_CUSTOMER_ID = T3.SOURCE_CUSTOMER_ID)
	WHERE T1.SOURCE_MODIFIED_DTTM >= &LASTWEEK
	AND T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.SOURCE_DELETED_FLG = 'N';
%runquit(&job,&sub31);

/*GET CENTER INFORMATION AND ZONE INFORMATION*/
PROC SQL;
CREATE TABLE WORK.QFX_CENTER_INFO AS
	SELECT
		T1.CENTER_NBR,
		T1.CENTER_CD,
		T1.BRAND_CD AS BRANDCD,
		T2.COUNTRY_CD AS COUNTRYCD,
		T2.ADDRESS_CITY AS CITY,
		T2.STATE_CD AS STATE,
		T2.POSTAL_CD AS ZIP,
		T1.BUSINESS_UNIT_CD AS BUSINESS_UNIT,
		DATEPART(T7.OPEN_DT) AS LOC_OPEN_DT FORMAT = DATE9.,
		DATEPART(T7.CLS_DT) AS LOC_CLOSE_DT FORMAT = DATE9.,
		T3.ZONE_CD AS ZONENBR,
		UPPER(T4.NAME_LAST) AS ZONENAME,
		T3.REGION_CD AS REGIONNBR,
		CASE
			WHEN T5.NAME_LAST = 'OPEN' THEN 'OPEN'
			ELSE CATX(' ',UPPER(T5.NAME_FIRST),UPPER(T5.NAME_LAST)) 
		END AS REGIONRDO,
		T3.DIVISION_CD AS DIVISIONNBR,
		CASE
			WHEN T6.NAME_LAST = 'OPEN' THEN 'OPEN'
			ELSE CATX(' ',UPPER(T6.NAME_FIRST),UPPER(T6.NAME_LAST)) 
		END AS DIVISIONDDO
		FROM OMNICORE.CENTER_MASTER T1
			LEFT JOIN OMNICORE.LOCATION_ADDRESS T2 ON (T1.LOCATION_ID = T2.LOCATION_ID)
			LEFT JOIN OMNICORE.hierarchy T3 ON (T1.CENTER_NBR = T3.CENTER_NBR)
			LEFT JOIN OMNICORE.ZONE_CODE T4 ON (T3.ZONE_CD = T4.ZONE_CD)
			LEFT JOIN OMNICORE.REGION_CODE T5 ON (T3.ZONE_CD = T5.ZONE_CD AND T3.REGION_CD = T5.REGION_CD)
			LEFT JOIN OMNICORE.DIVISION_CODE T6 ON (T3.ZONE_CD = T6.ZONE_CD AND T3.REGION_CD = T6.REGION_CD AND T3.DIVISION_CD = T6.DIVISION_CD)
			LEFT JOIN EDW.D_LOCATION T7 ON (T1.CENTER_NBR = T7.LOC_NBR)
		WHERE T1.CENTER_NBR IN (SELECT CENTER_NBR FROM WORK.QFX_BASE);
%runquit(&job,&sub31);

/*GET DEPOSIT DATE*/
PROC SQL;
CREATE TABLE WORK.QFX_DEPOSITDT AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		INPUT(T1.LOAN_ID,30.) AS SOURCE_PRODUCT_ID,
		DATEPART(T1.TRANSACTION_DTTM) AS DEPOSITDT
	FROM OMNIFODS.FINANCIAL_RECORD_MASTER T1
		LEFT JOIN OMNIFODS.FINANCIAL_RECORD_DETAIL T2 ON (T1.TRANSACTION_ID = T2.TRANSACTION_ID)
	WHERE T1.TRANSACTION_CD = 'PAYMENT'
	AND T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.LOAN_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	AND T2.FINANCIAL_CD = 'TENDER' 
	AND T2.FINANCIAL_DETAIL_CD NOT IN ('CASH','CASHIERSCHECK','MONEYORDER','DEBITCARD','UNKNOWN');
%runquit(&job,&sub31);

PROC SORT DATA=WORK.QFX_DEPOSITDT;
	BY SOURCE_SYSTEM_CD SOURCE_PRODUCT_ID DEPOSITDT;
%runquit(&job,&sub31);

DATA WORK.QFX_DEPOSITDT2;
	SET WORK.QFX_DEPOSITDT;
	BY SOURCE_SYSTEM_CD SOURCE_PRODUCT_ID DEPOSITDT;
	IF FIRST.SOURCE_PRODUCT_ID;
%runquit(&job,&sub31);

PROC SQL;
CREATE TABLE WORK.QFX_DEFAULTDT AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		INPUT(T1.LOAN_ID,30.) AS SOURCE_PRODUCT_ID,
		DATEPART(T1.DEFAULT_DT) AS DEFAULTDT
	FROM OMNIFODS.PRODUCT_SUMMARY T1
	WHERE T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.LOAN_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE);
%runquit(&job,&sub31);

PROC SORT DATA=WORK.QFX_DEFAULTDT;
	BY SOURCE_SYSTEM_CD SOURCE_PRODUCT_ID DEFAULTDT;
%runquit(&job,&sub31);

DATA WORK.QFX_DEFAULTDT2;
	SET WORK.QFX_DEFAULTDT;
	BY SOURCE_SYSTEM_CD SOURCE_PRODUCT_ID DEFAULTDT;
	IF FIRST.SOURCE_PRODUCT_ID;
%runquit(&job,&sub31);

PROC SQL;
CREATE TABLE WORK.TRANSACTION_IDS AS
	SELECT 
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		T1.SOURCE_TRANSACTION_ID,
		T1.TRANSACTION_CD,
		T3.DETAIL_CD,
		T3.FINANCIAL_CD,
		T3.DETAIL_AMT
	FROM OMNICORE.TRANSACTION_MASTER T1
		LEFT JOIN OMNICORE.TRANSACTION_DETAIL T3 ON (T1.SOURCE_SYSTEM_CD = T3.SOURCE_SYSTEM_CD AND T1.SOURCE_TRANSACTION_ID = T3.SOURCE_TRANSACTION_ID)
	WHERE T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE);
%runquit(&job,&sub31);
		
/*GETS INTERESTFEE*/
PROC SQL;
CREATE TABLE WORK.QFX_INTERESTFEE AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		SUM(T1.DETAIL_AMT) AS INTERESTFEE
	FROM WORK.TRANSACTION_IDS T1
	WHERE T1.DETAIL_CD = 'INTERESTFEE'
	AND T1.SOURCE_SYSTEM_CD IN (&_SYSTEM) 
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	AND T1.TRANSACTION_CD IN ('ORIGINATION','RENEWAL','STATEMENT')
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.SOURCE_PRODUCT_ID;
%runquit(&job,&sub31);

/*GETS QFX_CUSTOMARYFEE*/
PROC SQL;
CREATE TABLE WORK.QFX_CUSTOMARYFEE AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		SUM(T1.DETAIL_AMT) AS CUSTOMARYFEE
	FROM WORK.TRANSACTION_IDS T1
	WHERE T1.DETAIL_CD = 'CUSTOMARYFEE'
	AND T1.SOURCE_SYSTEM_CD IN (&_SYSTEM) 
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	AND T1.TRANSACTION_CD IN ('ORIGINATION','RENEWAL','STATEMENT')
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.SOURCE_PRODUCT_ID;
%runquit(&job,&sub31);

/*GETS ADVANCEFEE*/
PROC SQL;
CREATE TABLE WORK.QFX_ADVANCEFEE AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		SUM(T1.DETAIL_AMT) AS FEEAMT
	FROM WORK.TRANSACTION_IDS T1
	WHERE T1.DETAIL_CD = 'ADVANCEFEE'
	AND T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	AND T1.TRANSACTION_CD IN ('ORIGINATION','RENEWAL')
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.SOURCE_PRODUCT_ID;
%runquit(&job,&sub31);

/*GETS REBATEAMT*/
PROC SQL;
CREATE TABLE WORK.QFX_REBATEFEE AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		SUM(T1.DETAIL_AMT) AS REBATEFEE
	FROM WORK.TRANSACTION_IDS T1
	WHERE T1.DETAIL_CD = 'REBATEFEE'
	AND T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.SOURCE_PRODUCT_ID;
%runquit(&job,&sub31);

/*GET PREVIOUS DEALNBR*/
PROC SQL;
CREATE TABLE WORK.PREVDEALNBR AS
	SELECT DISTINCT
		SOURCE_SYSTEM_CD,
		LOAN_ID AS DEALNBR, 
        (input(scan(REFINANCED_LOAN_ID,1,'.'),best32.)) AS PREVDEALNBR
	FROM OMNIFODS.FINANCIAL_RECORD_MASTER
	WHERE REFINANCED_LOAN_ID IS NOT MISSING
	AND SOURCE_SYSTEM_CD IN (&_SYSTEM);
%runquit(&job,&sub31);

/*GET COLLATERAL TYPE*/
PROC SQL;
CREATE TABLE WORK.COLLATERALTYPE AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		T1.COLLATERAL_CD,
		T1.CHECK_AMT,
		CASE
			WHEN T1.COLLATERAL_STATUS_CD = 'WRITEOFF' THEN 'WO'
			WHEN T1.COLLATERAL_STATUS_CD = 'DEFAULT' THEN 'DEF'
			WHEN T1.COLLATERAL_STATUS_CD = 'VOID' THEN 'V'
			WHEN T1.COLLATERAL_STATUS_CD IN ('ACHDEPOSIT','DEPOSITED','DEPOSIT') THEN 'DEP'
			WHEN T1.COLLATERAL_STATUS_CD = 'PAYMENT' THEN 'BUY'
			WHEN T1.COLLATERAL_STATUS_CD = 'BOUGHT' THEN 'BUY'
			WHEN T1.COLLATERAL_STATUS_CD IN ('CLEAR','ACHCLEAR') THEN 'CLR'
			WHEN T1.COLLATERAL_STATUS_CD IN ('HELD','DELINQUENT') THEN 'HLD'
			WHEN T1.COLLATERAL_STATUS_CD = 'PAYMENTPLAN' THEN 'RPP'
			WHEN T1.COLLATERAL_STATUS_CD = 'ACHRETURN' THEN 'RTN'
			ELSE T1.COLLATERAL_STATUS_CD
		END AS COLLATERAL_STATUS_CD,
		T1.CHECK_ID,
		T1.COLLATERAL_NBR
	FROM OMNICORE.PRODUCT_COLLATERAL T1
	WHERE T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.SOURCE_PRODUCT_ID
	HAVING MAX(T1.COLLATERAL_NBR) = T1.COLLATERAL_NBR;
%runquit(&job,&sub31);

/*GET TOTAL OWED and TOTAL PAID*/
PROC SQL;
CREATE TABLE WORK.TOTALOWED AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.LOAN_ID AS DEALNBR,
		INPUT(T1.TRANSACTION_ID,30.) AS TRAN,
		T1.LOAN_TOTAL_OWED AS TOTALOWED,
		T1.FINANCIAL_RECORD_KEY_ID
	FROM OMNIFODS.FINANCIAL_RECORD_MASTER T1
		INNER JOIN WORK.QFX_BASE T2 ON (T1.SOURCE_SYSTEM_CD = T2.INSTANCE AND T1.LOAN_ID = T2.DEALNBR)
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.LOAN_ID
	HAVING MAX(INPUT(T1.TRANSACTION_ID,30.)) = TRAN;
%runquit(&job,&sub31);

/*GETS TRANSACTION SIGNS*/
PROC SQL;
CREATE TABLE WORK.SIGNS AS
	SELECT 
		TRANSACTION_CD,
		REVERSAL_CD,
		ASSIGNED_SIGN
		FROM OMNIREF.TRANSACTION_CODE_SIGNS;
QUIT;

PROC SQL;
CREATE TABLE WORK.FIN_KEYS AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.LOAN_ID AS DEALNBR, 
		T1.FINANCIAL_RECORD_KEY_ID,
		T1.TRANSACTION_CD,
		T1.REVERSAL_CD
	FROM OMNIFODS.FINANCIAL_RECORD_MASTER T1
	WHERE T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.LOAN_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	AND TRANSACTION_CD IN ('PAYMENT','RETURN');

CREATE TABLE WORK.TOTAL_PAID AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.DEALNBR,
		SUM(-(T2.FINANCIAL_DETAIL_AMT * T3.ASSIGNED_SIGN)) AS TOTAL_PAID
	FROM WORK.FIN_KEYS T1
		LEFT JOIN OMNIFODS.FINANCIAL_RECORD_DETAIL T2 ON (T1.FINANCIAL_RECORD_KEY_ID = T2.FINANCIAL_RECORD_KEY_ID AND T2.FINANCIAL_CD = 'ITEM')
		LEFT JOIN WORK.SIGNS T3 ON (T1.TRANSACTION_CD = T3.TRANSACTION_CD AND T1.REVERSAL_CD = T3.REVERSAL_CD)
	WHERE T2.FINANCIAL_CD = 'ITEM'
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.DEALNBR;
%runquit(&job,&sub31);

PROC SQL;
CREATE TABLE WORK.TOTALOWED_PAID AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.DEALNBR,
		T1.TOTALOWED,
		CASE
			WHEN T2.TOTAL_PAID = . THEN 0
			ELSE T2.TOTAL_PAID
		END AS TOTAL_PAID
	FROM WORK.TOTALOWED T1
		LEFT JOIN WORK.TOTAL_PAID T2 ON (T1.SOURCE_SYSTEM_CD = T2.SOURCE_SYSTEM_CD and T1.DEALNBR = T2.DEALNBR);
%runquit(&job,&sub31);

PROC SQL;
CREATE TABLE WORK.RETURNREASONCD AS
	SELECT
		T1.SOURCE_SYSTEM_CD,
		T1.SOURCE_PRODUCT_ID,
		T1.PRESENTMENT_NBR,
		T1.RETURN_REASON_CD
	FROM OMNICORE.PRODUCT_PRESENTMENT T1
	WHERE T1.SOURCE_SYSTEM_CD IN (&_SYSTEM)
	AND T1.SOURCE_PRODUCT_ID IN (SELECT DEALNBR FROM WORK.QFX_BASE)
	GROUP BY T1.SOURCE_SYSTEM_CD, T1.SOURCE_PRODUCT_ID
	HAVING MAX(T1.PRESENTMENT_NBR) = T1.PRESENTMENT_NBR;
%runquit(&job,&sub31);

PROC SQL;
CREATE TABLE WORK.QFX_DAILY_UPDATE AS
	SELECT
		CASE 
        	WHEN (substr(T1.PRODUCT,3,1)) = "T" then "TITLE"
         	WHEN T1.PRODUCT LIKE "CM%" THEN "INSTALLMENT"
			WHEN T1.PRODUCT LIKE "OM%" THEN "LINEOFCREDIT" 
          	ELSE "PAYDAY" 
        END AS PRODUCT length=50 format=$50.,
		CASE
			WHEN T1.POS = 'LOANGUARD' THEN 'ONLINE'
			WHEN T1.POS IN ('QFUND_1', 'QFUND_2', 'QFUND_3', 'QFUND_4','QFUND_5','QFUND_X') THEN 'QFUND'
			ELSE T1.POS
		END AS POS length=10 format=$10.,
		CASE
			WHEN T1.INSTANCE = 'EADVANTAGE' THEN 'EAPROD1'
			WHEN T1.INSTANCE = 'LOANGUARD' THEN 'AANET'
			WHEN T1.INSTANCE = 'QFUND_1' THEN 'QFUND1'
			WHEN T1.INSTANCE = 'QFUND_2' THEN 'QFUND2'
			WHEN T1.INSTANCE = 'QFUND_3' THEN 'QFUND3'
			WHEN T1.INSTANCE = 'QFUND_4' THEN 'QFUND4'
			WHEN T1.INSTANCE = 'QFUND_5' THEN 'QFUND5'
			WHEN T1.INSTANCE = 'QFUND_X' THEN 'FUSE'
			WHEN T1.INSTANCE = 'NEXTGEN' THEN 'NG'
			WHEN T1.INSTANCE = 'OMNI' THEN 'OMNI'
		END AS INSTANCE length=7 format=$7.,
		T2.CENTER_CD AS CHANNELCD,
		T2.BRANDCD,
		CASE
			WHEN T1.STATE = 'TX' THEN 'CSO'
			WHEN T1.STATE = 'OH' AND T1.INSTANCE NE 'EADVANTAGE' THEN 'CSO'
			ELSE 'STANDARD'
		END AS BANKMODEL LENGTH = 8,
		T2.COUNTRYCD length=10 format=$10.,
		T2.STATE length=20 format=$20.,
		T2.CITY length=50 format=$50.,
		T2.ZIP length=15 format=$15.,
		INPUT(T2.ZONENBR,16.) AS ZONENBR,
		T2.ZONENAME length=100 format=$100.,
		INPUT(T2.REGIONNBR,16.) AS REGIONNBR,
		T2.REGIONRDO length=100 format=$100.,
		INPUT(T2.DIVISIONNBR,16.) AS DIVISIONNBR,
		T2.DIVISIONDDO length=100 format=$100.,
		T2.BUSINESS_UNIT length=9 format=$9.,
		T1.CENTER_NBR AS LOCNBR FORMAT=16.,
		DHMS(T2.LOC_OPEN_DT,0,0,0) AS LOC_OPEN_DT FORMAT = DATETIME20.,
		DHMS(T2.LOC_CLOSE_DT,0,0,0) AS LOC_CLOSE_DT FORMAT = DATETIME20.,
		DHMS(T1.DEAL_DT,0,0,0) AS DEAL_DT FORMAT = DATETIME20.,
		T1.DEAL_DTTM FORMAT = DATETIME20.,
		(DHMS(INTNX('DAY',TODAY(),-1,'B'),00,00,00)) FORMAT=DATETIME20. AS LAST_REPORT_DT,
		T1.DEALNBR length=30 format=$30.,
		'0' AS TITLE_DEALNBR length=30 format=$30.,
		T1.CUSTNBR length=30 format=$30.,
		T1.SSN length=50 format=$50.,
		CASE
			WHEN T1.OMNINBR = '.' THEN ' '
			ELSE T1.OMNINBR
		END AS OMNINBR length=30 format=$30.,
		T1.ADVAMT,
		CASE
			WHEN T11.FEEAMT = . THEN 0
			ELSE T11.FEEAMT
		END AS FEEAMT,
		CASE 
			WHEN T12.CUSTOMARYFEE = . THEN 0
			ELSE T12.CUSTOMARYFEE
		END AS CUSTOMARYFEE,
		T1.NSFFEEAMT,
		T1.LATEFEEAMT,
		T1.OTHERFEEAMT,
		T1.WAIVEDFEEAMT,
		T3.REBATEFEE AS REBATEAMT,
		. AS COUPONAMT,
		T4.TOTAL_PAID AS TOTALPAID,
		T4.TOTALOWED,
		. AS CONSECUTIVEDEALFLG,
		. AS CASHAGNCNT,
		DHMS(T1.DUEDT,0,0,0) AS DUEDT FORMAT = DATETIME20.,
		DHMS(T1.DEALENDDT,0,0,0) AS DEALENDDT FORMAT = DATETIME20.,
		DHMS(T1.WRITEOFFDT,0,0,0) AS WRITEOFFDT FORMAT = DATETIME20.,
		DHMS(T5.DEPOSITDT,0,0,0) AS DEPOSITDT FORMAT = DATETIME20.,
		DHMS(T6.DEFAULTDT,0,0,0) AS DEFAULTDT FORMAT = DATETIME20.,
		CASE 
			WHEN T7.COLLATERAL_CD = 'CHECK' THEN T7.COLLATERAL_STATUS_CD
			ELSE ' '
		END AS CHECKSTATUSCD length=5 format=$5.,
		CASE
			WHEN T1.DEALSTATUSCD IN ('CURRENT','DELINQUENT','PENDINGCLOSED') THEN 'OPN'
			WHEN T1.DEALSTATUSCD IN ('DEFAULT','NONCURRENT') THEN 'DEF'
			WHEN T1.DEALSTATUSCD IN ('CLOSED','OVERPAID') THEN 'CLO'
			WHEN T1.DEALSTATUSCD IN ('WRITEOFF') THEN 'WO'
			WHEN T1.DEALSTATUSCD IN ('VOID') THEN 'V'
			ELSE 'UNK'
		END AS DEALSTATUSCD length=30 format=$30.,
		CASE
			WHEN T7.COLLATERAL_CD = 'ACH' THEN T7.COLLATERAL_STATUS_CD
			ELSE ' '
		END AS ACHSTATUSCD length=6 format=$6.,
		T10.RETURN_REASON_CD AS RETURNREASONCD length=5 format=$5.,
		T7.COLLATERAL_CD AS COLLATERAL_TYPE length=15 format=$15.,
		T7.CHECK_ID AS CUSTCHECKNBR length=15 format=$15.,
		T1.ETLDT,
		T8.PREVDEALNBR,
		T1.PRODUCT AS PRODUCTCD length=10 format=$10.,
		T9.INTERESTFEE,
		CASE
			WHEN T7.COLLATERAL_CD = 'ACH' THEN 'Y'
			ELSE 'N'
		END AS ACHAUTHFLG length=1 format=$1.,
		T1.UPDATEDT,
		. AS OUTSTANDING_DRAW_AMT,
		' ' AS UNDER_COLLATERALIZED length=1 format=$1.
	FROM WORK.QFX_BASE T1
		LEFT JOIN WORK.QFX_CENTER_INFO T2 ON (T1.CENTER_NBR = T2.CENTER_NBR)
		LEFT JOIN WORK.QFX_REBATEFEE T3 ON (T1.INSTANCE = T3.SOURCE_SYSTEM_CD AND T1.DEALNBR = T3.SOURCE_PRODUCT_ID)
		LEFT JOIN WORK.TOTALOWED_PAID T4 ON (T1.INSTANCE = T4.SOURCE_SYSTEM_CD AND T1.DEALNBR = T4.DEALNBR)
		LEFT JOIN WORK.QFX_DEPOSITDT2 T5 ON (T1.INSTANCE = T5.SOURCE_SYSTEM_CD AND T1.DEALNBR = COMPRESS(PUT(T5.SOURCE_PRODUCT_ID,30.)))
		LEFT JOIN WORK.QFX_DEFAULTDT2 T6 ON (T1.INSTANCE = T6.SOURCE_SYSTEM_CD AND T1.DEALNBR = COMPRESS(PUT(T6.SOURCE_PRODUCT_ID,30.)))
		LEFT JOIN WORK.COLLATERALTYPE T7 ON (T1.INSTANCE = T7.SOURCE_SYSTEM_CD AND T1.DEALNBR = T7.SOURCE_PRODUCT_ID)	
		LEFT JOIN WORK.PREVDEALNBR T8 ON (T1.INSTANCE = T8.SOURCE_SYSTEM_CD AND T1.DEALNBR = T8.DEALNBR)
		LEFT JOIN WORK.QFX_INTERESTFEE T9 ON (T1.INSTANCE = T9.SOURCE_SYSTEM_CD AND T1.DEALNBR = T9.SOURCE_PRODUCT_ID)
		LEFT JOIN WORK.RETURNREASONCD T10 ON (T1.INSTANCE = T10.SOURCE_SYSTEM_CD AND T1.DEALNBR = T10.SOURCE_PRODUCT_ID)
		LEFT JOIN WORK.QFX_ADVANCEFEE T11 ON (T1.INSTANCE = T11.SOURCE_SYSTEM_CD AND T1.DEALNBR = T11.SOURCE_PRODUCT_ID)
		LEFT JOIN WORK.QFX_CUSTOMARYFEE T12 ON (T1.INSTANCE = T12.SOURCE_SYSTEM_CD AND T1.DEALNBR = T12.SOURCE_PRODUCT_ID);
%runquit(&job,&sub31);

PROC SORT DATA=WORK.QFX_DAILY_UPDATE nodupkey;
	BY INSTANCE DEALNBR;
RUN;

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
RUN;

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\FUSE",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\FUSE\DIR2",'G');
%RUNQUIT(&job,&sub1);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_FUSE (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.QFX_DAILY_UPDATE;
%RUNQUIT(&job,&sub31);


/*UPLOAD FUSE*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_FUSE.SAS";