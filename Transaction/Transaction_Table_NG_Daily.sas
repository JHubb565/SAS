*ERROR CHECKING;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\NROCHESTER\TRANSACTION_DATAMART_DAILY\TRANSACTION_TABLE_ERROR_CHECK.SAS";

*LIBNAME STATEMENTS;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\NROCHESTER\LIBNAME_STATEMENTS.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";

*CUSTOMER JOIN PROGRAM;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\NROCHESTER\TRANSACTION_DATAMART_DAILY\CUSTOMER_SSN_MACRO.SAS";
%SSN(NG);

/* MACROS FOR SELECTING DATA */
%PUT &SYSUSERID;
%LET START=INTNX('DTDAY',DHMS(18628,0,0,0),1,'BEGINNING');
%LET THIRTYDAYS=INTNX('DTDAY',DHMS(%SYSFUNC(TODAY()),0,0,0),-5,'BEGINNING');
%PUT &THIRTYDAYS;
%PUT &START;

DATA _NULL_;
	CALL SYMPUTX('TRAN_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('TRAN_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\TRANSACTION",'G');
RUN;
/* START TIMER */
%LET _TIMER_START = %SYSFUNC(DATETIME());

/* DOWNLOAD LOAN TRANSACTION INFORMATION */
DATA WORK.MV_FINANCIAL_RECORD WORK.MV_FINANCIAL_RECORD_TT;
	SET DWQ1FIN.MV_FINANCIAL_RECORD;
WHERE FC_PRODUCT_CD^='LOC';
IF FC_PRODUCT_CD NOT IN ('MONEYGRAM','MISC','PPC','SAFE') AND FC_FINANCIAL_CD="ITEM" THEN OUTPUT WORK.MV_FINANCIAL_RECORD;
ELSE IF FC_PRODUCT_CD NOT IN ('MONEYGRAM','MISC','PPC','SAFE') AND FC_FINANCIAL_CD="TENDER" THEN OUTPUT WORK.MV_FINANCIAL_RECORD_TT;
%RUNQUIT(&job,&sub11);

/* JOIN BASE POS INFORMATION TOGETHER */
PROC SQL;
	CREATE TABLE TRANSACTION_TABLE_NG AS	
		SELECT 
			A.FI_CENTER_ID AS LOCNBR,
			A.FC_LOAN_ID AS DEALNBR LENGTH=15,
			A.FC_TRANSACTION_ID AS DEALTRANNBR LENGTH=15,
			A.FC_TRANSACTION_ID_OTHER AS ORIGTRANNBR LENGTH=15,
			A.FD_ORIGINATION_DATE AS DEAL_DT FORMAT=DATETIME20.,
			C.VOID_RESCIND_FLAG AS VOIDFLG,
			A.FC_TRANSACTION_CD AS POSTRANCD,
			A.FD_TRANSACTION_DTTM AS TRANDT,
			A.FC_FINANCIAL_DETAIL_CD AS POSAPPLIEDCD,
			A.FN_FINANCIAL_DETAIL_AMT AS TRANAMT,
			A.FD_TRANSACTION_DTTM AS TRANCREATEDT FORMAT=DATETIME20.,
			A.FC_PRODUCT_CD AS PRODUCTCD,
			A.FI_CUSTOMER_NUMBER,
/*			D.SSN AS SSN,*/
			A.FC_LOAN_STATUS_CD_CURRENT AS DEALSTATUSCD LENGTH=15,
			B.FC_FINANCIAL_DETAIL_CD AS MONETARYCD LENGTH=15
		FROM WORK.MV_FINANCIAL_RECORD A
			LEFT JOIN WORK.MV_FINANCIAL_RECORD_TT B ON (A.FC_LOAN_ID=B.FC_LOAN_ID AND A.FC_TRANSACTION_ID=B.FC_TRANSACTION_ID AND A.FI_CUSTOMER_NUMBER=B.FI_CUSTOMER_NUMBER /*AND A.FC_FINANCIAL_DETAIL_CD=B.FC_FINANCIAL_DETAIL_CD AND A.FC_TRANSACTION_CD=B.FC_TRANSACTION_CD*/)
			LEFT JOIN NG_DM.DM_FINANCIAL_RECORD C ON (A.FC_LOAN_ID=C.FC_LOAN_ID AND A.FC_TRANSACTION_ID=C.FC_TRANSACTION_ID AND A.FI_CUSTOMER_NUMBER=C.FI_CUSTOMER_NUMBER AND A.FC_FINANCIAL_DETAIL_CD=C.FC_FINANCIAL_DETAIL_CD AND A.FC_TRANSACTION_CD=C.FC_TRANSACTION_CD)
/*			LEFT JOIN (&SQL_TEXT.) D ON (D.CUSTNBR=A.FI_CUSTOMER_NUMBER)*/
		WHERE A.FD_TRANSACTION_DTTM > &THIRTYDAYS.;
%RUNQUIT(&job,&sub11);

/* CONVERT NUMERIC FIELDS TO CHARACTER */
DATA WORK.TRANSACTION_TABLE_NG (DROP=CUSTNBR1 FI_CUSTOMER_NUMBER);
	SET WORK.TRANSACTION_TABLE_NG;
CUSTNBR1=PUT(FI_CUSTOMER_NUMBER,15.);
CUSTNBR=STRIP(LEFT(CUSTNBR1));
%RUNQUIT(&job,&sub11);

/* FILTER VOIDED TRANSACTIONS INTO SEPARATE TABLE                        */
/* RENAME ORIGTRANNBR TO DEALTRANNBR FOR LATER JOINING BACK TO THE TABLE */
PROC SQL;
	CREATE TABLE VOIDS AS 
		SELECT 
			LOCNBR,
			DEALNBR,
			ORIGTRANNBR AS DEALTRANNBR,
			VOIDFLG,
			POSTRANCD,
			TRANDT AS VOIDDT,
			POSAPPLIEDCD,
			TRANAMT,
			TRANCREATEDT FORMAT=DATETIME20.,
			PRODUCTCD,
			CUSTNBR,
			DEALSTATUSCD,
			MONETARYCD
		FROM TRANSACTION_TABLE_NG
		WHERE VOIDFLG IN ('V','R');
%RUNQUIT(&job,&sub11);

/* CREATE 30 DAY PORTION OF POS INFORMATION */
PROC SQL;
	CREATE TABLE WORK.TRANSACTION_NG_UPLOAD AS
		SELECT 
			(CASE WHEN SUBSTR(T1.PRODUCTCD,3)   = 'T' THEN 'TITLE'
                  WHEN SUBSTR(T1.PRODUCTCD,1,2) = 'CS' AND SUBSTR(T1.PRODUCTCD,3) ~= 'T' THEN 'PAYDAY'
                  WHEN SUBSTR(T1.PRODUCTCD,1,2) = 'CM' AND SUBSTR(T1.PRODUCTCD,3) ~= 'T' THEN 'INSTALLMENT'
				  ELSE 'UNKNOWN'
                  END) AS PRODUCT LENGTH=20 FORMAT=$20.,
			(CASE WHEN T1.PRODUCTCD = 'CSC' THEN "Single Pay Check"
                  WHEN T1.PRODUCTCD = 'CSA' THEN "Single Pay ACH"
                  WHEN T1.PRODUCTCD = 'CSU' THEN "Single Pay Unsecured"
            	  WHEN T1.PRODUCTCD = 'CMC' THEN "Multi Pay Check"
              	  WHEN T1.PRODUCTCD = 'CMA' THEN "Multi Pay ACH"
             	  WHEN T1.PRODUCTCD = 'CMU' THEN "Multi Pay Unsecured"
             	  WHEN SUBSTR(T1.PRODUCTCD,3,1) = 'T' THEN "Title"
                  ELSE "UNKNOWN" END) 
				  AS PRODUCTDESC LENGTH=30 FORMAT=$30.,
			'NEXTGEN' AS POS length=20  format=$20.,
			'NG' AS INSTANCE length=20  format=$20.,
			'STOREFRONT'		AS CHANNELCD,
			T2.ST_PVC_CD AS STATE length=2 format=$2.,
			T1.LOCNBR,
			T11.SSN,
			T1.CUSTNBR,
			''		AS OMNINBR,
			DHMS(DATEPART(T1.DEAL_DT),0,0,0) AS DEAL_DT 		FORMAT=DATETIME20.,
			T1.DEAL_DT AS DEAL_DTTM								FORMAT=DATETIME20.,
			T1.DEALNBR,
			LEFT("0") AS TITLE_DEALNBR LENGTH=15 FORMAT=$15.,
			T1.DEALTRANNBR,
			T1.ORIGTRANNBR,
			T1.VOIDFLG LENGTH=2 FORMAT=$2.,
			T10.VOIDDT,
			T1.DEALSTATUSCD LENGTH=15,
			T1.POSTRANCD LENGTH=50 FORMAT=$50.,
   COALESCE(T8.STNDTRANCD,'UNKNOWN') AS STNDTRANCD LENGTH=25 FORMAT=$25.,
			T1.POSAPPLIEDCD LENGTH=50 FORMAT=$50.,
 (CASE WHEN T1.POSTRANCD='PAY' AND T1.POSAPPLIEDCD='IFC' THEN 'INTEREST FEE CHARGED' ELSE
   COALESCE(T7.STNDAPPLIEDCD,'UNKNOWN')END) AS STNDAPPLIEDCD LENGTH=20 FORMAT=$20.,
 (CASE WHEN T1.MONETARYCD CONTAINS 'ACH' AND T1.DEALSTATUSCD NOT IN ('WO','DEF','WOT') THEN 'Y'
 	   WHEN T1.MONETARYCD CONTAINS 'ACH' AND T1.DEALSTATUSCD IN ('WO','DEF','WOT') THEN 'N'
	   WHEN T1.POSTRANCD CONTAINS  'ACH' AND T1.DEALSTATUSCD NOT IN ('WO','DEF','WOT') THEN 'Y'
 	   WHEN T1.POSTRANCD CONTAINS  'ACH' AND T1.DEALSTATUSCD IN ('WO','DEF','WOT') THEN 'N'
	   WHEN T1.POSTRANCD CONTAINS  'ECC' AND T1.DEALSTATUSCD NOT IN ('WO','DEF','WOT') THEN 'Y'
	   WHEN T1.POSTRANCD CONTAINS  'ECC' AND T1.DEALSTATUSCD IN ('WO','DEF','WOT') THEN 'N'
	   WHEN T1.POSTRANCD IN ('ACHP','ACHPP') THEN 'Y'
	   WHEN T1.POSTRANCD IN ('ACHD') THEN 'N'
	   ELSE T8.CI_FLG END) AS CI_FLG,
			T1.MONETARYCD,
			T1.TRANAMT,
			T1.TRANDT,
			T1.TRANCREATEDT FORMAT=DATETIME20.,
			DHMS(DATEPART(T1.TRANCREATEDT),0,0,0) AS BUSINESSDT FORMAT=DATETIME20.,
			DHMS(DATEPART(T1.TRANDT),0,0,0) 	  AS TRANDATE   FORMAT=DATETIME20.,
			%SYSFUNC(DATETIME()) AS UPDATEDT FORMAT=DATETIME20.,
			"" AS NCP_IND LENGTH=1,
			. AS CREATEUSR
		FROM TRANSACTION_TABLE_NG T1
			LEFT JOIN EDW.D_LOCATION T2 ON T2.LOC_NBR=T1.LOCNBR
			LEFT JOIN BIOR.I_LOCATION_LATLONG T3 ON T3.LOCNBR=T1.LOCNBR
			LEFT JOIN BIOR.L_APPLIEDCODES T7 ON T7.POSAPPLIEDCD=T1.POSAPPLIEDCD
			LEFT JOIN BIOR.L_TRANSACTIONCODES T8 ON (T8.POS='NG' AND T8.POSTRANCD=T1.POSTRANCD)
			LEFT JOIN WORK.VOIDS T10 ON (T10.DEALNBR=T1.DEALNBR AND T10.DEALTRANNBR=T1.DEALTRANNBR AND T10.POSAPPLIEDCD=T1.POSAPPLIEDCD AND T10.POSTRANCD=T1.POSTRANCD)
			LEFT JOIN (&SQL_TEXT.) T11 ON (T11.CUSTNBR=T1.CUSTNBR)
		WHERE T1.TRANCREATEDT > &THIRTYDAYS.;
%RUNQUIT(&job,&sub11);

/* SORT POS INFORMATION */
/* STORE IN SAS DATA FOLDER */
PROC SORT DATA=WORK.TRANSACTION_NG_UPLOAD OUT=TRANSACTION_TABLE_NG_UPDATE NODUPKEY /*DUPOUT=DUPSDELETE*/;
BY DEALNBR DEALTRANNBR POSTRANCD POSAPPLIEDCD TITLE_DEALNBR INSTANCE TRANDATE;
%RUNQUIT(&job,&sub11);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
RUN;

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\TRANSACTION\NG",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\TRANSACTION\NG\DIR2\",'G');
RUN;

PROC SQL;
    INSERT INTO SKY.TRAN_DATAMART_NG (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM TRANSACTION_TABLE_NG_UPDATE;
QUIT;

/*UPLOAD NG*/
%INCLUDE "&TRAN_FILE_PATH.\TRANSACTION_UPLOAD_NG.SAS";


/* STOP TIMER */
DATA _NULL_;
  DUR = DATETIME() - &_TIMER_START;
  PUT 30*'-' / ' TOTAL DURATION:' DUR TIME13.2 / 30*'-';
RUN;

