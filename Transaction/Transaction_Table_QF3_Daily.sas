*Error Checking;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY\Transaction_Table_Error_Check.sas";

*Libname Statements;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";

*Wait For Macro;

*CUSTOMER JOIN PROGRAM;
%INCLUDE "E:\SHARED\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY\CUSTOMER_SSN_MACRO.SAS";
%SSN(QFUND3);

/* MACROS FOR SELECTING DATA */
%PUT &SYSUSERID;
%LET START=INTNX('DTDAY',DHMS(18628,0,0,0),1,'BEGINNING');
%LET THIRTYDAYS=INTNX('DTDAY',DHMS(%SYSFUNC(TODAY()),0,0,0),-5,'BEGINNING');
%PUT &START;
%PUT &THIRTYDAYS;

DATA _NULL_;
	CALL SYMPUTX('TRAN_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('TRAN_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\TRANSACTION",'G');
RUN;
/* START TIMER */
%LET _TIMER_START = %SYSFUNC(DATETIME());

/* DOWNLAOD LOAN TRANSACTION INFORMATION */
DATA WORK.LOAN_TRANSACTION_QF3;
	SET QFUND3.LOAN_TRANSACTION;
WHERE DATE_CREATED > &THIRTYDAYS.;
%RUNQUIT(&job,&sub6);

DATA WORK.LOAN_TRANSACTION_QF3_1;
SET WORK.LOAN_TRANSACTION_QF3;
IF TRAN_ID IN ('ADV','AGN','ROL') AND TRANSACTION_AMOUNT>0 AND PAY_PRINCIPAL=0 THEN PAY_PRINCIPAL=TRANSACTION_AMOUNT;
%RUNQUIT(&job,&sub6);

/* DOWNLOAD LOAN INFORMATION */
DATA WORK.LOAN_SUMMARY_QF3;
	SET QFUND3.LOAN_SUMMARY;
%RUNQUIT(&job,&sub6);

/* SORT LOAN TRANSACTION INFORMATION */
PROC SORT DATA=WORK.LOAN_TRANSACTION_QF3_1;
BY LOAN_ID LOAN_CODE LOAN_TRAN_CODE TRAN_ID ORIG_TRAN_CODE;
%RUNQUIT(&job,&sub6);

/* TRANSPOSE APPLIED CODE COLUMNS DOWN FROM LOAN TRANSACTION */
PROC TRANSPOSE DATA=WORK.LOAN_TRANSACTION_QF3_1 OUT=WORK.LOAN_TRANSPOSE_QF3;
VAR PAY_PRINCIPAL PAY_INTEREST CSO_FEE NSF_FEE LENDER_NSF_FEE LATE_FEE WO_PRINCIPAL WO_INTEREST WO_CSO_FEE WO_FEE 
WO_LENDER_NSF_FEE WO_LATE_FEE WAIVE_AMT REPRESENTMENT_AMT;
BY LOAN_ID LOAN_CODE LOAN_TRAN_CODE TRAN_ID ORIG_TRAN_CODE VOID_ID TRAN_DATE DATE_CREATED TENDER_TYPE LOAN_STATUS_ID;
%RUNQUIT(&job,&sub6);

/* ONLY TAKE TRANSACTIONS THAT ARE NOT ZERO FOR JOINING */
DATA WORK.LOAN_TRANSPOSE_NOZEROQF3 (RENAME=(COL1=TRANAMT _LABEL_=POSAPPLIEDCD));
SET WORK.LOAN_TRANSPOSE_QF3;
WHERE COL1 NOT IN (0,.);
%RUNQUIT(&job,&sub6);

/* JOIN BASE POS INFORMATION TOGETHER */
PROC SQL;
	CREATE TABLE TRANSACTION_TABLE_QF31 AS	
		SELECT 
			B.ST_CODE AS LOCNBR,
			A.LOAN_CODE AS DEALNBR1,
			A.LOAN_ID,
			A.LOAN_TRAN_CODE AS DEALTRANNBR1,
			A.ORIG_TRAN_CODE AS ORIGTRANNBR1,
			A.VOID_ID AS VOIDFLG,
			A.TRAN_ID AS POSTRANCD,
			A.TRAN_DATE AS TRANDT,
			A.POSAPPLIEDCD,
			A.TRANAMT,
			A.DATE_CREATED AS TRANCREATEDT FORMAT=DATETIME20.,
			B.LOAN_DATE AS DEAL_DT FORMAT=DATETIME20.,
			B.PRODUCT_TYPE AS PRODUCTCD,
			B.BO_CODE AS CUSTNBR1,
			C.SSN AS SSN,
			A.LOAN_STATUS_ID AS DEALSTATUSCD LENGTH=15,
			A.TENDER_TYPE AS MONETARYCD LENGTH=15
		FROM WORK.LOAN_TRANSPOSE_NOZEROQF3 A
			LEFT JOIN WORK.LOAN_SUMMARY_QF3 B ON (B.LOAN_ID=A.LOAN_ID)
			LEFT JOIN (&SQL_TEXT.) C ON (C.CUSTNBR=B.BO_CODE)
		WHERE A.DATE_CREATED > &THIRTYDAYS.;
%RUNQUIT(&job,&sub6);

/* CONVERT NUMERIC FIELDS TO CHARACTER */
DATA TRANSACTION_TABLE_QF3 (DROP=DEALNBR1 DEALTRANNBR1 ORIGTRANNBR1 CUSTNBR1);
	SET TRANSACTION_TABLE_QF31;
     IF PUT(CUSTNBR1,15.)~='.'            THEN CUSTNBR=PUT(STRIP(CUSTNBR1),15.);
ELSE IF PUT(STRIP(CUSTNBR1),15.)='.'      THEN CUSTNBR='';
     IF PUT(STRIP(DEALNBR1),15.)~='.'     THEN DEALNBR=PUT(STRIP(DEALNBR1),15.);
ELSE IF PUT(STRIP(DEALNBR1),15.)='.'      THEN DEALNBR='';
	 IF PUT(STRIP(DEALTRANNBR1),15.)~='.' THEN DEALTRANNBR=PUT(STRIP(DEALTRANNBR1),15.);
ELSE IF PUT(DEALTRANNBR1,15.)='.'         THEN DEALTRANNBR='';
	 IF PUT(STRIP(ORIGTRANNBR1),15.)~='.' THEN ORIGTRANNBR=PUT(STRIP(ORIGTRANNBR1),15.);
ELSE IF PUT(STRIP(ORIGTRANNBR1),15.)='.'  THEN ORIGTRANNBR='';
%RUNQUIT(&job,&sub6);

/* SORY LOAN TRANSACTION INFORMATION FOR DUPLICATES */
PROC SORT DATA=WORK.TRANSACTION_TABLE_QF3 NODUP DUPOUT=dup2;
BY DEALNBR DEALTRANNBR POSTRANCD ORIGTRANNBR TRANAMT;
%RUNQUIT(&job,&sub6);

/* FILTER VOIDED TRANSACTIONS INTO SEPARATE TABLE                        */
/* RENAME ORIGTRANNBR TO DEALTRANNBR FOR LATER JOINING BACK TO THE TABLE */
PROC SQL;
	CREATE TABLE VOIDS_QF3 AS 
		SELECT 
			LOCNBR,
			DEALNBR,
			LOAN_ID,
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
		FROM TRANSACTION_TABLE_QF3
		WHERE VOIDFLG IN ('V','R');
%RUNQUIT(&job,&sub6);

/* CREATE 30 DAY PORTION OF POS INFORMATION */
PROC SQL;
	CREATE TABLE TRANSACTION_TABLE_QF3_UPDATE AS
		SELECT DISTINCT
			(CASE WHEN T1.PRODUCTCD IN ("TLP","TTOC") THEN "TITLE" 
				  WHEN T1.PRODUCTCD IN ("ILP", "FAI") THEN "INSTALLMENT" 
/*				  WHEN T1.PRODUCTCD = "TTOC" THEN "TEXAS TITLE"*/
				  ELSE "UNKNOWN" END) AS 
            	  PRODUCT LENGTH=20 FORMAT=$20.,
			(CASE WHEN T1.PRODUCTCD = "TLP"  THEN "TX TITLE" 
                  WHEN T1.PRODUCTCD = "ILP"  THEN "TX TETL"
				  WHEN T1.PRODUCTCD = "TTOC" THEN "TX TTOC"
				  WHEN T1.PRODUCTCD = "FAI"  THEN "TX FAI"
                  ELSE "UNKNOWN" END)
				  AS PRODUCTDESC  LENGTH=30 FORMAT=$30.,
			'QFUND' AS POS LENGTH=20  FORMAT=$20.,
			'QFUND3' AS INSTANCE LENGTH=20  FORMAT=$20.,
			'STOREFRONT'		AS CHANNELCD,
			T2.ST_PVC_CD AS STATE LENGTH=2 FORMAT=$2.,
			T1.LOCNBR,
			T1.SSN,
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
			T1.TRANCREATEDT,
			DHMS(DATEPART(T1.TRANCREATEDT),0,0,0) AS BUSINESSDT FORMAT=DATETIME20.,
			DHMS(DATEPART(T1.TRANDT),0,0,0) 	  AS TRANDATE   FORMAT=DATETIME20.,
			%SYSFUNC(DATETIME()) AS UPDATEDT FORMAT=DATETIME20.,
			"" AS NCP_IND LENGTH=1,
			. AS CREATEUSR
		FROM TRANSACTION_TABLE_QF3 T1
			LEFT JOIN EDW.D_LOCATION T2 ON T2.LOC_NBR=T1.LOCNBR
/*			LEFT JOIN BIOR.I_LOCATION_LATLONG T3 ON T3.LOCNBR=T1.LOCNBR*/
			LEFT JOIN BIOR.L_APPLIEDCODES T7 ON T7.POSAPPLIEDCD=T1.POSAPPLIEDCD
			LEFT JOIN BIOR.L_TRANSACTIONCODES T8 ON (T8.POS='QFUND3' AND T8.POSTRANCD=T1.POSTRANCD)
			LEFT JOIN WORK.VOIDS_QF3 T10 ON (T10.LOAN_ID=T1.LOAN_ID AND T10.DEALTRANNBR=T1.DEALTRANNBR AND T10.POSAPPLIEDCD=T1.POSAPPLIEDCD AND T10.POSTRANCD=T1.POSTRANCD)
/*			LEFT JOIN BIOR.O_CUSTOMER_ALL T11 ON (T11.CUSTNBR=T1.CUSTNBR AND T11.INSTANCE='QFUND3')*/
		WHERE T1.TRANCREATEDT > &THIRTYDAYS.;
%RUNQUIT(&job,&sub6);

/* SORT POS INFORMATION */
/* STORE IN SAS DATA FOLDER */
PROC SORT DATA=WORK.TRANSACTION_TABLE_QF3_UPDATE OUT=TRANSACTION_TABLE_QF3_UPDATE NODUPKEY DUPOUT=DUPSDELETEQF3;
BY DEALNBR DEALTRANNBR POSTRANCD POSAPPLIEDCD TRANAMT TITLE_DEALNBR;
%RUNQUIT(&job,&sub6);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
RUN;

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\TRANSACTION\QFUND3",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\TRANSACTION\QFUND3\DIR2\",'G');
RUN;

PROC SQL;
    INSERT INTO SKY.TRAN_DATAMART_QF3 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM TRANSACTION_TABLE_QF3_UPDATE;
QUIT;


/* STOP TIMER */
DATA _NULL_;
  DUR = DATETIME() - &_TIMER_START;
  PUT 30*'-' / ' TOTAL DURATION:' DUR TIME13.2 / 30*'-';
RUN;


/*UPLOAD QF3*/
%INCLUDE "&TRAN_FILE_PATH.\TRANSACTION_UPLOAD_QF3.SAS";

