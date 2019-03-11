/****************************************************************************
Program		: Customer Lifecycle
Purpose		: Track customer transitions between statuses
Programmer  : Spencer Hopkins
Date		: 05/12/2016
****************************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
 05/12/16		Spencer Hopkins		Add audit program
 									Capture SSN detail
									Pull trans from 30 day load
 05/25/16		Spencer Hopkins		Add instance, product, product desc to
										final output(s)
									Change email to bi_data
 06/02/16		Spencer Hopkins		Change how transitions are counted
									Remove statuses (for time being)
 06/03/16		Spencer Hopkins		Convert to T-1
 06/17/16		Spencer Hopkins		Fixed T-1 on update_date
 06/21/16 		Spencer Hopkins		Excluding Online
 06/26/16		Spencer Hopkins		Include online 
									Calculate status counts
 06/30/16		Spencer Hopkins		Email to BI_DATA
 07/14/16		Spencer Hopkins		Fix merge into statement to update
										all fields (except SSN)
 09/19/16		Spencer Hopkins		Added fix to handle online
 11/03/16		Spencer Hopkins		Don't pull trans or deals without SSNs
									Eliminate daily, detail & CSBD backups
									Stop join on ODT & ODS	
 01/24/18	    Nathan Rochester	Added code to update Marketing customer
									status table
  
*****************************************************************************
*****************************************************************************
*/


/* AUDIT PRORGAM */
%include "E:\Shared\CADA\SAS Source Code\PRODUCTION\SAS Macro\CADA_AUDIT_PROG.sas";
%AUDITPROG(Skynet_CUST_AND_TRANS_Cust_Lifecycle_Daily,DAILY,BIOR.CUST_CATEGORY_DAILY_COUNT)


/*	INCLUDE LIBNAMES SCRIPT */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\TOP SECRET PROGRAM.SAS";


%LET DAYS_BACK = 5;


PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET EADV_STATUS = 'RUNNING'
			    ,EADV_RUN_DATE = CURRENT_DATE
				,QFUND1_INSTALL_STATUS = 'RUNNING'
				,QFUND1_INSTALL_RUN_DATE = CURRENT_DATE
				,QFUND1_TITLE_STATUS = 'RUNNING'
				,QFUND1_TITLE_RUN_DATE = CURRENT_DATE
				,QFUND2_INSTALL_STATUS = 'RUNNING'
				,QFUND2_INSTALL_RUN_DATE = CURRENT_DATE
				,QFUND3_TTOC_STATUS = 'RUNNING'
				,QFUND3_TTOC_RUN_DATE = CURRENT_DATE
				,QFUND3_TXTITLE_STATUS = 'RUNNING'
				,QFUND3_TXTITLE_RUN_DATE = CURRENT_DATE
				,QFUND3_TETL_STATUS = 'RUNNING'
				,QFUND3_TETL_RUN_DATE = CURRENT_DATE
				,QFUND3_FAI_STATUS = 'RUNNING'
				,QFUND3_FAI_RUN_DATE = CURRENT_DATE
				,QFUND4_PAYDAY_STATUS = 'RUNNING'
				,QFUND4_PAYDAY_RUN_DATE = CURRENT_DATE
				,QFUND4_TITLE_STATUS = 'RUNNING'
				,QFUND4_TITLE_RUN_DATE = CURRENT_DATE
				,QFUND5_PAYDAY_STATUS = 'RUNNING'
				,QFUND5_PAYDAY_RUN_DATE = CURRENT_DATE
				,QFUND5_INSTALL_STATUS = 'RUNNING'
				,QFUND5_INSTALL_RUN_DATE = CURRENT_DATE
				,QFUND5_TITLE_STATUS = 'RUNNING'
				,QFUND5_TITLE_RUN_DATE = CURRENT_DATE
				,NG_STATUS = 'RUNNING'
				,NG_RUN_DATE = CURRENT_DATE
				,ONLINE_STATUS = 'RUNNING'
				,ONLINE_RUN_DATE = CURRENT_DATE
				,LOC_STATUS = 'RUNNING'
				,LOC_RUN_DATE = CURRENT_DATE
				,FUSE_STATUS = 'RUNNING'
				,FUSE_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'CUSTOMER LIFECYCLE'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;

/* START TIMER */
%LET _TIMER_START = %SYSFUNC(DATETIME());

%MACRO ONLINE_FIX;
	
	%IF "&SYSDAY" = "Sunday" %THEN %DO;

		/*	RESET DAILY TO T-2 */
		PROC SQL;
			DELETE FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE BUSINESS_DATE = DHMS(TODAY()-2,0,0,0)
			;
		QUIT;

		/*	RESET BASE TO T-2 */
		PROC SQL;
			CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
				EXEC(TRUNCATE TABLE BIOR.WT_CUST_LAST_TRAN_ACTIVITY) BY ORACLE;
			DISCONNECT FROM ORACLE;
		QUIT;

		PROC APPEND BASE=BIOR.WT_CUST_LAST_TRAN_ACTIVITY DATA=CLA.TRANSITIONS_BASE_1_DAY_OLD;
		RUN;

		/*	RESET UPDATE TO T-2 */
		PROC SQL;
			CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
				EXEC(TRUNCATE TABLE BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR) BY ORACLE;
			DISCONNECT FROM ORACLE;
		QUIT;

		PROC APPEND BASE=BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR DATA=CLA.TRANSITIONS_UPDATE_1_DAY_OLD;
		RUN;

		/*	RESET DETAIL TO T-2 */
		PROC SQL;
			DELETE FROM BIOR.CUST_CATEGORY_DETAIL
			WHERE BUSINESS_DATE = DHMS(TODAY()-2,0,0,0)
			;
		QUIT;

		/*	RESET CUST STATUS BY DEAL TO T-2 */
		PROC SQL;
			DELETE FROM BIOR.CUST_STATUS_BY_DEAL
			WHERE DEAL_DT = DHMS(TODAY()-2,0,0,0)
			;
		QUIT;

		%CUSTLIFE(2)
		%CUSTLIFE(1)

	%END;

	%ELSE %DO;
		%CUSTLIFE(1)
	%END;
%MEND ONLINE_FIX;

%MACRO CUSTLIFE(i);
	%LET DATE_PULL = DHMS(TODAY()-&i,0,0,0);

	/*	COUNT # DAYS BETWEEN &NEW_DT & 18 MONTHS AGO */
	DATA _NULL_;
		CALL SYMPUTX('NEW_DT',DATEPART(&DATE_PULL));
	RUN;
	%LET NUM_DAYS_18 = %SYSFUNC(INTCK(DAY,%SYSFUNC(INTNX(MONTH,&NEW_DT,-18,SAME)),&NEW_DT));

	/*	PULL ALL DEALS FROM T-i */
	PROC SQL;
		CREATE TABLE WORK._DEALS_ AS
		SELECT *
		FROM BIOR.O_DEAL_SUMMARY_ALL
		WHERE DEAL_DT = &DATE_PULL
		;
	QUIT;

	/*	JOIN DEALS WITH BASE */
	PROC SQL;
		CREATE TABLE WORK.DEAL_W_LCITD AS
		SELECT DEAL.DEAL_DT 	
				,DEAL.SSN
				,DEAL.PRODUCT
				,DEAL.INSTANCE
				,DEAL.CHANNELCD
				,DEAL.LOCNBR
				,DEAL.DEALNBR
				,DEAL.TITLE_DEALNBR
				,BASE.TRANSACTION_DT AS LCITD
				,BASE.BALANCE_AMT AS BALANCE
				,INTCK('DAY',DATEPART(LCITD),&NEW_DT) AS DAYS_SINCE_LCITD FORMAT=5.
		FROM WORK._DEALS_ DEAL
			LEFT JOIN BIOR.WT_CUST_LAST_TRAN_ACTIVITY BASE
				ON DEAL.SSN = BASE.SSN
		WHERE DEAL.SSN IS NOT MISSING
		;
	QUIT;

	/*	DETERMINE STATUS AT TIME OF ORIGINATION */
	DATA WORK.DEAL_W_STATUS;
		SET WORK.DEAL_W_LCITD;
		FORMAT CUST_STATUS $15.;
		IF LCITD = . THEN CUST_STATUS = 'NOT_A_CUSTOMER';
		ELSE IF DAYS_SINCE_LCITD <= 60 THEN CUST_STATUS = 'ACTIVE';
		ELSE IF DAYS_SINCE_LCITD > 60 AND DAYS_SINCE_LCITD <= &NUM_DAYS_18 THEN CUST_STATUS = 'INACTIVE';
		ELSE IF DAYS_SINCE_LCITD > &NUM_DAYS_18 AND BALANCE = 0 THEN CUST_STATUS = 'DORMANT';
		ELSE IF DAYS_SINCE_LCITD > &NUM_DAYS_18 AND BALANCE NE 0 THEN CUST_STATUS = 'SUSPENDED';
		DROP LCITD BALANCE DAYS_SINCE_LCITD;
	RUN;

	/*	TRUNCATE BIOR.WT_CSBD */
	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
			EXEC(TRUNCATE TABLE BIOR.WT_CSBD) BY ORACLE;
		DISCONNECT FROM ORACLE;
	QUIT;

	/*	APPEND DATA TO BIOR.WT_CSBD */

/*	PROC SQL;*/
/*		CREATE TABLE BIOR.WT_CSBD_USE LIKE BIOR.WT_CSBD*/
/*	;*/
/*	QUIT;*/

	PROC SQL;
		CREATE TABLE CSBD_INSERT AS
		SELECT *
		FROM BIOR.WT_CSBD_USE
		UNION ALL CORR
		SELECT *
		FROM WORK.DEAL_W_STATUS
	;
	QUIT;

	PROC APPEND BASE=BIOR.WT_CSBD DATA=CSBD_INSERT FORCE;
	RUN;

	/*
	=========================================================================
		CUST STATUS BY DEAL
	=========================================================================
	*/

	/* MAKE 3 BACKUPS OF DEALS - T-1, T-2, T-3 */
/*	DATA CLA.CUST_STATUS_BY_DEAL_3_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET CLA.CUST_STATUS_BY_DEAL_2_DAY_OLD;*/
/*	RUN;*/
/**/
/*	DATA CLA.CUST_STATUS_BY_DEAL_2_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET CLA.CUST_STATUS_BY_DEAL_1_DAY_OLD;*/
/*	RUN;*/

/*	DATA CLA.CUST_STATUS_BY_DEAL_1_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET BIOR.CUST_STATUS_BY_DEAL;*/
/*	RUN;*/


	/*	APPEND RESULTS TO BIOR TABLE */
	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
		EXECUTE (MERGE INTO BIOR.CUST_STATUS_BY_DEAL BASE
				USING BIOR.WT_CSBD UPSERT
					ON (BASE.INSTANCE=UPSERT.INSTANCE
						AND BASE.DEALNBR=UPSERT.DEALNBR
						AND BASE.TITLE_DEALNBR=UPSERT.TITLE_DEALNBR)
					WHEN MATCHED THEN UPDATE
						SET
							BASE.DEAL_DT=UPSERT.DEAL_DT,
                			BASE.SSN=UPSERT.SSN,
                			BASE.PRODUCT=UPSERT.PRODUCT,
                			BASE.LOCNBR=UPSERT.LOCNBR,
                			BASE.CUST_STATUS=UPSERT.CUST_STATUS
					WHEN NOT MATCHED
						THEN INSERT
							(
							DEAL_DT,
                			SSN,
                			PRODUCT,
                			INSTANCE,
							CHANNELCD,
                			LOCNBR,
                			DEALNBR,
                			TITLE_DEALNBR,
                			CUST_STATUS
                			)
					VALUES
							(
							UPSERT.DEAL_DT,
							UPSERT.SSN,
							UPSERT.PRODUCT,
							UPSERT.INSTANCE,
							UPSERT.CHANNELCD,
							UPSERT.LOCNBR,
							UPSERT.DEALNBR,
							UPSERT.TITLE_DEALNBR,
							UPSERT.CUST_STATUS
							)
			)
		BY ORACLE;
		DISCONNECT FROM ORACLE;	
	QUIT;

	/*	MAKE BACKUP OF UPDATED/CURRENT */
/*	DATA CLA.CUST_STATUS_BY_DEAL_CURRENT (ALTER=justdont WRITE=justdont);*/
/*		SET BIOR.CUST_STATUS_BY_DEAL;*/
/*	RUN;*/


	/*	PULL ALL TRANSACTIONS FOR T-i DAYS TO EXCLUDE DEALS PRIOR TO 01/15/2011 & BLANK SOCIALS */
	PROC SQL;
		CREATE TABLE WORK.SSN_TRANS_DS AS 
		SELECT SSN
				,LOCNBR
				,CASE WHEN INSTANCE = 'QFUND5' THEN 'QFUND5-6'
						ELSE INSTANCE
					END AS INSTANCE
				,CHANNELCD
				,PRODUCT
				,CASE WHEN INSTANCE = 'FUSE' THEN ' '
						ELSE PRODUCTDESC 
					END AS PRODUCTDESC
				,DATEPART(DEAL_DT) AS DEAL_DT format date9.
				,DEALNBR format $15.
				,TRANDT
				,TRANAMT
				,CI_FLG
		FROM BIOR.O_DEALTRANSACTION_ALL
/*			INNER JOIN BIOR.O_DEAL_SUMMARY_ALL t2 ON (t1.INSTANCE = t2.INSTANCE AND t1.DEALNBR = t2.DEALNBR AND t1.TITLE_DEALNBR = t2.TITLE_DEALNBR)*/
		WHERE DEAL_DT > '15JAN2011:0:0:0'dt
				AND TRANDATE = &DATE_PULL
				AND SSN IS NOT MISSING
		;
	QUIT;


	/*	SUM TRAN AMOUNTS BY DATE/TIME & ORDER BY SSN THEN TRANDT */
	PROC SQL;
		CREATE TABLE WORK._UPDATE_ AS 
		SELECT SSN
				,LOCNBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
				,DEAL_DT	
				,DEALNBR
				,TRANDT
				,CASE WHEN (SUM(TRANAMT)) BETWEEN 0.009 AND -0.009 
						THEN 0
						ELSE (SUM(TRANAMT)) 
					END AS TRANAMT FORMAT=11.2
				,CI_FLG
		FROM WORK.SSN_TRANS_DS
		GROUP BY SSN
				,LOCNBR
				,INSTANCE
				,CHANNELCD
				,DEAL_DT
				,DEALNBR
				,TRANDT
				,CI_FLG
				,PRODUCT
				,PRODUCTDESC
		ORDER BY SSN
				,CI_FLG DESC
				,TRANDT
		;
	QUIT;


	/*	GRAB 1ST CIT (OR IF NO CIT, THEN 1ST ACTIVITY) */
	DATA WORK.UPDATE_CIT;
		SET WORK._UPDATE_;
		BY SSN DESCENDING CI_FLG TRANDT;
		IF FIRST.SSN;
	RUN;


	/*	SUM TRANAMT BY SSN TO GET DAILY TRANAMT BY CUSTOMER */
	PROC SQL;
		CREATE TABLE WORK.UPDATE_BAL AS
		SELECT SSN
			  ,CASE WHEN SUM(TRANAMT) BETWEEN 0.009 AND -0.009
			  		THEN 0
					ELSE SUM(TRANAMT)
				END AS DAILY_TRAN_AMT
		FROM WORK._UPDATE_
		GROUP BY SSN
		;
	QUIT;

	/*	BRING TOGETHER 1ST TRAN OF DAY AND BALANCE OF DAY FOR EACH SSN */
	PROC SQL;
		CREATE TABLE WORK.NEW_TRANS_SUM AS
		SELECT CIT.SSN
				,CIT.LOCNBR
				,CIT.INSTANCE
				,CIT.CHANNELCD
				,CIT.PRODUCT
				,CIT.PRODUCTDESC
				,CIT.DEAL_DT
				,CIT.DEALNBR
				,CIT.CI_FLG
				,DATEPART(CIT.TRANDT) AS TRAN_DT format DATE9.
				,CASE WHEN ROUND(BAL.DAILY_TRAN_AMT,0.01) = .
						THEN 0
						ELSE ROUND(BAL.DAILY_TRAN_AMT,0.01)
					END AS DAILY_TRAN_AMT
		FROM WORK.UPDATE_CIT CIT
			LEFT JOIN WORK.UPDATE_BAL BAL
				ON CIT.SSN = BAL.SSN
		;
	QUIT;


	/*	COMBINE BASE & NEW TRANS INFO DATES */
	PROC SQL;
		CREATE TABLE WORK.COMPARE_SETUP AS
		SELECT NEW.SSN
				,NEW.LOCNBR AS LOCATION_NBR
				,NEW.INSTANCE
				,NEW.CHANNELCD
				,NEW.PRODUCT
				,NEW.PRODUCTDESC
				,DHMS(NEW.DEAL_DT,0,0,0) AS DEAL_DT format DATETIME20.
				,NEW.DEALNBR AS DEAL_NBR
				,DHMS(NEW.TRAN_DT,0,0,0) AS NEW_TRANSACTION_DT format DATETIME20.
				,NEW.DAILY_TRAN_AMT AS TRANSACTION_AMT
				,NEW.CI_FLG
				,BASE.TRANSACTION_DT
				,CASE WHEN BASE.BALANCE_AMT = .
						THEN 0
						ELSE BASE.BALANCE_AMT
					END AS BALANCE_AMT
		FROM WORK.NEW_TRANS_SUM NEW
			LEFT JOIN BIOR.WT_CUST_LAST_TRAN_ACTIVITY BASE ON
				NEW.SSN = BASE.SSN
		;
	QUIT;


	/*	CALCULATE DAYS IN BETWEEN TRANSACTIONS & CALCULATE NEW BALANCES */
	PROC SQL;
		CREATE TABLE WORK._COMPARE_ AS
		SELECT t1.*
				,INTCK('dtday',t1.TRANSACTION_DT,t1.NEW_TRANSACTION_DT) AS INACTIVE_DAY_CNT
				,ROUND(t1.BALANCE_AMT + t1.TRANSACTION_AMT,0.01) AS NEW_BALANCE_AMT
		FROM WORK.COMPARE_SETUP t1
		;
	QUIT;

	/*	COUNT # DAYS BETWEEN &NEW_DT & 18 MONTHS AGO */
	DATA _NULL_;
		CALL SYMPUTX('NEW_DT',DATEPART(&DATE_PULL));
	RUN;
	%LET NUM_DAYS_18 = %SYSFUNC(INTCK(DAY,%SYSFUNC(INTNX(MONTH,&NEW_DT,-18,SAME)),&NEW_DT));

	/*	GRAB DISTINCT LOCNBRS */
	/*PROC SQL;*/
	/*	CREATE TABLE WORK.DIST_LOCNBRS AS*/
	/*		SELECT DISTINCT LOC_NBR AS LOCATION_NBR*/
	/*		FROM EDW.D_LOCATION*/
	/*		ORDER BY LOCATION_NBR*/
	/*	;*/
	/*QUIT;*/


	/* 	NEW_FIRST DETAILS */
	PROC SQL;
		CREATE TABLE WORK.NEW_FIRST_CUST_DET AS 
		SELECT	NEW_TRANSACTION_DT AS BUSINESS_DATE
				,LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
				,SSN
				,'NEW_FIRST_CUST' AS TRANSITION_TYPE format $20.
		FROM WORK._COMPARE_
		WHERE CI_FLG = 'Y' 
			AND TRANSACTION_DT = .
		;
	QUIT;

	/*	COUNT NEW_FIRST CUSTOMERS */
	PROC SQL;
		CREATE TABLE WORK.NEW_FIRST_CUST AS 
		SELECT DISTINCT LOCATION_NBR
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
		   				,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS NEW_FIRST_CUST
		FROM WORK.NEW_FIRST_CUST_DET
		GROUP BY LOCATION_NBR
				,INSTANCE
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;


	/*	NEW_REPEAT DETAILS */
	PROC SQL;
		CREATE TABLE WORK.NEW_REPEAT_CUST_DET AS 
		SELECT	NEW_TRANSACTION_DT AS BUSINESS_DATE
				,LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
				,SSN
				,'NEW_REPEAT_CUST' AS TRANSITION_TYPE format $20.
		FROM WORK._COMPARE_
	    WHERE CI_FLG = 'Y' 
			AND INACTIVE_DAY_CNT > &NUM_DAYS_18 
			AND BALANCE_AMT=0
		;
	QUIT;

	/*	COUNT NEW_REPEAT CUSTOMERS */
	PROC SQL;
		CREATE TABLE WORK.NEW_REPEAT_CUST AS 
		SELECT DISTINCT LOCATION_NBR
	 					,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
						,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS NEW_REPEAT_CUST
		FROM WORK.NEW_REPEAT_CUST_DET
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;


	/*	REACTIVATED DETAILS */
	PROC SQL;
		CREATE TABLE WORK.REACT_CUST_DET AS 
		SELECT	NEW_TRANSACTION_DT AS BUSINESS_DATE
				,LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
	            ,SSN
				,'REACTIVATED_CUST' AS TRANSITION_TYPE format $20.
		FROM WORK._COMPARE_
	    WHERE CI_FLG = 'Y' 
			AND INACTIVE_DAY_CNT BETWEEN 61 AND &NUM_DAYS_18
		;
	QUIT;

	/*	COUNT REACTIVATED CUSTOMERS */
	PROC SQL;
		CREATE TABLE WORK.REACT_CUST AS 
		SELECT DISTINCT LOCATION_NBR
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC 
	            		,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS REACT_CUST
		FROM WORK.REACT_CUST_DET
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;


	/*	REDEEMED DETAILS */
	PROC SQL;
		CREATE TABLE WORK.REDEEM_CUST_DET AS 
		SELECT	NEW_TRANSACTION_DT AS BUSINESS_DATE
				,LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
	            ,SSN
				,'REDEEMED_CUST' AS TRANSITION_TYPE format $20.
		FROM WORK._COMPARE_
	    WHERE CI_FLG = 'Y' 
			AND INACTIVE_DAY_CNT > &NUM_DAYS_18 
			AND BALANCE_AMT NE 0
		;
	QUIT;

	/*	COUNT REEDEMED CUSTOMERS */
	PROC SQL;
		CREATE TABLE WORK.REDEEM_CUST AS 
		SELECT DISTINCT LOCATION_NBR 
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
	            		,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS REDEEM_CUST
		FROM WORK.REDEEM_CUST_DET
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;


	/*	ROLLUP DETAILS */
	DATA WORK.ROLLUP_DETAILS;
		FORMAT TRANSITION_TYPE $20.;
		SET	WORK.NEW_FIRST_CUST_DET
			WORK.NEW_REPEAT_CUST_DET
			WORK.REACT_CUST_DET
			WORK.REDEEM_CUST_DET;
	RUN;

	/*
	=========================================================================
		DETAILS
	=========================================================================
	*/

	/* MAKE 3 BACKUPS OF DAILY - T-1, T-2, T-3 */
/*	DATA CLA.TRANSITIONS_DETAIL_3_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET CLA.TRANSITIONS_DETAIL_2_DAY_OLD;*/
/*	RUN;*/
/**/
/*	DATA CLA.TRANSITIONS_DETAIL_2_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET CLA.TRANSITIONS_DETAIL_1_DAY_OLD;*/
/*	RUN;*/

/*	DATA CLA.TRANSITIONS_DETAIL_1_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET BIOR.CUST_CATEGORY_DETAIL;*/
/*	RUN;*/


	/*	APPEND DETAILS TO RUNNING TABLE IN BIOR */

/*	PROC SQL;*/
/*		CREATE TABLE BIOR.CUST_CATEGORY_DETAIL_USE LIKE BIOR.CUST_CATEGORY_DETAIL*/
/*	;*/
/*	QUIT;*/

	PROC SQL;
		CREATE TABLE WORK.ROLLUP_DETAILS_INSERT AS
		SELECT *
		FROM BIOR.CUST_CATEGORY_DETAIL_USE
		UNION ALL CORR
		SELECT *
		FROM WORK.ROLLUP_DETAILS
	;
	QUIT;

	PROC APPEND BASE=BIOR.CUST_CATEGORY_DETAIL DATA=WORK.ROLLUP_DETAILS_INSERT FORCE;
	RUN;

	/*	MAKE BACKUP OF UPDATED/CURRENT */
/*	DATA CLA.TRANSITIONS_DETAIL_CURRENT (ALTER=justdont WRITE=justdont);*/
/*		SET BIOR.CUST_CATEGORY_DETAIL;*/
/*	RUN;*/



	/*
	=========================================================================
		UPDATE
	=========================================================================
	*/

	/* MAKE 3 BACKUPS OF UPDATE - T-1, T-2, T-3 */
	DATA CLA.TRANSITIONS_UPDATE_3_DAY_OLD (ALTER=justdont WRITE=justdont);
		SET CLA.TRANSITIONS_UPDATE_2_DAY_OLD;		
	RUN;

	DATA CLA.TRANSITIONS_UPDATE_2_DAY_OLD (ALTER=justdont WRITE=justdont);
		SET CLA.TRANSITIONS_UPDATE_1_DAY_OLD;
	RUN;

	DATA CLA.TRANSITIONS_UPDATE_1_DAY_OLD (ALTER=justdont WRITE=justdont);
		SET BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR;
	RUN;

	/*	TRUNC OLD NEW CUST UPDATE*/ 
	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
			EXEC(TRUNCATE TABLE BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR) BY ORACLE;
		DISCONNECT FROM ORACLE;
	QUIT;

/*	PROC SQL;*/
/*		CREATE TABLE BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR_U LIKE BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR;*/
/*	QUIT;*/

	PROC SQL;
		CREATE TABLE WORK._COMPARE_INSERT AS
		SELECT *
		FROM BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR_U
		UNION ALL CORR
		SELECT *
		FROM WORK._COMPARE_
	;
	QUIT;

	/*	PUSH FRESH NEW CUST UPDATE TO BIOR */
	PROC APPEND BASE=BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR DATA=WORK._COMPARE_INSERT FORCE;
	RUN;

	/*	MAKE BACKUP OF UPDATED/CURRENT */
	DATA CLA.TRANSITIONS_UPDATE_CURRENT (ALTER=justdont WRITE=justdont);
		SET BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR;
	RUN;


	/*
	=========================================================================
		BASE
	=========================================================================
	*/

	/* MAKE 3 BACKUPS OF BASE - T-1, T-2, T-3 */
	DATA CLA.TRANSITIONS_BASE_3_DAY_OLD (ALTER=justdont WRITE=justdont);
		SET CLA.TRANSITIONS_BASE_2_DAY_OLD;		
	RUN;

	DATA CLA.TRANSITIONS_BASE_2_DAY_OLD (ALTER=justdont WRITE=justdont);
		SET CLA.TRANSITIONS_BASE_1_DAY_OLD;		
	RUN;

	DATA CLA.TRANSITIONS_BASE_1_DAY_OLD (ALTER=justdont WRITE=justdont);
		SET BIOR.WT_CUST_LAST_TRAN_ACTIVITY;
	RUN;

	/*	UPDATE BASE TABLE WITH NEW LCITD */
	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
		EXECUTE (MERGE INTO BIOR.WT_CUST_LAST_TRAN_ACTIVITY BASE
					USING BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR UPSERT		
					ON (BASE.SSN = UPSERT.SSN)
					WHEN MATCHED THEN UPDATE SET BASE.TRANSACTION_DT = UPSERT.NEW_TRANSACTION_DT
												,BASE.LOCATION_NBR = UPSERT.LOCATION_NBR
												,BASE.INSTANCE = UPSERT.INSTANCE
												,BASE.CHANNELCD = UPSERT.CHANNELCD
												,BASE.PRODUCT = UPSERT.PRODUCT
												,BASE.PRODUCTDESC = UPSERT.PRODUCTDESC
												,BASE.DEAL_DT = UPSERT.DEAL_DT
												,BASE.DEAL_NBR = UPSERT.DEAL_NBR
						WHERE (UPSERT.CI_FLG = 'Y')
					WHEN NOT MATCHED THEN INSERT (BASE.SSN, BASE.LOCATION_NBR, BASE.INSTANCE, BASE.CHANNELCD, BASE.PRODUCT, BASE.PRODUCTDESC, BASE.DEAL_DT, BASE.DEAL_NBR, BASE.TRANSACTION_DT, BASE.BALANCE_AMT)
						VALUES (UPSERT.SSN, UPSERT.LOCATION_NBR, UPSERT.INSTANCE, UPSERT.CHANNELCD, UPSERT.PRODUCT, UPSERT.PRODUCTDESC, UPSERT.DEAL_DT, UPSERT.DEAL_NBR, UPSERT.NEW_TRANSACTION_DT, UPSERT.NEW_BALANCE_AMT))
		BY ORACLE;
		DISCONNECT FROM ORACLE;
	QUIT;


	/*	UPDATE BASE TABLE WITH NEW BALANCES */
	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
		EXECUTE (MERGE INTO BIOR.WT_CUST_LAST_TRAN_ACTIVITY BASE
					USING BIOR.WT_CUST_LAST_TRAN_ACTV_COMPR UPSERT		
					ON (BASE.SSN = UPSERT.SSN)
					WHEN MATCHED THEN UPDATE SET BASE.BALANCE_AMT = UPSERT.NEW_BALANCE_AMT)
		BY ORACLE;
		DISCONNECT FROM ORACLE;
	QUIT;



	/*	UPDATE AS OF DATE IN BASE */
	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
		EXECUTE (ALTER TABLE BIOR.WT_CUST_LAST_TRAN_ACTIVITY DROP COLUMN UPDATE_DT)
		BY ORACLE;
		DISCONNECT FROM ORACLE;	
	QUIT;

	PROC SQL;
		CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
		EXECUTE (ALTER TABLE BIOR.WT_CUST_LAST_TRAN_ACTIVITY ADD UPDATE_DT DATE DEFAULT TRUNC(SYSDATE)-1)
		BY ORACLE;
		DISCONNECT FROM ORACLE;	
	QUIT;


	/*	MAKE BACKUP OF UPDATED/CURRENT */
	DATA CLA.TRANSITIONS_BASE_CURRENT (ALTER=justdont WRITE=justdont);
		SET BIOR.WT_CUST_LAST_TRAN_ACTIVITY;
	RUN;


	/*	CREATE TABLE WITH SSN, LOCNBR, LCITD, BALANCE & DAYS SINCE LCITD,  */
DATA WORK.WT_STATUS_SETUP;
		SET BIOR.WT_CUST_LAST_TRAN_ACTIVITY (DROP=DEAL_DT DEAL_NBR UPDATE_DT);
		DAYS_LCITD = INTCK('DAY',DATEPART(TRANSACTION_DT),DATEPART(&DATE_PULL));
		IF LOCATION_NBR IN (2774
2115
2712
2208
2209
2211
2216
2218
2219
2220
2224
2225
2228
2756
382
385
5327
5328
370
371
374
375
376
5303
5335
5337
5378
377
380
381
388
390
395
397
4297
5309
5322
5323
5324
5333
5334
392
396
398
399
4114
4172
5301
5304
5318
5319
5320
5321
5325
5330
5339
379
383
386
389
4493
5308
5310
5311
5312
5313
5316
391
393
4619
5305
5306
5314
5317
5326
2227
394
2201
2202
2203
2204
2205
2206
2217
2221
2222
2223
2226
384
5387
373
4276
372
5338
4189
6874
2703) THEN INSTANCE = 'FUSE';
		IF INSTANCE = 'FUSE' THEN PRODUCTDESC = ' ';
	RUN;

	PROC SQL;
		CREATE TABLE WORK.WT_STATUS_ACTIVE AS
		SELECT DISTINCT LOCATION_NBR
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
						,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS ACTIVE_CUST
		FROM WORK.WT_STATUS_SETUP
	    WHERE DAYS_LCITD <= 60
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;

	PROC SQL;
		CREATE TABLE WORK.WT_STATUS_INACTIVE AS
		SELECT DISTINCT LOCATION_NBR
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
						,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS INACTIVE_CUST
		FROM WORK.WT_STATUS_SETUP
	    WHERE DAYS_LCITD BETWEEN 61 AND &NUM_DAYS_18
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		ORDER BY LOCATION_NBR
		;
	QUIT;

	PROC SQL;
		CREATE TABLE WORK.WT_STATUS_SUSPENDED AS
		SELECT DISTINCT LOCATION_NBR
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
						,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS SUSPENDED_CUST
		FROM WORK.WT_STATUS_SETUP
	    WHERE DAYS_LCITD > &NUM_DAYS_18 
			AND BALANCE_AMT NE 0
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;

	PROC SQL;
		CREATE TABLE WORK.WT_STATUS_NAC AS
		SELECT DISTINCT LOCATION_NBR
						,INSTANCE
						,CHANNELCD
						,PRODUCT
						,PRODUCTDESC
						,CASE WHEN (COUNT(SSN)) = .
							THEN 0
							ELSE (COUNT(SSN))
						  END AS NAC_CUST
		FROM WORK.WT_STATUS_SETUP
	    WHERE DAYS_LCITD > &NUM_DAYS_18 
			AND BALANCE_AMT = 0
		GROUP BY LOCATION_NBR
				,INSTANCE
				,CHANNELCD
				,PRODUCT
				,PRODUCTDESC
		;
	QUIT;





	/*	COMBINE CUST INFO */
	PROC SQL;
		CREATE TABLE WORK.NEW_CUST_ADD AS
		SELECT &DATE_PULL AS BUSINESS_DATE format DATETIME20.
				,LOC.LOCNBR AS LOCATION_NBR
				,LOC.INSTANCE
				,LOC.CHANNELCD
				,LOC.PRODUCT
				,LOC.PRODUCTDESC
				,CASE WHEN FIRST.NEW_FIRST_CUST = .
						THEN 0
						ELSE FIRST.NEW_FIRST_CUST
					END AS NEW_CUST_CNT
				,CASE WHEN REPEAT.NEW_REPEAT_CUST = .
						THEN 0
						ELSE REPEAT.NEW_REPEAT_CUST
					END AS NEW_REPEAT_CUST_CNT
				,CASE WHEN REDEEM.REDEEM_CUST = .
						THEN 0
						ELSE REDEEM.REDEEM_CUST
					END AS REDEEM_CUST_CNT
				,CASE WHEN REACT.REACT_CUST = .
						THEN 0
						ELSE REACT.REACT_CUST
					END AS REACTIVE_CUST_CNT
				,CASE WHEN ACTIVE.ACTIVE_CUST = .
						THEN 0
						ELSE ACTIVE.ACTIVE_CUST
					END AS ACTIVE_CUST_CNT
				,CASE WHEN INACTIVE.INACTIVE_CUST = .
						THEN 0
						ELSE INACTIVE.INACTIVE_CUST
					END AS INACTIVE_CUST_CNT
				,CASE WHEN SUSPEND.SUSPENDED_CUST = .
						THEN 0
						ELSE SUSPEND.SUSPENDED_CUST
					END AS SUSPENDED_CUST_CNT
				,CASE WHEN NAC.NAC_CUST = .
						THEN 0
						ELSE NAC.NAC_CUST
					END AS NAC_CUST_CNT
		FROM CLA.MASTER_LOC_TBL LOC
			LEFT JOIN WORK.NEW_FIRST_CUST FIRST 
				ON LOC.LOCNBR = FIRST.LOCATION_NBR
					AND LOC.INSTANCE = FIRST.INSTANCE
					AND LOC.PRODUCT = FIRST.PRODUCT
					AND LOC.PRODUCTDESC	= FIRST.PRODUCTDESC
			LEFT JOIN WORK.NEW_REPEAT_CUST REPEAT 
				ON LOC.LOCNBR = REPEAT.LOCATION_NBR
					AND LOC.INSTANCE = REPEAT.INSTANCE
					AND LOC.PRODUCT = REPEAT.PRODUCT
					AND LOC.PRODUCTDESC	= REPEAT.PRODUCTDESC
			LEFT JOIN WORK.REDEEM_CUST REDEEM 
				ON LOC.LOCNBR = REDEEM.LOCATION_NBR
					AND LOC.INSTANCE = REDEEM.INSTANCE
					AND LOC.PRODUCT = REDEEM.PRODUCT
					AND LOC.PRODUCTDESC	= REDEEM.PRODUCTDESC
			LEFT JOIN WORK.REACT_CUST REACT 
				ON LOC.LOCNBR = REACT.LOCATION_NBR
					AND LOC.INSTANCE = REACT.INSTANCE
					AND LOC.PRODUCT = REACT.PRODUCT
					AND LOC.PRODUCTDESC	= REACT.PRODUCTDESC
			LEFT JOIN WORK.WT_STATUS_ACTIVE ACTIVE 
				ON LOC.LOCNBR = ACTIVE.LOCATION_NBR
					AND LOC.INSTANCE = ACTIVE.INSTANCE
					AND LOC.PRODUCT = ACTIVE.PRODUCT
					AND LOC.PRODUCTDESC	= ACTIVE.PRODUCTDESC
			LEFT JOIN WORK.WT_STATUS_INACTIVE INACTIVE 
				ON LOC.LOCNBR = INACTIVE.LOCATION_NBR
					AND LOC.INSTANCE = INACTIVE.INSTANCE
					AND LOC.PRODUCT = INACTIVE.PRODUCT
					AND LOC.PRODUCTDESC	= INACTIVE.PRODUCTDESC
			LEFT JOIN WORK.WT_STATUS_SUSPENDED SUSPEND 
				ON LOC.LOCNBR = SUSPEND.LOCATION_NBR
					AND LOC.INSTANCE = SUSPEND.INSTANCE
					AND LOC.PRODUCT = SUSPEND.PRODUCT
					AND LOC.PRODUCTDESC	= SUSPEND.PRODUCTDESC
			LEFT JOIN WORK.WT_STATUS_NAC NAC 
				ON LOC.LOCNBR = NAC.LOCATION_NBR
					AND LOC.INSTANCE = NAC.INSTANCE
					AND LOC.PRODUCT = NAC.PRODUCT
					AND LOC.PRODUCTDESC	= NAC.PRODUCTDESC
		;		
	QUIT;


	/*	REMOVE LOCATIONS WITH NO TRANSITIONS */
	DATA WORK.NEW_CUST_ADD_CLEAN;
		SET WORK.NEW_CUST_ADD;
		IF (NEW_CUST_CNT + NEW_REPEAT_CUST_CNT + REDEEM_CUST_CNT + REACTIVE_CUST_CNT 
			+ ACTIVE_CUST_CNT + INACTIVE_CUST_CNT + SUSPENDED_CUST_CNT + NAC_CUST_CNT)=0 THEN DELETE;
	RUN;


	/*
	=========================================================================
		DAILY
	=========================================================================
	*/

	/* MAKE 3 BACKUPS OF DAILY - T-1, T-2, T-3  */
/*	DATA CLA.TRANSITIONS_DAILY_3_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET CLA.TRANSITIONS_DAILY_2_DAY_OLD;		*/
/*	RUN;*/
/**/
/*	DATA CLA.TRANSITIONS_DAILY_2_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET CLA.TRANSITIONS_DAILY_1_DAY_OLD;*/
/*	RUN;*/

/*	DATA CLA.TRANSITIONS_DAILY_1_DAY_OLD (ALTER=justdont WRITE=justdont);*/
/*		SET BIOR.CUST_CATEGORY_DAILY_COUNT;*/
/*	RUN;*/

	/*	APPEND NEW_CUST_TEMP TO NEW_CUST (IN BIOR) */
/*	PROC SQL;*/
/*		CREATE TABLE BIOR.CUST_CATEGORY_DAILY_COUNT_USE LIKE BIOR.CUST_CATEGORY_DAILY_COUNT*/
/*	;*/
/*	QUIT; */

	PROC SQL;
		CREATE TABLE NEW_CUST_ADD_CLEAN_INSERT AS
		SELECT *
		FROM BIOR.CUST_CATEGORY_DAILY_COUNT_USE
		UNION ALL CORR
		SELECT *
		FROM NEW_CUST_ADD_CLEAN
	;
	QUIT;

	PROC APPEND BASE=BIOR.CUST_CATEGORY_DAILY_COUNT DATA=NEW_CUST_ADD_CLEAN_INSERT FORCE;
	RUN;

	/*	MAKE BACKUP OF UPDATED/CURRENT */
/*	DATA CLA.TRANSITIONS_DAILY_CURRENT (ALTER=justdont WRITE=justdont);*/
/*		SET BIOR.CUST_CATEGORY_DAILY_COUNT;*/
/*	RUN;*/

%MEND CUSTLIFE;

/* RUN PROGRAM */
%ONLINE_FIX

PROC SQL;
	CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET EADV_STATUS = 'FINISHED'
			    ,EADV_COMPLETION_DATE = CURRENT_DATE
				,QFUND1_INSTALL_STATUS = 'FINISHED'
				,QFUND1_INSTALL_COMPLETION_DATE = CURRENT_DATE
				,QFUND1_TITLE_STATUS = 'FINISHED'
				,QFUND1_TITLE_COMPLETION_DATE = CURRENT_DATE
				,QFUND2_INSTALL_STATUS = 'FINISHED'
				,QFUND2_INSTALL_COMPLETION_DATE = CURRENT_DATE
				,QFUND3_TTOC_STATUS = 'FINISHED'
				,QFUND3_TTOC_COMPLETION_DATE = CURRENT_DATE
				,QFUND3_TXTITLE_STATUS = 'FINISHED'
				,QFUND3_TXTITLE_COMPLETION_DATE = CURRENT_DATE
				,QFUND3_TETL_STATUS = 'FINISHED'
				,QFUND3_TETL_COMPLETION_DATE = CURRENT_DATE
				,QFUND3_FAI_STATUS = 'FINISHED'
				,QFUND3_FAI_COMPLETION_DATE = CURRENT_DATE
				,QFUND4_PAYDAY_STATUS = 'FINISHED'
				,QFUND4_PAYDAY_COMPLETION_DATE = CURRENT_DATE
				,QFUND4_TITLE_STATUS = 'FINISHED'
				,QFUND4_TITLE_COMPLETION_DATE = CURRENT_DATE
				,QFUND5_PAYDAY_STATUS = 'FINISHED'
				,QFUND5_PAYDAY_COMPLETION_DATE = CURRENT_DATE
				,QFUND5_INSTALL_STATUS = 'FINISHED'
				,QFUND5_INSTALL_COMPLETION_DATE = CURRENT_DATE
				,QFUND5_TITLE_STATUS = 'FINISHED'
				,QFUND5_TITLE_COMPLETION_DATE = CURRENT_DATE
				,NG_STATUS = 'FINISHED'
				,NG_COMPLETION_DATE = CURRENT_DATE
				,ONLINE_STATUS = 'FINISHED'
				,ONLINE_COMPLETION_DATE = CURRENT_DATE
				,LOC_STATUS = 'FINISHED'
				,LOC_COMPLETION_DATE = CURRENT_DATE
				,FUSE_STATUS = 'FINISHED'
				,FUSE_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'CUSTOMER LIFECYCLE'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;



/* UPDATE DEAL SUMMARY FOR CUSTOMER STATUS BY DEAL */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET CUSTOMER DATAMART\CUST_STATUS_BY_DEAL_UPDATE.SAS";

/* UPDATE MKTG CUSTOMER STATUS TABLE IN BIOR */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET CUSTOMER DATAMART\CUSTOMER_STATUS_MARKETING.SAS";

/*KICK OFF UPDATED DAILY CUSTOMER #'S*/
/*SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'*/
/*				 '&SKYNETREDESIGN.\MERGE INTO CUSTOMER NUMBERS.SAS'*/
/*				 -LOG '&CUST_LOGPATH.\CUSTOMER_NUM_DRIV_&TIMESTAMP..LOG'*/
/*				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"*/
/*TASKNAME=CUST_UPDATE*/
/*STATUS=CUST_UPDATE;*/


/* EMAIL BI_DATA & SLACK WHEN PROCESSES ARE COMPLETE */
/*%INCLUDE "E:\SHARED\CADA\SAS DATA\USER\SCOCHRAN\EMAIL PASSWORD\EMAIL_AUTH.SAS";*/
%LET addlPPL_TO = ;
%LET addlPPL_CC = ;
%LET addlPPL_BCC = ;
%LET SKYJOB = Customer Lifecycle;

%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\INCLUDES\SKYNETEMAIL\SKYNETEMAIL.SAS";


/* STOP TIMER */
DATA _NULL_;
  DUR = DATETIME() - &_TIMER_START;
  PUT 30*'-' / ' TOTAL DURATION:' DUR TIME13.2 / 30*'-';
RUN;

/* AUDIT PROGRAM */
%AUDITPROG(Skynet_CUST_AND_TRANS_Cust_Lifecycle_Daily,DAILY,BIOR.CUST_CATEGORY_DAILY_COUNT)

/* STOP TIMER */
DATA _NULL_;
  DUR = DATETIME() - &_TIMER_START;
  PUT 30*'-' / ' TOTAL DURATION:' DUR TIME13.2 / 30*'-';
RUN;


	