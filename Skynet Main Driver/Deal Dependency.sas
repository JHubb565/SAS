/*	THIS PROGRAM'S GOAL IS TO CHECK FOR 2 THINGS*/
/*	1) IS DEAL SUMMARY FINISHED*/
/*	2) IF DEAL SUMMARY IS FINISHED THEN KICK OFF ONLY DEAL SUMMARY DEPENDENCIES*/
OPTIONS NOXWAIT;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\TOP SECRET PROGRAM.SAS";


%MACRO DEAL_DEPENDENCIES();


	/*QUERY RESULTS FROM THE LOOP TO MAKE SURE EACH INSTANCE IS FINISHED*/

	%DO %UNTIL (%EVAL(&COUNT_FINISHED. >= 17));
			
				PROC SQL;
					CREATE TABLE TEMP AS
					SELECT DISTINCT SOURCE
								    ,EADV_STATUS
									,QFUND1_INSTALL_STATUS
									,QFUND1_TITLE_STATUS
									,QFUND2_INSTALL_STATUS
									,QFUND3_TTOC_STATUS
									,QFUND3_TXTITLE_STATUS
									,QFUND3_TETL_STATUS
									,QFUND3_FAI_STATUS
									,QFUND4_PAYDAY_STATUS
									,QFUND4_TITLE_STATUS
									,QFUND5_PAYDAY_STATUS
									,QFUND5_INSTALL_STATUS
									,QFUND5_TITLE_STATUS
									,NG_STATUS
									,ONLINE_STATUS
									,LOC_STATUS
									,FUSE_STATUS
					FROM BIOR.DATAMART_STATUS
					WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
				;
				QUIT;

			/*REVERSE THE TRANSPOSE*/
				DATA TEMP_STATUS;
					SET TEMP;
					ARRAY VARS EADV_STATUS QFUND1_INSTALL_STATUS QFUND1_TITLE_STATUS QFUND2_INSTALL_STATUS QFUND3_TTOC_STATUS QFUND3_TXTITLE_STATUS QFUND3_TETL_STATUS
							   QFUND3_FAI_STATUS QFUND4_PAYDAY_STATUS QFUND4_TITLE_STATUS QFUND5_PAYDAY_STATUS QFUND5_INSTALL_STATUS QFUND5_TITLE_STATUS NG_STATUS
								ONLINE_STATUS LOC_STATUS FUSE_STATUS; 	
					DO _T = 1 TO DIM(VARS);              	
					  IF NOT MISSING(VARS[_T]) THEN DO;  	
					    INSTANCE=VNAME(VARS[_T]);           
					    STATUS=VARS[_T];                   	
					    OUTPUT;                          	
					  END;
					END;
					DROP EADV_STATUS QFUND1_INSTALL_STATUS QFUND1_TITLE_STATUS QFUND2_INSTALL_STATUS QFUND3_TTOC_STATUS QFUND3_TXTITLE_STATUS QFUND3_FAI_STATUS QFUND3_TETL_STATUS
							   QFUND4_PAYDAY_STATUS QFUND4_TITLE_STATUS QFUND5_PAYDAY_STATUS QFUND5_INSTALL_STATUS QFUND5_TITLE_STATUS NG_STATUS
								ONLINE_STATUS LOC_STATUS FUSE_STATUS _T;  *DROP THE OLD VARS (COLS) AND THE DUMMY VARIABLE _T;
				RUN;

			/*LOOK AT TABLES WHICH HAVE FINISHED*/
				DATA COMPLETED_TABLES;
					SET TEMP_STATUS;
					WHERE STATUS = 'FINISHED';
				RUN;

				PROC SQL NOPRINT;
					SELECT COUNT(*) INTO: COUNT_FINISHED /*THIS NUMBER SHOULD BE EQUAL TO 16*/
					FROM COMPLETED_TABLES
					WHERE STATUS = 'FINISHED';
				QUIT;

				%IF %EVAL(&COUNT_FINISHED. < 17) %THEN 
					%DO;
						/*SLEEPS FOR 300 SECONDS (5 MINUTES) UNTIL IT FINDS 16 FINISHED TABLES, IT WILL LOOP FOREVER UNTIL THE 16 FINISHED TABLES*/
						DATA SLEEP;
							CALL SLEEP(300,1);
						RUN;

					%END;

	%END;

	/*	QUERY THAT CHECKS TO SEE IF DEAL SUMMARY HAS DEALS FOR PREVIOUS RUN */
	PROC SQL;
	    CREATE TABLE WORK.DEAL_SUMMARY_CHECK AS
	        SELECT
	             INSTANCE
	            ,PRODUCT
	            ,DATEPART(MAX(DEAL_DT)) AS DEAL_DT FORMAT MMDDYY10.
	            ,CASE WHEN 
	                  WEEKDAY(TODAY()) = 2 AND CALCULATED DEAL_DT >= TODAY()-2 THEN 'SUCCESSFUL'
	                  WHEN
	                  CALCULATED DEAL_DT >= TODAY()-1 THEN 'SUCCESSFUL' 
	                  ELSE 'ERROR' 
	             END AS STATUS
	        FROM BIOR.O_DEAL_SUMMARY_ALL
	    WHERE DEAL_DT >= DHMS(TODAY()-5,00,00,00)
	    GROUP BY INSTANCE
	             ,PRODUCT
	    ORDER BY INSTANCE
	             ,PRODUCT
		;
	QUIT;

	%LET TDY = %SYSFUNC(TODAY(),WEEKDATE30.);

	/* 	EMAIL BI_DATA AND SPENCER THE RESULTS OF THE QUERIES  */
	OPTIONS EMAILSYS=SMTP EMAILHOST=MAIL.ADVANCEAMERICA.NET EMAILPORT=25;
	FILENAME EML EMAIL TO=('BI_DATA@ADVANCEAMERICA.NET','BI_REPORTING@ADVANCEAMERICA.NET'
						 ,'FINANCEDEPT@ADVANCEAMERICA.NET','FINANCEPRODUCT@ADVANCEAMERICA.NET') CC=('SHOPKINS@ADVANCEAMERICA.NET') SUBJECT="DEAL SUMMARY DATAMART STATUS FOR &TDY" CT='TEXT/HTML';
	ODS LISTING CLOSE;
	ODS HTML BODY=EML;
	ODS SELECT SQL_RESULTS;
	ODS NOPROCTITLE;

	PROC SQL;
	TITLE "DEAL SUMMARY CHECK";
		SELECT *
			FROM WORK.DEAL_SUMMARY_CHECK
			;
	QUIT;
	/* CREATE TIMESTAMP */
	PROC FORMAT;
		PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
		PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
	RUN;

	DATA _NULL_;
		CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
		CALL SYMPUTX('MONDAY_OR_NOT',(WEEKDAY(DATE())),'G');
	RUN;

	/* KICK OFF RTDM_SUMMARY */
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 'E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\RTDM UW\O_RTDM_UW_DM\DAILY RUN\RTDM_UW_DM_1_DAILY.SAS'
					 -LOG 'E:SHARED\CADA\LOGS\O_RTDM_SUMMARY_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=O_RTDM_S
					 STATUS=O_RTDM_S;

	/* KICK OFF ONLINE INCENTIVE */
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 'E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\ONLINE INCENTIVE\ONLINE_INCENTIVE_PROGRAM.SAS'
					 -LOG 'E:SHARED\CADA\LOGS\ONLINE_INCENTIVE_PROGRAM_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=INCENTIVE
					 STATUS=INCENTIVE;

	/*-----------------------------------------------------------TDE PART------------------------------------------------------------*/


		/*GET DASHBOARDS THAT ONLY DEPEEND ON TRANSPOSE AND PASS THROUGH TO NEW VARIABLE*/

	%IF %EVAL(&MONDAY_OR_NOT. ^= 2) %THEN
		%DO;
			PROC SQL;
				CREATE TABLE DASHBOARD_LIST AS
				SELECT DISTINCT DASHBOARD_NAME
					  ,DEPENDENCIES
				FROM SKY.TDE_DEPENDENCY_DATAMART
				WHERE DEPENDENCIES = 'BIOR.O_DEAL_SUMMARY_ALL' AND TABLE_DEPENDENT_COUNT = 1
			;
			QUIT;

			%INCLUDE "&SKYNETREDESIGN.\TDE BAT UPLOAD.SAS";
		%END;

%MEND;

%DEAL_DEPENDENCIES