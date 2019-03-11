OPTIONS NOXWAIT;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\TOP SECRET PROGRAM.SAS";


%MACRO CUST_DEAL_DEPENDENCIES();

	/*QUERY RESULTS FROM THE LOOP TO MAKE SURE EACH INSTANCE IS FINISHED*/

	%DO %UNTIL (%EVAL(&COUNT_FINISHED. >= 34));
			
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
					WHERE SOURCE IN ('BIOR.O_CUSTOMER_ALL','BIOR.O_DEAL_SUMMARY_ALL')
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
					SELECT COUNT(*) INTO: COUNT_FINISHED /*THIS NUMBER SHOULD BE EQUAL TO 32*/
					FROM COMPLETED_TABLES
					WHERE STATUS = 'FINISHED';
				QUIT;

				%IF %EVAL(&COUNT_FINISHED. < 34) %THEN 
					%DO;
						/*SLEEPS FOR 300 SECONDS (5 MINUTES) UNTIL IT FINDS 32 FINISHED TABLES, IT WILL LOOP FOREVER UNTIL THE 32 FINISHED TABLES*/
						DATA SLEEP;
							CALL SLEEP(300,1);
						RUN;

					%END;

	%END;	 

	/* CREATE TIMESTAMP */
	PROC FORMAT;
		PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
		PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
	RUN;

	DATA _NULL_;
		CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
		CALL SYMPUTX('MONDAY_OR_NOT',(WEEKDAY(DATE())),'G');
	RUN;

	/* KICK OFF SPENCERS'S DOT DRIVER */
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 'E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\DOT818_SUPPRESSION\DOT818_DAILY_SUPPRESSION.SAS'
					 -LOG 'E:SHARED\CADA\LOGS\DOT818_DAILY_SUPPRESSION_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=DOT
					 STATUS=DOT;

	/*-----------------------------------------------------------TDE PART------------------------------------------------------------*/


		/*GET DASHBOARDS THAT ONLY DEPEEND ON DEAL AND TRAN AND PASS THROUGH TO NEW VARIABLE*/


	%IF %EVAL(&MONDAY_OR_NOT. ^= 2) %THEN
		%DO;
			PROC SQL;
				CREATE TABLE DASHBOARD_LIST AS
				SELECT DISTINCT DASHBOARD_NAME
					  ,DEPENDENCIES
				FROM SKY.TDE_DEPENDENCY_DATAMART
				WHERE TABLE_DEPENDENT_COUNT ^= 1 AND (DEPENDENCIES = 'BIOR.O_CUSTOMER_ALL' AND DEPENDENCIES = 'BIOR.O_DEAL_SUMMARY_ALL')
			;
			QUIT;

			%INCLUDE "&SKYNETREDESIGN.\TDE BAT UPLOAD.SAS";

			%LET TDY = %SYSFUNC(TODAY(),WEEKDATE30.);

			/* 	EMAIL BI_DATA AND SPENCER THE RESULTS OF THE QUERIES  */
			OPTIONS EMAILSYS=SMTP EMAILHOST=MAIL.ADVANCEAMERICA.NET EMAILPORT=25;
			FILENAME EML EMAIL TO=('BI_DATA@ADVANCEAMERICA.NET','BI_REPORTING@ADVANCEAMERICA.NET') CC=('SHOPKINS@ADVANCEAMERICA.NET') SUBJECT="CUSTOMER/DEAL DEPENDENT TDE'S KICKED OFF &TDY" CT='TEXT/HTML';
			ODS LISTING CLOSE;
			ODS HTML BODY=EML;
			ODS SELECT SQL_RESULTS;
			ODS NOPROCTITLE;

			PROC SQL;
			TITLE "CUST/DEAL DASHBOARD TDE'S KICKED OFF";
				SELECT A.*
					  ,'SUCCESSFUL'			AS STATUS
					FROM DASHBOARD_LIST A
					;
			QUIT;
		%END;

%MEND;

%CUST_DEAL_DEPENDENCIES