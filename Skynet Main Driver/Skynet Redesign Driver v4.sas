/*-------------------------------------------------------------------------------------------

	TITLE: SKYNET RE-DESIGN
	AUTHOR: JUSTIN HUBBARD
	GOAL: TO CONDITIONALLY KICK OFF INSTANCES AND DATA UPLOADS IN ORDER TO HAVE A BETTER COMMUNICATION BETWEEN ETL, BI_DATA, AND BI_REPORTING TEAMS

	PROCEDURE:
		
		1) CHECK ETL SC_EDW_CONTROLS.TA_BATCH_PROCESS_MASTER TO SEE IF DATA IS AVAILABLE AND THE TIME WHEN IT IS COMPLETED
		2) BASED ON THE TIME COMPLETE, KICK OFF THE RESPECTIVE SOURCE DATA PULLS FOR SKYNET
		3) LOOP THROUGH 1) AND 2) UNTIL ALL DATA LOADS ARE KICKED OFF
		4) WHILE 1), 2), 3) ARE HAPPENING BIOR.DATAMART_STATUS IS BEING UPDATED WITH ALL ACTIVITY OF WHERE THE DATAMART IS 
		   IN ITS JOURNEY TO BEING UPDATED FOR THE DAY
		5) ONCE ALL DATA HAS WENT THROUGH STEPS 1), 2), 3), 4) THEN DEPENDENCY PROGRAMS START
				- DEPENDENCY PROGRAMS WILL KICK OFF PROGRAMS THAT DEPEND ON THE SOURCE SKYNET PULLS BASED ON WHAT IS COMPLETED
				- EACH DEPENDENCY PROGRAM RUNS SIMULTANEOUSLY
		6) DATA IS UPDATED "YAY!"

	PROGRAM ASSUMPTIONS:
		1) PROGRAM WILL LOOP FROM 3:30AM EASTERN TIME UNTIL ALL DATA HAS BEEN KICKED OFF
		2) BIOR.DATAMART_STATUS IS THE BRAINS OF THE ENTIRE PROGRAM. IF THIS TABLE IS DAMAGED/DROPPED OR LOST THEN PROGRAM WILL ERROR

---------------------------------------------------------------------------------------------------------------------------------------------------*/

/*ONLY INPUT NEEDED*/

/*ADD STORE PROCEDURE INPUTS HERE (NEED TO ADD)*/

/*END OF INPUTS*/
OPTIONS SYMBOLGEN MPRINT MLOGIC NOXWAIT NOQUOTELENMAX LRECL=32767;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\SKYNET_ERROR_INPUTS.SAS";

/*ETL CHECK MACRO LOOPS*/
	%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\ETL_CHECK_REDESIGN.SAS";
	%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\SKYNET ETL CHECK LOOP.SAS";

%LET HOUR = 21;
%GLOBAL DATE;
/*DATE*/
%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

SYSTASK KILL _ALL_; 

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub1);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
%RUNQUIT(&job,&sub1);

%PUT &TIMESTAMP;

/*TRUNCATE STAGING TABLES AND RESET DATAMART STATUSES*/
	%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\RESET STAGING AND DATAMART STATUSES.SAS";


/*START EMAIL MONITORING*/
	%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\EMAIL INSTANCE STATUS DRIVER.SAS";

/*START CUST LIFE BACKUP OF PREVIOUS DAY*/
	%INCLUDE "&CUST_FILE_PATH.\CUST LIFE BACKUP BEFORE RUN DRIVER.SAS";

%MACRO SKYNET_DATA_CHECK();

	/*----------CREATE/UPDATE SKYNET_INSTANCE_CHECK TABLE--------------------*/

		%IF %SYSFUNC(EXIST(BIOR.SKYNET_INSTANCE_CHECK)) %THEN
			%DO;

				/*BIOR ETL TABLE*/
				%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\BIOR ETL ASSIGNMENT.SAS";



		/*LOOK AT DATAMART STATUSES FOR CUSTOMER / DEAL / TRAN / DAILY*/

				/*CUSTOMER*/
				%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\CHECK DATAMART STATUS CUSTOMER.SAS";

				/*DEAL*/
				%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\CHECK DATAMART STATUS DEAL.SAS";

				/*DAILY*/
				%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\CHECK DATAMART STATUS DAILY.SAS";

				/*TRAN*/
				%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\CHECK DATAMART STATUS TRAN.SAS";

				/*TRANSPOSE*/
				%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\CHECK DATAMART STATUS TRANSPOSE.SAS";


/*------------------------------------------------END OF CHECKING STATUSES NEED TO WAIT FOR THESE NOW-------------------------------------*/

				/*KICK OFF 
				,CUSTOMER DATAMART EADV
				,DEAL DATAMART EADV
				,TRAN DATAMART EADV
				,DAILY DATAMART EADV*/
				%IF &EADV. = 1 %THEN 
					%DO;
						/*KICK OFF EADV DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\EADV DRIVER DECISION.SAS";
							%EADV_DRIVER_DECISION
					%END;

				/*KICK OFF (KIND OF TRICKY HERE, ONLY KICKING OFF QFUND1 STUFF THAT DOESNT RELY ON QF2)
				,CUSTOMER DATAMART QF1
				,DEAL DATAMART QF1
				,TRAN DATAMART QF1
				,DAILY DATAMART QF1*/
				%IF &QFUND1. = 1 AND &QFUND1_QFUND2. = 1 %THEN 
					%DO;
						/*KICK OFF QFUND1 DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\QFUND1 DRIVER DECISION.SAS";
							%QFUND1_DRIVER_DECISION	
					%END;


				/*KICK OFF 
				,CUSTOMER DATAMART QF1/QF2
				,DEAL DATAMART QF1/QF2
				,TRAN DATAMART QF1/QF2
				,DAILY DATAMART QF1/QF2*/
				%IF &QFUND1. = 1 AND &QFUND2. = 1 AND &QFUND1_QFUND2. = 1 %THEN 
					%DO;
						/*KICK OFF QFUND2 DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\QFUND1_QFUND2 DRIVER DECISION.SAS";
							%QF1_QF2_DRIVER_DECISION
					%END;

				/*KICK OFF 
				,CUSTOMER DATAMART QF3
				,DEAL DATAMART QF3
				,TRAN DATAMART QF3
				,DAILY DATAMART QF3*/
				%IF &QFUND3. = 1 %THEN 
					%DO;
						/*KICK OFF QFUND3 DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\QFUND3 DRIVER DECISION.SAS";
							%QFUND3_DRIVER_DECISION						
					%END;

	
				/*KICK OFF 
				,CUSTOMER DATAMART QF4
				,DEAL DATAMART QF4
				,TRAN DATAMART QF4
				,DAILY DATAMART QF4*/
				%IF &QFUND4. = 1 %THEN 
					%DO;
						/*KICK OFF QFUND4 DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\QFUND4 DRIVER DECISION.SAS";
							%QFUND4_DRIVER_DECISION												
					%END;
	

				/*KICK OFF 
				,CUSTOMER DATAMART QF5
				,DEAL DATAMART QF5
				,TRAN DATAMART QF5
				,DAILY DATAMART QF5*/
				%IF &QFUND5. = 1 %THEN 
					%DO;
						/*KICK OFF QFUND5 DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\QFUND5 DRIVER DECISION.SAS";
							%QFUND5_DRIVER_DECISION												
					%END;

				/*KICK OFF 
				,CUSTOMER DATAMART NG
				,DEAL DATAMART NG
				,TRAN DATAMART NG
				,DAILY DATAMART NG*/
				%IF &NG. = 1 %THEN 
					%DO;
						/*KICK OFF NG DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\NG DRIVER DECISION.SAS";
							%NG_DRIVER_DECISION												
					%END;


				/*KICK OFF 
				,CUSTOMER DATAMART ONLINE/LOC
				,DEAL DATAMART ONLINE/LOC
				,TRAN DATAMART ONLINE/LOC
				,DAILY DATAMART ONLINE/LOC*/
				%IF &OL. = 1 %THEN 
					%DO;
						/*KICK OFF ONLINE DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\ONLINE DRIVER DECISION.SAS";
							%ONLINE_DRIVER_DECISION												
					%END;


				/*KICK OFF 
				,CUSTOMER DATAMART FUSE
				,DEAL DATAMART FUSE
				,TRAN DATAMART FUSE
				,DAILY DATAMART FUSE*/
				%IF &FUSE. = 1 %THEN 
					%DO;
						/*KICK OFF FUSE DRIVER DECISION*/
						%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\FUSE DRIVER DECISION.SAS";
							%FUSE_DRIVER_DECISION

					%END;

				DATA SLEEP;
					CALL SLEEP(300,1);
				RUN;


/*---------------------------------------------- END OF INSTANCES ----------------------------------------------------------*/


			%END;

%MEND;

%GLOBAL LOADED_COUNT;
%GLOBAL INSTANCE_COUNT;

%MACRO COUNTS(); 

	PROC SUMMARY DATA=ASSIGN_VALUES;
	VAR LOADED;
	OUTPUT OUT=COUNTS SUM=;
	RUN;

	PROC SQL NOPRINT;
	SELECT LOADED INTO:LOADED_COUNT
	FROM COUNTS;
	QUIT;

	PROC SQL NOPRINT;
	SELECT _FREQ_ INTO:INSTANCE_COUNT
	FROM COUNTS;
	QUIT;

	%PUT &LOADED_COUNT;
	%PUT &INSTANCE_COUNT;

%MEND;

%MACRO UPDATE_ETL_TABLE();

	/*CHECK REFRESH SKYNET ETL_CHECK_TABLE*/
	%SKYNET_ETL_CHECK
	
	/*ASSIGN VALUES TO INSTANCE VARIABLES*/
	%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\ASSIGN INSTANCE VALUES.SAS";

%MEND;


/*LOOP TO RUN*/

%MACRO SKYNET_DATA_LOOP();
		
	%IF %EVAL(&LOADED_COUNT. NE &INSTANCE_COUNT.) %THEN 

	%DO;

		%DO %UNTIL(%EVAL(&LOADED_COUNT. = &INSTANCE_COUNT.));
		
			%UPDATE_ETL_TABLE

			%COUNTS

			%SKYNET_DATA_CHECK

		%END;

	%END;

	%IF %EVAL(&LOADED_COUNT. = &INSTANCE_COUNT.) %THEN 

	%DO;
		
		%UPDATE_ETL_TABLE

		%COUNTS

		%SKYNET_DATA_CHECK

	%END;

%MEND;

%UPDATE_ETL_TABLE
%COUNTS
%SKYNET_DATA_LOOP


/*--------------------------------------------------------- END OF SKYNET DATACHECK LOOP STUFF -------------------------------------------*/ 



/*GET TIME*/
	DATA _NULL_;
		CALL SYMPUTX('CURRENT_TIME',PUT(DATETIME(),DATEAMPM20.));
	RUN;

	%PUT &CURRENT_TIME;

/*EMAIL TEAM*/
	OPTIONS EMAILSYS=SMTP EMAILHOST=MAIL.ADVANCEAMERICA.NET EMAILPORT=25;
	FILENAME MYMAIL EMAIL; 
	DATA _NULL_;
	   FILE MYMAIL
			TO=('BI_DATA@ADVANCEAMERICA.NET','BI_REPORTING@ADVANCEAMERICA.NET','SHOPKINS@ADVANCEAMERICA.NET')
		    FROM=('SKYNET@COMPLETE.NET')
	     SUBJECT="SKYNET DATA LOAD LOOP COMPLETE - STARTING DEPENDENCIES NOW - &CURRENT_TIME ";
		 PUT ' SKYNET DATA LOOP HAS BEEN COMPLETED AND ALL DATA HAS BEEN FOUND. THE NEXT STEP IN SKYNET IS TO WAIT FOR DATA TO LOAD AND FOR REPORTS/TABLES BUILT OFF SKYNET TO BE RAN . ';
	RUN;
	


/*---------------------------------------------END OF CONDITIONAL SOURCE PULL LOGIC-----------------------------------------------------

		NOW THE PROGRAM WILL VISIT PROGRAMS THAT HAVE DEPENDENCIES ON THE PROGRAMS ABOVE FINISHING

		THESE PROGRAMS WILL ONLY BE KICKED OFF IF EVERY INSTANCE/PRODUCT COMBINATION OF DATA HAS BEEN COMPLETED

----------------------------------------------------------------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------------------------------------------------------------

		ONCE WE GET TO THIS POINT ALL THE DATA HAS: 

			1) DATA FROM ETL HAS BEEN COMPLETED
			2) ALL SKYNET SOURCE PULLS HAVE BEEN KICKED OFF / ARE FINISHING
		
		KNOWING BOTH OF THOSE THINGS, WE NOW NEED TO CONDITIONALLY KICK OFF PROCESSES THAT ARE DEPENDENT ON THE ABOVE SOURCE PULLS. 
		4 DEPENDENCY PROGRAMS WILL KICK OFF SIMULTANEOUSLY NEXT.

			1) DEAL DEPENDENCY
			2) TRAN DEPENDENCY
			3) DEAL AND TRAN DEPENDENCY
			4) DAILY DEPENDENCY
			5) TRANSPOSE DEPENDENCY
			6) CUSTOMER LIFECYCLE DEPENDENCY


---------------------------------------------------------------------------------------------------------------------------------------*/
 
	
	/* KICK OFF DEAL DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\DEAL DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\DEAL_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=DEAL_DEPEND
					 STATUS=DEAL_DEPEND;

	/* KICK OFF TRAN DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\TRAN DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\TRAN_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=TRAN_DEPEND
					 STATUS=TRAN_DEPEND;

	/* KICK OFF DEAL AND TRAN DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\DEAL AND TRAN DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\DEAL_TRAN_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=DT_DEPEND
					 STATUS=DT_DEPEND;

	/* KICK OFF CUSTOMER/DEAL DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\CUSTOMER AND DEAL DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\CUST_DEAL_DEPEND_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=CUST_DEAL
					 STATUS=CUST_DEAL;

	/* KICK OFF DAILY DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\DAILY DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\DAILY_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=DAILY_DEPEND
					 STATUS=DAILY_DEPEND;

	/* KICK OFF TRANSPOSE DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\TRANSPOSE DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\TRANSPOSE_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=TRANSP_DEPEND
					 STATUS=TRANSP_DEPEND;

	/* KICK OFF CUSTOMER DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\CUSTOMER DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\CUSTOMER_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=CUST_DEPEND
					 STATUS=CUST_DEPEND;

	/* KICK OFF CUSTOMER LIFECYCLE DEPENDENCY*/
	SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
					 '&SKYNETREDESIGN.\CUSTOMER LIFECYCLE DEPENDENCY.SAS'
					 -LOG '&SKYNETREDESIGN_LOGS.\CUSTOMER_LC_DEPENDENCY_DRIVER_&TIMESTAMP..LOG'
					 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
					 TASKNAME=CUST_LC_DEPEND
					 STATUS=CUST_LC_DEPEND;


WAITFOR _ALL_ DEAL_DEPEND TRAN_DEPEND DT_DEPEND DAILY_DEPEND CUST_DEPEND CUST_LC_DEPEND TRANSP_DEPEND CUST_DEAL;



	/*GET TIME*/
	DATA _NULL_;
		CALL SYMPUTX('CURRENT_TIME',PUT(DATETIME(),DATEAMPM20.),'G');
		CALL SYMPUTX('MONDAY_OR_NOT',(WEEKDAY(DATE())),'G');
	RUN;

	%PUT &CURRENT_TIME;
	%PUT &MONDAY_OR_NOT;

/*EMAIL TEAM*/
	OPTIONS EMAILSYS=SMTP EMAILHOST=MAIL.ADVANCEAMERICA.NET EMAILPORT=25;
	FILENAME MYMAIL EMAIL; 
	DATA _NULL_;
	   FILE MYMAIL
			TO=('BI_DATA@ADVANCEAMERICA.NET','SHOPKINS@ADVANCEAMERICA.NET','BI_REPORTING@ADVANCEAMERICA.NET')
		    FROM=('SKYNET@COMPLETE.NET')
	     SUBJECT="SKYNET DEPENDENCY PROGRAMS COMPLETED - SKYNET DEPENDENT TDE'S KICKED OFF - &CURRENT_TIME ";
	RUN;


/*KICK OFF THE REST OF THE TDES*/

/*UPLOAD TDE SCRIPT*/

%MACRO MONDAY_OR_NOT();

	%IF %EVAL(&MONDAY_OR_NOT. ^= 2) %THEN
		%DO;
			DATA _NULL_;
			     UPLOADFILE = '"E:\shared\cada\sas source code\Production\Tableau_Refresh\TDE Bat Files\ALL-Skynet.bat" /console /command';
			    CALL SYSTEM(UPLOADFILE);
			 RUN;
		%END;
%MEND;

%MONDAY_OR_NOT

/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%abort cancel;
	%END;

%MEND;

%STOPPROGRAM












