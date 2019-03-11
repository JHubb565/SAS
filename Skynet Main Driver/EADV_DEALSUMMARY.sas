/*						%IF &DEAL_EADV_STATUS. = NOT_COMPLETE AND &DEAL_EADV_RUN_DATE. = . %THEN*/
/*							%DO; */
								OPTIONS SYMBOLGEN MPRINT MLOGIC NOXWAIT;
								%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V3\SKYNET REDESIGN\PATHS.SAS";



								/* CREATE TIMESTAMP */
								PROC FORMAT;
									PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
									PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
								RUN;

								DATA _NULL_;
									CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
								RUN;

								%PUT &TIMESTAMP;

								/*UPDATE TABLE WITH RUNNING FLAG AND CURRENT RUN DATE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET EADV_STATUS = 'RUNNING'
											   ,EADV_RUN_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF EADV_DEALSUMMSARY*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_1_EADV.SAS'
												 -LOG '&DEAL_LOGPATH.\EADV_DEAL_SUM_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=DEAL_EADV
								STATUS=DEAL_EADV;

								WAITFOR _ALL_ DEAL_EADV;

								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET EADV_STATUS = 'FINISHED'
											   ,EADV_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;