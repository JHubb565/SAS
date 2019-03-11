/*						%IF &DEAL_QFUND4_STATUS. = NOT_COMPLETE AND &DEAL_QFUND4_RUN_DATE. = . %THEN*/
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
											SET QFUND4_PAYDAY_STATUS = 'RUNNING'
											   ,QFUND4_PAYDAY_RUN_DATE = CURRENT_DATE
											   ,QFUND4_TITLE_STATUS = 'RUNNING'
											   ,QFUND4_TITLE_RUN_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF QFUND4_DEALSUMMARY*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_8_QF4_TLP.SAS'
												 -LOG '&DEAL_LOGPATH.\QF4_TLP_DS_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_TLP_DEAL
								STATUS=QF4_TLP_DEAL;

								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_9_QF4_PDL.SAS'
												 -LOG '&DEAL_LOGPATH.\QF4_PDL_DS_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_PDL_DEAL
								STATUS=QF4_PDL_DEAL;

								WAITFOR _ALL_ QF4_TLP_DEAL QF4_PDL_DEAL;

								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET QFUND4_PAYDAY_STATUS = 'FINISHED'
											   ,QFUND4_PAYDAY_COMPLETION_DATE = CURRENT_DATE
											   ,QFUND4_TITLE_STATUS = 'FINISHED'
											   ,QFUND4_TITLE_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;