/*						%IF &TRAN_QFUND3_STATUS. = NOT_COMPLETE AND &TRAN_QFUND3_RUN_DATE. = . %THEN*/
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
											  SET 
											   QFUND3_TXTITLE_STATUS = 'RUNNING'
											   ,QFUND3_TXTITLE_RUN_DATE = CURRENT_DATE
											   ,QFUND3_TETL_STATUS = 'RUNNING'
											   ,QFUND3_TETL_RUN_DATE = CURRENT_DATE
											   ,QFUND3_TTOC_STATUS = 'RUNNING'
											   ,QFUND3_TTOC_RUN_DATE = CURRENT_DATE
											   ,QFUND3_FAI_STATUS = 'RUNNING'
											   ,QFUND3_FAI_RUN_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF QFUND3_DEALSUMMARY*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_4_QF3_TXTITLE.SAS'
												 -LOG '&DEAL_LOGPATH.\QF3_TXTITLE_DS_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_TXTITLE_DEAL
								STATUS=QF3_TXTITLE_DEAL;

								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_5_QF3_TETL.SAS'
												 -LOG '&DEAL_LOGPATH.\QF3_TETL_DS_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_TETL_DEAL
								STATUS=QF3_TETL_DEAL;

								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_6_QF3_TTOC.SAS'
												 -LOG '&DEAL_LOGPATH.\QF3_TTOC_DS_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_TTOC_DEAL
								STATUS=QF3_TTOC_DEAL;

								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&DEAL_FILE_PATH.\SKYNET_STEP_7_QF3_FAI.SAS'
												 -LOG '&DEAL_LOGPATH.\QF3_FAI_DS_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_FAI_DEAL
								STATUS=QF3_FAI_DEAL;

								WAITFOR _ALL_ QF3_TXTITLE_DEAL QF3_TETL_DEAL QF3_TTOC_DEAL QF3_FAI_DEAL;

								/*UPDATE TABLE WITH RUNNING FLAG AND CURRENT RUN DATE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											  SET 
											   QFUND3_TXTITLE_STATUS = 'FINISHED'
											   ,QFUND3_TXTITLE_COMPLETION_DATE = CURRENT_DATE
											   ,QFUND3_TETL_STATUS = 'FINISHED'
											   ,QFUND3_TETL_COMPLETION_DATE = CURRENT_DATE
											   ,QFUND3_TTOC_STATUS = 'FINISHED'
											   ,QFUND3_TTOC_COMPLETION_DATE = CURRENT_DATE
											   ,QFUND3_FAI_STATUS = 'FINISHED'
											   ,QFUND3_FAI_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;