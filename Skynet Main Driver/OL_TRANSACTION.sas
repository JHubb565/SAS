/*						%IF &TRAN_OL_STATUS. = NOT_COMPLETE AND &TRAN_OL_RUN_DATE. = . %THEN*/
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
											SET ONLINE_STATUS = 'RUNNING'
											   ,ONLINE_RUN_DATE = CURRENT_DATE
											   ,LOC_STATUS = 'RUNNING'
											   ,LOC_RUN_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF OL_TRANSACTION*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&TRAN_FILE_PATH.\TRANSACTION_TABLE_AANET_DAILY.SAS'
												 -LOG '&TRAN_LOGPATH.\AANET_TRAN_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=AANET_TRAN
								STATUS=AANET_TRAN;

								WAITFOR _ALL_ AANET_TRAN;


								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET ONLINE_STATUS = 'FINISHED'
											   ,ONLINE_COMPLETION_DATE = CURRENT_DATE
											   ,LOC_STATUS = 'FINISHED'
											   ,LOC_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT; 