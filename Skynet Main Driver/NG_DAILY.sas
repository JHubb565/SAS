/*						%IF &DAILY_NG_STATUS. = NOT_COMPLETE AND &DAILY_NG_RUN_DATE. = . %THEN*/
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
											SET NG_STATUS = 'RUNNING'
											   ,NG_RUN_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF NG_DAILY SUMMARY*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&DAILY_FILE_PATH.\STDM_STEP14_NEXTGEN_CURRENT.SAS'
												-LOG '&DAILY_LOGPATH.\NEXTGEN_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=NG1_DAILY
								STATUS=NG1_DAILY;

								WAITFOR _ALL_ NG1_DAILY;

								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET NG_STATUS = 'FINISHED'
											   ,NG_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT; 