/*						%IF &TRAN_EADV_STATUS. = NOT_COMPLETE AND &TRAN_EADV_RUN_DATE. = . %THEN*/
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
											WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF EADV_DEALTRANSACTION*/

								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&TRAN_FILE_PATH.\DELETE_CSO_TEMP.SAS'
												 -LOG '&TRAN_LOGPATH.\DELETE_CSO_TEMP_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=DELETECSOTEMP
								STATUS=DELETECSOTEMP;

								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&TRAN_FILE_PATH.\TRANSACTION_TABLE_EADV_DAILY.SAS'
												 -LOG '&TRAN_LOGPATH.\TRANSACTION_EADV_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=TRAN_EADV
								STATUS=TRAN_EADV;

								WAITFOR _ALL_ TRAN_EADV DELETECSOTEMP;

								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET EADV_STATUS = 'FINISHED'
											   ,EADV_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;


