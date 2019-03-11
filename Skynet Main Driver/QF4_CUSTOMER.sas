/*						%IF &CUSTOMER_QFUND4_STATUS. = NOT_COMPLETE AND &CUSTOMER_QFUND4_RUN_DATE. = . %THEN*/
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
											WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF QFUND4_CUSTOMER*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&CUST_FILE_PATH.\CUSTDM_4_QF4_INPUT.SAS'
												 -LOG '&CUST_LOGPATH.\QF4_CUST_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_CUSTOMER
								STATUS=QF4_CUSTOMER;

								WAITFOR _ALL_ QF4_CUSTOMER;

								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET QFUND4_PAYDAY_STATUS = 'FINISHED'
											   ,QFUND4_PAYDAY_COMPLETION_DATE = CURRENT_DATE
											   ,QFUND4_TITLE_STATUS = 'FINISHED'
											   ,QFUND4_TITLE_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;