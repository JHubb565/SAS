/*						%IF &CUSTOMER_NG_STATUS. = NOT_COMPLETE AND &CUSTOMER_NG_RUN_DATE. = . %THEN*/
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
											SET FUSE_STATUS = 'RUNNING'
											   ,FUSE_RUN_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT;
								
								/*KICK OFF NG_CUSTOMER*/
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												 '&CUST_FILE_PATH.\CUSTDM_8_FUSE_INPUT.SAS'
												 -LOG '&CUST_LOGPATH.\FUSE_CUST_&TIMESTAMP..LOG'
												 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=FUSE_CUSTOMER
								STATUS=FUSE_CUSTOMER;

								WAITFOR _ALL_ FUSE_CUSTOMER;

								/*UPDATE STATUS TABLE*/
								PROC SQL;
								CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
									EXECUTE(UPDATE BIOR.DATAMART_STATUS
											SET FUSE_STATUS = 'FINISHED'
											   ,FUSE_COMPLETION_DATE = CURRENT_DATE
											WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
											)
									 BY ORACLE;
									 DISCONNECT FROM ORACLE;
								QUIT; 