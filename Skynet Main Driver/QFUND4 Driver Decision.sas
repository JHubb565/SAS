	%MACRO QFUND4_DRIVER_DECISION();
						/*CUSTOMER*/
						%IF &CUST_QF4_TITLE_STATUS. = NOT_COMPLETE 
								AND %EVAL(&CUST_QF4_TITLE_RUN_DATE. = .)
								AND %EVAL(&CUST_QF4_PAYDAY_RUN_DATE. = .)
								AND &CUST_QF4_PAYDAY_STATUS. = NOT_COMPLETE %THEN 
							%DO;
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF4_CUSTOMER.SAS'
												-LOG '&CUST_LOGPATH.\CUST_QF4_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_CUST_DRIVE
								STATUS=QF4_CUST_DRIVE;							
							%END;

						/*TRANSACTION*/
						%IF &TRAN_QF4_TITLE_STATUS. = NOT_COMPLETE 
								AND %EVAL(&TRAN_QF4_TITLE_RUN_DATE. = .)
								AND %EVAL(&TRAN_QF4_PAYDAY_RUN_DATE. = .)
								AND &TRAN_QF4_PAYDAY_STATUS. = NOT_COMPLETE %THEN 
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF4_TRANSACTION.SAS'
												-LOG '&TRAN_LOGPATH.\TRAN_QF4_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_TRAN_DRIVE
								STATUS=QF4_TRAN_DRIVE;							
							%END;

						/*DEALSUMMARY*/
						%IF &DEAL_QF4_TITLE_STATUS. = NOT_COMPLETE 
								AND %EVAL(&DEAL_QF4_TITLE_RUN_DATE. = .)
								AND %EVAL(&DEAL_QF4_PAYDAY_RUN_DATE. = .)
								AND &DEAL_QF4_PAYDAY_STATUS. = NOT_COMPLETE %THEN 
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF4_DEALSUMMARY.SAS'
												-LOG '&DEAL_LOGPATH.\DEAL_QF4_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_DEAL_DRIVE
								STATUS=QF4_DEAL_DRIVE;
							%END;

						/*DAILY*/
						%IF &DAIL_QF4_TITLE_STATUS. = NOT_COMPLETE 
								AND %EVAL(&DAIL_QF4_TITLE_RUN_DATE. = .)
								AND %EVAL(&DAIL_QF4_PAYDAY_RUN_DATE. = .)
								AND &DAIL_QF4_PAYDAY_STATUS. = NOT_COMPLETE %THEN 
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF4_DAILY.SAS'
												-LOG '&DAILY_LOGPATH.\DAILY_QF4_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF4_DAIL_DRIVE
								STATUS=QF4_DAIL_DRIVE;
							%END;

	%MEND;