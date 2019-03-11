	%MACRO QF1_QF2_DRIVER_DECISION();
						/*CUSTOMER*/
						%IF &CUST_QF1_INSTALL_STATUS. = NOT_COMPLETE 
								AND &CUST_QF1_TITLE_STATUS. = NOT_COMPLETE 
								AND %EVAL(&CUST_QF1_INSTALL_RUN_DATE. = .) 
								AND %EVAL(&CUST_QF1_TITLE_RUN_DATE. = .) 
								AND &CUST_QF2_INSTALL_STATUS. = NOT_COMPLETE
								AND %EVAL(&CUST_QF2_INSTALL_RUN_DATE. = .)	%THEN
							%DO;
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF1_QF2_CUSTOMER.SAS'
												-LOG '&CUST_LOGPATH.\QF1_QF2_CUSTOMER_DRIVE_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF1QF2_CUST_DRIVE
								STATUS=QF1QF2_CUST_DRIVE;
							%END;
								

						/*TRANSACTION*/
						%IF /*&TRAN_QF1_INSTALL_STATUS. = NOT_COMPLETE 
								AND*/ /*&TRAN_QF1_TITLE_STATUS. = NOT_COMPLETE*/ 
								/*AND &TRAN_QF1_INSTALL_RUN_DATE. = . 
								AND &TRAN_QF1_TITLE_RUN_DATE. = .
								AND*/ &TRAN_QF2_INSTALL_STATUS. = NOT_COMPLETE
								AND %EVAL(&TRAN_QF2_INSTALL_RUN_DATE. = .)	%THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF1_QF2_TRANSACTION.SAS'
												-LOG '&TRAN_LOGPATH.\QF1_QF2_TRANSACTION_DRIVE_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF1QF2_TRAN_DRIVE
								STATUS=QF1QF2_TRAN_DRIVE;

							%END;

						/*DEALSUMMARY*/
						%IF &DEAL_QF1_INSTALL_STATUS. = NOT_COMPLETE 
								/*AND &DEAL_QF1_TITLE_STATUS. = NOT_COMPLETE*/ 
								AND %EVAL(&DEAL_QF1_INSTALL_RUN_DATE. = .) 
								/*AND &DEAL_QF1_TITLE_RUN_DATE. = . */
								AND &DEAL_QF2_INSTALL_STATUS. = NOT_COMPLETE
								AND %EVAL(&DEAL_QF2_INSTALL_RUN_DATE. = .)	%THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF1_QF2_DEALSUMMARY.SAS'
												-LOG '&DEAL_LOGPATH.\QF1_QF2_DEALSUMMARY_DRIVE_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF1QF2_DEAL_DRIVE
								STATUS=QF1QF2_DEAL_DRIVE;
							%END;

						/*DAILY*/
						%IF &DAIL_QF1_INSTALL_STATUS. = NOT_COMPLETE 
								/*AND &DAIL_QF1_TITLE_STATUS. = NOT_COMPLETE*/
								AND %EVAL(&DAIL_QF1_INSTALL_RUN_DATE. = .) 
								/*AND &DAIL_QF1_TITLE_RUN_DATE. = . */
								AND &DAIL_QF2_INSTALL_STATUS. = NOT_COMPLETE
								AND &DAIL_QF1_TITLE_STATUS. = NOT_COMPLETE
								AND %EVAL(&DAIL_QF1_TITLE_RUN_DATE. = .)
								AND %EVAL(&DAIL_QF2_INSTALL_RUN_DATE. = .)	%THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF1_QF2_DAILY.SAS'
												-LOG '&DAILY_LOGPATH.\QF1_QF2_DAILY_DRIVE_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF1QF2_DAILY_DRIVE
								STATUS=QF1QF2_DAILY_DRIVE;

							%END;

	%MEND;