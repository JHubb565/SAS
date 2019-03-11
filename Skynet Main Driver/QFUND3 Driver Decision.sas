	%MACRO QFUND3_DRIVER_DECISION();
						/*CUSTOMER*/
						%IF &CUST_QF3_TTOC_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&CUST_QF3_TTOC_RUN_DATE. = .)
				   			AND &CUST_QF3_TXTITLE_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&CUST_QF3_TXTITLE_RUN_DATE. = .)
				   			AND &CUST_QF3_TETL_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&CUST_QF3_TETL_RUN_DATE. = .) %THEN 
							%DO;
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF3_CUSTOMER.SAS'
												-LOG '&CUST_LOGPATH.\CUST_QF3_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_CUST_DRIVE
								STATUS=QF3_CUST_DRIVE;							
							%END;
								

						/*TRANSACTION*/
						%IF &TRAN_QF3_TTOC_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&TRAN_QF3_TTOC_RUN_DATE. = .)
				   			AND &TRAN_QF3_TXTITLE_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&TRAN_QF3_TXTITLE_RUN_DATE. = .)
				   			AND &TRAN_QF3_TETL_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&TRAN_QF3_TETL_RUN_DATE. = .) %THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF3_TRANSACTION.SAS'
												-LOG '&TRAN_LOGPATH.\TRAN_QF3_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_TRAN_DRIVE
								STATUS=QF3_TRAN_DRIVE;							
							%END;

						/*DEALSUMMARY*/
						%IF &DEAL_QF3_TTOC_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&DEAL_QF3_TTOC_RUN_DATE. = .)
				   			AND &DEAL_QF3_TXTITLE_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&DEAL_QF3_TXTITLE_RUN_DATE. = .)
				   			AND &DEAL_QF3_TETL_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&DEAL_QF3_TETL_RUN_DATE. = .) %THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF3_DEALSUMMARY.SAS'
												-LOG '&DEAL_LOGPATH.\DEAL_QF3_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_DEAL_DRIVE
								STATUS=QF3_DEAL_DRIVE;
							%END;

						/*DAILY*/
						%IF &DAIL_QF3_TTOC_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&DAIL_QF3_TTOC_RUN_DATE. = .)
				   			AND &DAIL_QF3_TXTITLE_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&DAIL_QF3_TXTITLE_RUN_DATE. = .)
				   			AND &DAIL_QF3_TETL_STATUS. = NOT_COMPLETE
				   			AND %EVAL(&DAIL_QF3_TETL_RUN_DATE. = .) %THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\QF3_DAILY.SAS'
												-LOG '&DAILY_LOGPATH.\DAILY_QF3_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=QF3_DAILY_DRIVE
								STATUS=QF3_DAILY_DRIVE;
							%END;

	%MEND;