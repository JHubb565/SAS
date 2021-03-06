	%MACRO FUSE_DRIVER_DECISION();
						/*CUSTOMER*/
						%IF &CUST_FUSE_STATUS. = NOT_COMPLETE 
							AND %EVAL(&CUST_FUSE_RUN_DATE. = .) %THEN 
							%DO;
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\FUSE_CUSTOMER.SAS'
												-LOG '&CUST_LOGPATH.\CUST_FUSE_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=FUSE_CUST_DRIVE
								STATUS=FUSE_CUST_DRIVE;							
							%END;
								
						/*TRANSACTION*/
						%IF &TRAN_FUSE_STATUS. = NOT_COMPLETE 
							AND %EVAL(&TRAN_FUSE_RUN_DATE. = .) %THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\FUSE_TRANSACTION.SAS'
												-LOG '&TRAN_LOGPATH.\TRAN_FUSE_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=FUSE_TRAN_DRIVE
								STATUS=FUSE_TRAN_DRIVE;							
							%END;

						/*DEALSUMMARY*/
						%IF &DEAL_FUSE_STATUS. = NOT_COMPLETE 
							AND %EVAL(&DEAL_FUSE_RUN_DATE. = .) %THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\FUSE_DEALSUMMARY.SAS'
												-LOG '&DEAL_LOGPATH.\DEAL_FUSE_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=FUSE_DEAL_DRIVE
								STATUS=FUSE_DEAL_DRIVE;
							%END;

						/*DAILY*/
						%IF &DAIL_FUSE_STATUS. = NOT_COMPLETE 
							AND %EVAL(&DAIL_FUSE_RUN_DATE. = .) %THEN
							%DO; 
								SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
												'&SKYNETREDESIGN.\FUSE_DAILY.SAS'
												-LOG '&DAILY_LOGPATH.\DAILY_FUSE_DRIVER_&TIMESTAMP..LOG'
												-CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
								TASKNAME=FUSE_DAILY_DRIVE
								STATUS=FUSE_DAILY_DRIVE;
							%END;

	%MEND;