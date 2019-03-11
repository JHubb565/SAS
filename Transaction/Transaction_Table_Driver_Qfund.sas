/****************************************************************************
* Program		: Master Transaction Table Data Mart					  	*
* Description	: Transactional Data From All POS Systems                	*
* Programmer	: Nathan Rochester										  	*
* Date			: 12/04/2015											  	*	
****************************************************************************/
*Libname Statements;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";

*Include Audit Program;
%include "E:\Shared\CADA\SAS Source Code\PRODUCTION\SAS Macro\CADA_AUDIT_PROG.sas";
%AUDITPROG(Skynet_CUST_AND_TRANS_Transaction_Table_Driver_Qfund,DAILY,BIOR.O_DEALTRANSACTION_ALL);

*Wait For Macro;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SAS MACRO\ETL_CHECK.SAS";

/* INCLUDE ERROR_CHECK */
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY\Transaction_Table_Error_Check.sas";
%CLEARERRORTABLE;

/* DROP DATASET MACRO FOR STATS */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\NROCHESTER\TRANSACTION_DATAMART_DAILY\DROP_DATASET_MACRO.SAS";
%_EG_CONDITIONAL_DROPDS(TRANDM.CUST_STAT_ABORT);


/* CREATE TIMESTAMP */
proc format;
	picture checktheday  other=%0Y.%0m.%0d (datatype=date);
	picture checkthetime other=%0h.%0M.%0S (datatype=time);
run;

data _null_;
	call symputx('timestamp',catx('_',put(today(),checktheday.),put(time(),checkthetime.)),'G');
run;

/* DETERMINE WHO IS RUNNING PROGRAM & WHERE TO STORE LOGS */
%macro look_at_it_go;
data _null_;
	if "&SYSUSERID" = '' then call symputx('userid',"SVC_SASDATA",'G');
	else call symputx('userid',upcase("&SYSUSERID"),'G');
run;
%if &userid = SVC_SASDATA %then %do;
	data _null_;
		call symputx('logpath',"E:SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET TRANSACTION DATAMART\Logs",'G');
	run;
	data _null_;
		call symputx('PATH',"E:SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET TRANSACTION DATAMART",'G');
	run;
	%end;
%else %do;
	data _null_;
		call symputx('logpath',"E:SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET TRANSACTION DATAMART\Logs",'G');
	run;
	data _null_;
		call symputx('PATH',"E:SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET TRANSACTION DATAMART",'G');
	run;
	%end;
%mend;

%look_at_it_go;

systask kill _all_;

options nosymbolgen noquotelenmax lrecl=32767;

/* RUN SUBPROGRAMS */
systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF1_DAILYT.SAS'
				 -log '&logpath.\QF1T_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF1T
				 status=QF1T;

/*systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'*/
/*				 '&PATH.\TRANSACTION_TABLE_QF2_DAILY.SAS'*/
/*				 -log '&logpath.\QF2_TRAN_&timestamp..log'*/
/*				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"*/
/*				 taskname=QF2*/
/*				 status=QF2;*/

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF4_DAILYT.SAS'
				 -log '&logpath.\QF4T_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF4T
				 status=QF4T;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF4_DAILYP.SAS'
				 -log '&logpath.\QF4P_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF4P
				 status=QF4P;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF5_DAILY.SAS'
				 -log '&logpath.\QF5_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF5
				 status=QF5;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_NG_DAILY.SAS'
				 -log '&logpath.\NG_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=NG
				 status=NG;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_AANET_DAILY.SAS'
				 -log '&logpath.\AANET_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=AANET
				 status=AANET;

/*systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'*/
/*				 '&PATH.\TRANSACTION_TABLE_LOC_DAILY.SAS'*/
/*				 -log '&logpath.\LOC_TRAN_&timestamp..log'*/
/*				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"*/
/*				 taskname=LOC*/
/*				 status=LOC;*/

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF3_DAILYTT.SAS'
				 -log '&logpath.\QF3TT_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF3TT
				 status=QF3TT;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF1_DAILYI.SAS'
				 -log '&logpath.\QF1I_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF1I
				 status=QF1I;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_QF3_DAILY.SAS'
				 -log '&logpath.\QF3_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=QF3
				 status=QF3;

waitfor _all_ QF5;
%CONTINUE

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TEMPTABLES_5_YEAR_MAINTAIN.SAS'
				 -log '&logpath.\5_year_maintain_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=YEARMAIN
				 status=YEARMAIN;

waitfor _all_ YEARMAIN;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\DELETE_NCP_BIOR.SAS'
				 -log '&logpath.\DELETE_NCP_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=DELETENCP
				 status=DELETENCP;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\TRANSACTION_TABLE_NCP_DAILY.SAS'
				 -log '&logpath.\NCP_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=NCP
				 status=NCP;

waitfor _all_ NCP;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\Transaction_Table_Rollup_QF5_DAILY.SAS'
				 -log '&logpath.\Rollup_QF5_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=RollupQF5
				 status=RollupQF5;


waitfor _all_ EADV QF1I QF3 QF4T QF4P NG DELETENCP RollupQF5 AANET  /*LOC*/  QF1T QF3TT;

%macro checkds(dsn);
  %if %sysfunc(exist(&dsn)) %then %do;
  %PUT EXISTS;
  %end;
  %else %do;
/*  %PUT DOES NOT EXIST;*/
  DATA _NULL_;
  %PUT ERROR: Dataset does not exist;
  %RUNQUIT(&job,&sub19);
	%ABORT CANCEL;
  %end;
%mend checkds;

%checkds(TRANDM.TRANSACTION_TABLE_QF2_UPDATE);


/* CHECK FOR ERROR(S) - IF ERROR(S) SEND EMAIL AND ABORT DRIVER */
%ifErrorSendEmail;
%CONTINUE

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\Transaction_Table_Rollup.SAS'
				 -log '&logpath.\Rollup_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=Rollup
				 status=Rollup;

waitfor _all_ ROLLUP;

/* UPLOAD TRANSACTION TABLE UPDATE */
systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\Transaction_Table_Upload_Daily.SAS'
				 -log '&logpath.\UPLOAD_QFUND_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=UPLOAD
				 status=UPLOAD;

waitfor _all_ UPLOAD;

%ifErrorSendEmail;

/* RE-INDEX TRANSACTION TABLE */
/*systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'*/
/*				 '&PATH.\Transaction_Table_Indexes_Daily.sas'*/
/*				 -log '&logpath.\INDEX_&timestamp..log'*/
/*				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"*/
/*				 taskname=INDEX*/
/*				 status=INDEX;*/
/**/
/*waitfor _all_ INDEX;*/
/**/
/*%ifErrorSendEmail;*/


/*MAKE TABLE FOR STATS */

DATA TRANDM.CUST_STAT_ABORT;
INPUT X;
CARDS;
1
;
RUN;


/* EMAIL NATHAN WHEN PROCESSES ARE COMPLETE */
OPTIONS emailsys=smtp EMAILHOST=mail.advanceamerica.net EMAILPORT=25;
filename mymail email; 
data _null_;
file mymail
to=('BI_Data@advanceamerica.net')
bcc=('w8z1d4s0t0c0e0m3@aacada.slack.com')
subject='Transaction Datamart is Complete - CSSSASMETA';
run;

/*systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'*/
/*				 'E:SHARED\CADA\SAS Source Code\Production\Skynet Customer Datamart\Cust_Lifecycle_Daily.SAS'*/
/*				 -log 'E:Shared\CADA\Logs\Skynet_CUST_AND_TRANS_Cust_Lifecycle_Daily_&timestamp..log'*/
/*				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"*/
/*				 taskname=CUSTLIFE*/
/*				 status=CUSTLIFE;*/

%AUDITPROG(Skynet_CUST_AND_TRANS_Transaction_Table_Driver_Qfund,DAILY,BIOR.O_DEALTRANSACTION_ALL);