/****************************************************************************
Program		: Customer Data Mart
Purpose		: Get all customer information from EADV & QF1-QF5
Programmer  : Spencer Hopkins
Date		: 10/29/2015
****************************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
 01/22/16       Spencer Hopkins     Clear temp error table at start
                					Check for errors in rollup & bior upload
									Reformatted code in driver
 02/01/16    	Spencer Hopkins		Add NextGen Input as sub program
 02/05/16       Spencer Hopkins     Check for errors after rollup, 
											before bior upload
 02/09/16       Spencer Hopkins		Add new macro variable 'filepath'
										- determines where to look for file
										  depending on who is running the
										  program (If svc_sasdata, look to
									      production folder - otherwise,
									      look to dev folder
 02/23/16		Spencer Hopkins		Remove EADV from driver
 05/18/16		Spencer Hopkins		Add Online systask command
 05/24/16		Spencer Hopkins		Add Email to BI_DATA & Slack integration
 02/27/17		Spencer Hopkins		Add ETL Check
 03/23/17		Spencer Hopkins		Update to allow any user to run
  
*****************************************************************************
*****************************************************************************
*/

/* AUDIT PRORGAM */
%include "E:\Shared\CADA\SAS Source Code\PRODUCTION\SAS Macro\CADA_AUDIT_PROG.sas";
%AUDITPROG(Skynet_CUST_AND_TRANS_CUSTDM_0_DRIVER,DAILY,BIOR.O_CUSTOMER_ALL)

/* INCLUDE ERROR_CHECK */
%include "E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart\CUSTDM_ERROR_INPUTS.sas";
%clearErrorTable;

/* INCLUDE ETL CHECK */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SAS MACRO\ETL_CHECK.SAS";

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE whatDayIsIt other=%0Y.%0m.%0d (DATATYPE=DATE);
	PICTURE whatTimeIsIt other=%0h.%0M.%0S (DATATYPE=TIME);
RUN;

DATA _NULL_;
	CALL SYMPUTX('timestamp',CATX('_',PUT(TODAY(),whatDayIsIt.),PUT(TIME(),whatTimeIsIt.)),'G');
RUN;

/* DETERMINE WHERE TO STORE LOGS */
%MACRO set_environment;
	DATA _NULL_;
		CALL SYMPUTX('logpath',"E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart\Logs",'G');
		CALL SYMPUTX('filepath',"E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart",'G');
	RUN;
%MEND;


/* RUN SUBPROGRAMS */
%set_environment

SYSTASK KILL _ALL_;

OPTIONS SYMBOLGEN NOQUOTELENMAX LRECL=32767;

/*SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'*/
/*				 '&filepath.\CUSTDM_1_EADV_INPUT.sas'*/
/*				 -log '&logpath.\EADV_CUST_&timestamp..log'*/
/*				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"*/
/*				 TASKNAME=EADV*/
/*				 STATUS=EADV;*/
				 
SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_2_QF1_QF2_INPUT.sas'
				 -log '&logpath.\QF1_QF2_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=QF1_QF2
				 STATUS=QF1_QF2;

SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_3_QF3_INPUT.sas'
				 -log '&logpath.\QF3_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=QF3
				 STATUS=QF3;

SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_4_QF4_INPUT.sas'
				 -log '&logpath.\QF4_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=QF4
				 STATUS=QF4;

SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_5_QF5_INPUT.sas'
				 -log '&logpath.\QF5_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=QF5
				 STATUS=QF5;

SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_6_NG_INPUT.sas'
				 -log '&logpath.\NG_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=NG
				 STATUS=NG;

SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_7_ONLINE_INPUT.sas'
				 -log '&logpath.\ONLINE_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=OL
				 STATUS=OL;

WAITFOR _ALL_ /*EADV*/ QF1_QF2 QF3 QF4 QF5 NG OL;


/* CHECK ETL STATUS */
%CONTINUE

/* CHECK FOR ERROR(S) - IF ERROR(S) SEND EMAIL AND ABORT DRIVER */
%ifErrorSendEmail

/* ROLLUP ALL CUSTOMER POS TABLES */
%include "&filepath.\CUSTDM_8_ROLLUP.sas";
%ifErrorSendEmail

/* UPLOAD ROLLUP TO BIOR */
%include "&filepath.\CUSTDM_9_BIOR_UPLOAD.sas";
%ifErrorSendEmail


/* EMAIL BI_DATA & SLACK WHEN PROCESSES ARE COMPLETE */
OPTIONS emailsys=smtp EMAILHOST=mail.advanceamerica.net EMAILPORT=25;
filename mymail email; 
data _null_;
   file mymail
		to=('bi_data@advanceamerica.net')
		bcc=('w8z1d4s0t0c0e0m3@aacada.slack.com')
     subject='Customer Datamart is Complete - CSSSASMETA';
run;

%INCLUDE "E:\Shared\CADA\SAS Source Code\Production\Operations\SSN Encryption.sas";

%AUDITPROG(Skynet_CUST_AND_TRANS_CUSTDM_0_DRIVER,DAILY,BIOR.O_CUSTOMER_ALL)

/*** THE END ***/