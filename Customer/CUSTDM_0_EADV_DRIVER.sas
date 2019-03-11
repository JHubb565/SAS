/****************************************************************************
Program		: Customer Data Mart - EADV Driver
Purpose		: Kick off EADV process
Programmer  : Spencer Hopkins
Date		: 11/3/2016
****************************************************************************/

*OPTIONS SOURCE2;
/* INCLUDE ERROR_CHECK */
%include "E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart\CUSTDM_ERROR_INPUTS.sas";
%clearErrorTable;

/*	INCLUDE LIBNAMES SCRIPT */
%include "E:\Shared\CADA\SAS Source Code\Development\shopkins\LIBNAMES.sas";


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
	IF "&SYSUSERID" = '' THEN CALL SYMPUTX('userid',"SVC_SASDATA",'G');
	ELSE CALL SYMPUTX('userid',UPCASE("&SYSUSERID"),'G');
RUN;

%IF &userid = SVC_SASDATA %THEN %DO;
	DATA _NULL_;
		CALL SYMPUTX('logpath',"E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart\Logs",'G');
		CALL SYMPUTX('filepath',"E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart",'G');
	RUN;
	%END;
%ELSE %DO;
	DATA _NULL_;
		CALL SYMPUTX('logpath',"E:\Shared\CADA\SAS Source Code\Development\shopkins\02_Customer Datamart\Logs",'G');
		CALL SYMPUTX('filepath',"E:\Shared\CADA\SAS Source Code\Development\shopkins\02_Customer Datamart",'G');
	RUN;
%END;

%MEND;

/* RUN SUBPROGRAMS */
%set_environment;

SYSTASK KILL _ALL_;

OPTIONS SYMBOLGEN NOQUOTELENMAX LRECL=32767;

SYSTASK COMMAND "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&filepath.\CUSTDM_1_EADV_INPUT.sas'
				 -log '&logpath.\EADV_CUST_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 TASKNAME=EADV
				 STATUS=EADV;

WAITFOR _ALL_ EADV;


/* CHECK FOR ERROR(S) - IF ERROR(S) SEND EMAIL AND ABORT DRIVER */
%ifErrorSendEmail;



/*
============================================================================= 
     TRUNCATE DATA IN BIOR & UPLOAD EADV DATA TO BIOR
=============================================================================
*/

/* TRUNCATE DATA IN TABLE */
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE BIOR.O_CUSTOMER_ALL) BY ORACLE; 
	DISCONNECT FROM ORACLE; 
QUIT;

/* EMAIL BI_DATA WHEN EADV UPLOAD IS COMPLETE ARE COMPLETE */

/*%INCLUDE "E:\SHARED\CADA\SAS DATA\USER\SCOCHRAN\EMAIL PASSWORD\EMAIL_AUTH.SAS";*/

OPTIONS EMAILSYS=SMTP EMAILHOST=MAIL.ADVANCEAMERICA.NET EMAILPORT=25;
FILENAME MYMAIL EMAIL; 
DATA _NULL_;
   FILE MYMAIL
		TO='BI_DATA@ADVANCEAMERICA.NET'
        SUBJECT='CUSTOMER DATAMART (EADV) IS COMPLETE - CSSSASMETA';
RUN;