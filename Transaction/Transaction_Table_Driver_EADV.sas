/****************************************************************************
* Program		: Master Transaction Table Data Mart					  	*
* Description	: Transactional Data From All POS Systems                	*
* Programmer	: Nathan Rochester										  	*
* Date			: 12/04/2015											  	*	
****************************************************************************/
*Libname Statements;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";

/* INCLUDE ERROR_CHECK */
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY\Transaction_Table_Error_Check.sas";
%clearErrorTable;

/* CREATE TIMESTAMP */
proc format;
	picture checktheday other=%0Y.%0m.%0d (datatype=date);
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

options symbolgen noquotelenmax lrecl=32767;

/* DROP INDEXES */
/*systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'*/
/*				 '&PATH\Transaction_Table_Indexes_Drop.SAS'*/
/*				 -log '&logpath.\Drop_&timestamp..log'*/
/*				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"*/
/*				 taskname=DROP*/
/*				 status=DROP;*/

/*waitfor _all_ DROP;*/

/* RUN SUBPROGRAMS */
systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH\Delete_From.SAS'
				 -log '&logpath.\DELETE_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=DELETE
				 status=DELETE;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH\Delete_CSO_Temp.SAS'
				 -log '&logpath.\DELETE_CSO_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=DELETECSO
				 status=DELETECSO;

systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH\Transaction_Table_EADV_Daily.SAS'
				 -log '&logpath.\EADV_TRAN_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=EADV
				 status=EADV;

waitfor _all_ DELETE DELETECSO EADV;

%ifErrorSendEmail;
		
/* UPLOAD TRANSACTION TABLE UPDATE */
systask command "'c:\program files\sashome\sasfoundation\9.4\sas.exe'
				 '&PATH.\Transaction_Table_Upload_Eadv.SAS'
				 -log '&logpath.\UPLOAD_Eadv_&timestamp..log'
				 -config 'c:\program files\sashome\sasfoundation\9.4\sasv9.cfg'"
				 taskname=UPLOAD
				 status=UPLOAD;

waitfor _all_ UPLOAD;

/* EMAIL NATHAN WHEN PROCESSES ARE COMPLETE */
options emailsys=smtp emailhost=mail.advanceamerica.net emailport=25;
filename mymail email; 
data _null_;
   file mymail
		to=('mascott@advanceamerica.net','rmugabe@advanceamerica.net','shopkins@advanceamerica.net','jhubbard@advanceamerica.net')
	    from='eadvupdm@complete.com'
        subject='Transaction Datamart Eadv is Complete - CSSSASMETA';
run;

%ifErrorSendEmail;