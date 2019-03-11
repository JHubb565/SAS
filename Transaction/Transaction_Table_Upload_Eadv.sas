*Error Checking;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY\Transaction_Table_Error_Check.sas";

*Libname Statements;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";

proc format;
	picture checktheday other=%0Y.%0m.%0d (datatype=date);
	picture checkthetime other=%0h.%0M.%0S (datatype=time);
%RUNQUIT(&job,&sub15);
/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
data _null_;
	call symputx('timestamp',catx('_',put(today(),checktheday.),put(time(),checkthetime.)),'G');
	call symputx('PATH',"E:SHARED\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY",'G');
	call symputx('PATHTWO',"E:\Shared\CADA\SAS Data\user\nrochester\",'G');
%RUNQUIT(&job,&sub15);

/* UPLOAD 30 DAY QFUND AND NG PORTION TO BIOR */
PROC SQL;
	INSERT INTO BIOR.O_DEALTRANSACTION_ALL (BULKLOAD=YES BL_LOG="&PATH.\Logs\BL_&timestamp..LOG" BL_DELETE_DATAFILE=YES 
												BL_DEFAULT_DIR="&PATHTWO.")
	SELECT 
		*
	FROM TRANDM.TRANSACTION_TABLE_EADV_UPDATE;
%RUNQUIT(&job,&sub15);
