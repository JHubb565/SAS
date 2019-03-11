*Libname Statements;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";

proc format;
	picture checktheday other=%0Y.%0m.%0d (datatype=date);
	picture checkthetime other=%0h.%0M.%0S (datatype=time);
RUN;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
data _null_;
	call symputx('timestamp',catx('_',put(today(),checktheday.),put(time(),checkthetime.)),'G');
	call symputx('PATH',"E:SHARED\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY",'G');
	call symputx('PATHTWO',"E:\Shared\CADA\SAS Data\user\nrochester\",'G');
RUN;

PROC SQL;    
CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;
EXECUTE (TRUNCATE TABLE TEMPTABLES.TRANSACTION_TABLE_UPDATE1) BY ORACLE;
DISCONNECT FROM ORACLE;
QUIT;

PROC SQL;
INSERT INTO TEMPTABL.TRANSACTION_TABLE_UPDATE1 (BULKLOAD=YES BL_LOG="&PATH.\Logs\BL_&timestamp..LOG" BL_DELETE_DATAFILE=YES 
												BL_DEFAULT_DIR="&PATHTWO.")
SELECT *
FROM TRANDM.TRANSACTION_TABLE_UPDATE1;
QUIT;