*Error Checking;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY\Transaction_Table_Error_Check.sas";

*Libname Statements;
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";


proc format;
	picture checktheday other=%0Y.%0m.%0d (datatype=date);
	picture checkthetime other=%0h.%0M.%0S (datatype=time);
run;

data _null_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
	call symputx('PATH',"E:SHARED\CADA\SAS Source Code\Development\nrochester\TRANSACTION_DATAMART_DAILY",'G');
	call symputx('PATHTWO',"E:\Shared\CADA\SAS Data\user\nrochester\",'G');
run;

PROC SQL;
INSERT INTO TEMPTABL.O_CSO_5_YEAR (BULKLOAD=YES BL_LOG="&PATH.\Logs\BL_&timestamp..LOG" BL_DELETE_DATAFILE=YES
												BL_DEFAULT_DIR="&PATHTWO.")
SELECT T1.*,
	  INPUT(T1.DEALNBR,15.) AS DEALNBR1
FROM TRANDM.TRANSACTION_TABLE_QF5_UPDATE T1;
QUIT;

PROC SQL;    
CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;
EXECUTE (TRUNCATE TABLE TEMPTABLES.O_DT_CSO_5_YEAR) BY ORACLE;
QUIT;

PROC SQL;    
CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;
EXECUTE (TRUNCATE TABLE TEMPTABLES.O_DT_CSO_5_YEAR_2) BY ORACLE;
QUIT;

PROC SQL;
INSERT INTO TEMPTABL.O_DT_CSO_5_YEAR  (BULKLOAD=YES BL_LOG="&PATH.\Logs\BL_&timestamp..LOG" BL_DELETE_DATAFILE=YES
												BL_DEFAULT_DIR="&PATHTWO.")
SELECT DISTINCT 
	DEALNBR,
	DEALNBR1
FROM TEMPTABL.O_CSO_5_YEAR  T1;
QUIT;

PROC SQL;
INSERT INTO TEMPTABL.O_DT_CSO_5_YEAR_2  (BULKLOAD=YES BL_LOG="&PATH.\Logs\BL_&timestamp..LOG" BL_DELETE_DATAFILE=YES
												BL_DEFAULT_DIR="&PATHTWO.")
SELECT DISTINCT 
	DEALNBR,
	DEALNBR1
FROM TEMPTABL.O_CSO_5_YEAR  T1;
QUIT;

