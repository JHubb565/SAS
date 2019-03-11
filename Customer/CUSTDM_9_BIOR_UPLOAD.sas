/**********************************************************************
Sub Program	: BIOR Upload
Main		: Customer Datamart
Purpose		: Upload customer master table to BIOR
Programmer  : Spencer Hopkins
***********************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
  01/22/2016	Spencer Hopkins		Change run/quit statements to %RUNQUIT
  02/11/2016    Spencer Hopkins		Drop indices before appending data
									Recreate indices after appending 
  04/05/2016    Spencer Hopkins		Added bulkload option
  05/13/2016    Spencer Hopkins		Small change to commented out section
										(pos_custnbr -> custnbr)
  07/15/2016	Spencer Hopkins		Comment out granting permissions
									Comment out truncate
									
*****************************************************************************
*****************************************************************************
*/


/*
============================================================================= 
     INCLUDE ERROR_CHECK
=============================================================================
*/
%include "E:\Shared\CADA\SAS Source Code\Production\Skynet Customer Datamart\CUSTDM_ERROR_INPUTS.sas";


/*
============================================================================= 
     SET UP LIBRARIES
=============================================================================
*/

/*	INCLUDE LIBNAMES SCRIPT */
%include "E:\Shared\CADA\SAS Source Code\Development\shopkins\LIBNAMES.sas";


/*
============================================================================= 
     INITIAL ORACLE TABLE (only use if you need to drop and recreate table)
=============================================================================
*/

/*proc sql;*/
/* connect to oracle (USER=svc_sasuser PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH='bior');*/
/* exec(drop table bior.O_CUSTOMER_ALL) by oracle;*/
/* disconnect from oracle;*/
/*quit;*/
/**/
/*proc sql;*/
/*	create table BIOR.O_CUSTOMER_ALL as*/
/*		select * from CUSTDM.CUSTOMER_DATAMART_ALL;*/
/*quit;*/


/*
============================================================================= 
	DROP CONSTRAINTS & INDICES 
=============================================================================
*/

* DROP CONSTRAINT;
/*PROC SQL; */
/*	CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;*/
/*	EXECUTE (ALTER TABLE BIOR.O_CUSTOMER_ALL DROP CONSTRAINT IDX_O_CUST_ALL_PK) BY ORACLE;*/
/*	DISCONNECT FROM ORACLE;*/
/*%RUNQUIT(&job,&sub9);*/

* DROP INDICES;
/*PROC SQL;    */
/*	CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;*/
/*	EXECUTE (DROP INDEX IDX_O_CUST_ALL_SSN)    BY ORACLE;*/
/*	EXECUTE (DROP INDEX IDX_O_CUST_ALL_PK)     BY ORACLE;*/
/*	DISCONNECT FROM ORACLE;*/
/*%RUNQUIT(&job,&sub9);*/


/*
============================================================================= 
     UPDATE (REPLACE) CUSTOMER TABLE
=============================================================================
*/

* TRUNCATE DATA IN TABLE;
/*PROC SQL;*/
/*	CONNECT TO ORACLE(USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH='BIOR');*/
/*	EXECUTE(TRUNCATE TABLE BIOR.O_CUSTOMER_ALL) BY ORACLE; */
/*	DISCONNECT FROM ORACLE; */
/*%RUNQUIT(&job,&sub9);*/

* APPEND ROLLUP TO BIOR;
/*PROC APPEND BASE=BIOR.O_CUSTOMER_ALL DATA=CUSTDM.CUSTOMER_DATAMART_ALL;*/
/*%RUNQUIT(&job,&sub9);*/

proc format;
    picture checktheday other=%0Y.%0m.%0d (datatype=date);
    picture checkthetime other=%0h.%0M.%0S (datatype=time);
%RUNQUIT(&job,&sub9);
/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
data _null_;
    call symputx('timestamp',catx('_',put(today(),checktheday.),put(time(),checkthetime.)),'G');
    call symputx('PATH',"E:SHARED\CADA\SAS Source Code\Development\shopkins\02_customer datamart",'G');
    call symputx('PATHTWO',"E:\Shared\CADA\SAS Data\user\SHOPKINS\",'G');
%RUNQUIT(&job,&sub9);

PROC SQL;
    INSERT INTO BIOR.O_CUSTOMER_ALL (BULKLOAD=YES BL_LOG="&PATH.\Logs\BL_&timestamp..LOG" BL_DELETE_DATAFILE=YES 
                                                BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM CUSTDM.CUSTOMER_DATA_NOEADV;
%RUNQUIT(&job,&sub9);

/*
============================================================================= 
     RECREATE INDICES & CONSTRAINTS + GRANT PERMISSIONS
=============================================================================
*/

/** ADD INDICES;*/
/*PROC SQL;    */
/*	CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;*/
/*	EXECUTE (CREATE UNIQUE INDEX IDX_O_CUST_ALL_PK  ON BIOR.O_CUSTOMER_ALL (INSTANCE,CUSTNBR) TABLESPACE FINOR_TBL9_INDEX) BY ORACLE;*/
/*	EXECUTE (CREATE INDEX IDX_O_CUST_ALL_SSN ON BIOR.O_CUSTOMER_ALL (SSN) TABLESPACE FINOR_TBL9_INDEX) BY ORACLE;*/
/*	DISCONNECT FROM ORACLE;  */
/*%RUNQUIT(&job,&sub9);*/
/**/
/** ADD CONSTRAINT;*/
/*PROC SQL;    */
/*	CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;*/
/*	EXECUTE (ALTER TABLE BIOR.O_CUSTOMER_ALL ADD (CONSTRAINT IDX_O_CUST_ALL_PK PRIMARY KEY (INSTANCE, CUSTNBR) USING INDEX SVC_SASUSER.IDX_O_CUST_ALL_PK ENABLE VALIDATE)) BY ORACLE;*/
/*	DISCONNECT FROM ORACLE;  */
/*%RUNQUIT(&job,&sub9);*/

/** GRANT PERMISSIONS;*/
/*PROC SQL;*/
/*     CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH='BIOR');*/
/*     EXECUTE(GRANT SELECT ON BIOR.O_CUSTOMER_ALL TO BIOR_SELECT_ONLY) BY ORACLE;*/
/*     DISCONNECT FROM ORACLE;*/
/*%RUNQUIT(&job,&sub9);*/
