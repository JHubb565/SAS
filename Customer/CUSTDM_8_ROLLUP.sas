/**********************************************************************
Sub Program	: Rollup
Main		: Customer Datamart
Purpose		: Combine all POS customer tables into master table
Programmer  : Spencer Hopkins
***********************************************************************/

/*
*****************************************************************************
*****************************************************************************
CHANGE LOG:
  DATE        	BY                 	COMMENTS  
=============================================================================
  01/22/2016	Spencer Hopkins		Change run/quit statements to %RUNQUIT    
  02/01/2016    Spencer Hopkins     Add NextGen to rollup	
  not sure		Spencer Hopkins		Add Online to rollup
  07/15/2016	Spencer Hopkins		Remove EADV from rollup
  11/29/2017	Justin Hubbard	    Added Marketing Aquisition to Non-EADV Instances
*****************************************************************************
*****************************************************************************
*/


/*
============================================================================= 
     INCLUDE ERROR_CHECK
=============================================================================
*/
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET CUSTOMER DATAMART\CUSTDM_ERROR_INPUTS.SAS";

/*
============================================================================= 
     SET UP LIBRARIES
=============================================================================
*/
LIBNAME CUSTDM "E:\SHARED\CADA\SAS DATA\DATAMART\CUSTOMER";


/*
============================================================================= 
     COMBINE DATA SETS
=============================================================================
*/

DATA CUSTDM.CUSTOMER_DATAMART_PRE;
	SET /*CUSTDM.CUSTOMER_DATAMART_EADV*/
		CUSTDM.CUSTOMER_DATAMART_QF1QF2
		CUSTDM.CUSTOMER_DATAMART_QF3
		CUSTDM.CUSTOMER_DATAMART_QF4
		CUSTDM.CUSTOMER_DATAMART_QF5
		CUSTDM.CUSTOMER_DATAMART_NG
		CUSTDM.CUSTOMER_DATAMART_ONLINE;
RUN;

/*
============================================================================= 
     Add Marketing Aquisition to Customer DM NON EADV 
=============================================================================
*/
PROC SQL;
	CREATE TABLE MARKETING_AQUISITION AS
	SELECT INSTANCE
		  ,SSN
		  ,CUSTNBR
		  ,(UPCASE(MARKETING_SOURCE))	AS MARKETING_SOURCE 
		  ,MARKETING_SOURCE_DATE
	FROM BIOR.CUSTOMER_AQUISITION
	WHERE INSTANCE ^= 'EAPROD1'
	ORDER BY SSN
			,CUSTNBR
			,MARKETING_SOURCE_DATE
;
QUIT;

/*GET MOST RECENT MARKETING SOURCE DATE*/

DATA MOST_RECENT_AQU;
	SET MARKETING_AQUISITION;
	BY SSN
	   CUSTNBR
	   MARKETING_SOURCE_DATE;
	IF LAST.CUSTNBR THEN OUTPUT MOST_RECENT_AQU;
RUN;

/*ADD TO CUSTOMER DM (EVERYTHING EXCEPT EADV)*/

PROC SQL;
	CREATE TABLE CUSTDM.CUSTOMER_DATA_NOEADV AS
	SELECT A.*
		  ,B.MARKETING_SOURCE
		  ,B.MARKETING_SOURCE_DATE
	FROM CUSTDM.CUSTOMER_DATAMART_PRE A
	LEFT JOIN MOST_RECENT_AQU B
			ON (A.INSTANCE = B.INSTANCE
			AND A.CUSTNBR = B.CUSTNBR
			AND A.SSN = B.SSN)
;
QUIT; 

%RUNQUIT(&job,&sub8);