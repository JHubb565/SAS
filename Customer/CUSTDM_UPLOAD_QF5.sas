%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\CUSTOMER\CUSTDM_ERROR_INPUTS.SAS";

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND5_PAYDAY_STATUS = 'UPLOADING'
			   ,QFUND5_PAYDAY_RUN_DATE = CURRENT_DATE
			   ,QFUND5_INSTALL_STATUS = 'UPLOADING'
			   ,QFUND5_INSTALL_RUN_DATE = CURRENT_DATE
			   ,QFUND5_TITLE_STATUS = 'UPLOADING'
			   ,QFUND5_TITLE_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub12);

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&CUST_FILE_PATH.\CUSTDM_UPLOAD_MASTER.SAS";
%CUSTMERGEINTO(QF5)


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND5_PAYDAY_STATUS = 'FINISHED'
			   ,QFUND5_PAYDAY_COMPLETION_DATE = CURRENT_DATE
			   ,QFUND5_INSTALL_STATUS = 'FINISHED'
			   ,QFUND5_INSTALL_COMPLETION_DATE = CURRENT_DATE
			   ,QFUND5_TITLE_STATUS = 'FINISHED'
			   ,QFUND5_TITLE_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub12);