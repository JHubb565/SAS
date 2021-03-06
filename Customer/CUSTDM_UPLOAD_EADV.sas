%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\CUSTOMER\CUSTDM_ERROR_INPUTS.SAS";
/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET EADV_STATUS = 'UPLOADING'
			   ,EADV_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub8);

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&CUST_FILE_PATH.\CUSTDM_UPLOAD_MASTER.SAS";
%CUSTMERGEINTO(EADV)


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH="&PATH.");
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET EADV_STATUS = 'FINISHED'
			   ,EADV_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_CUSTOMER_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub8);
