%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET ONLINE_STATUS = 'UPLOADING'
			   ,ONLINE_RUN_DATE = CURRENT_DATE
			   ,LOC_STATUS = 'UPLOADING'
			   ,LOC_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&TRAN_FILE_PATH.\TRANSACTION_UPLOAD_MASTER.SAS";
%TRANMERGEINTO(ONLINE)


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET ONLINE_STATUS = 'FINISHED'
			   ,ONLINE_COMPLETION_DATE = CURRENT_DATE
			   ,LOC_STATUS = 'FINISHED'
			   ,LOC_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;