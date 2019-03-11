%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND1_INSTALL_STATUS = 'UPLOADING'
			   ,QFUND1_INSTALL_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&TRAN_FILE_PATH.\TRANSACTION_UPLOAD_MASTER.SAS";
%TRANMERGEINTO(QF1I)


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND1_INSTALL_STATUS = 'FINISHED'
			   ,QFUND1_INSTALL_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;