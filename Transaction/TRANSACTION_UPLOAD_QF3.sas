%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			  SET QFUND3_TETL_STATUS = 'UPLOADING'
			   ,QFUND3_TETL_RUN_DATE = CURRENT_DATE
			   ,QFUND3_TTOC_STATUS = 'UPLOADING'
			   ,QFUND3_TTOC_RUN_DATE = CURRENT_DATE
			   ,QFUND3_FAI_STATUS = 'UPLOADING'
			   ,QFUND3_FAI_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&TRAN_FILE_PATH.\TRANSACTION_UPLOAD_MASTER.SAS";
%TRANMERGEINTO(QF3)


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			 SET QFUND3_TETL_STATUS = 'FINISHED'
			   ,QFUND3_TETL_COMPLETION_DATE = CURRENT_DATE
			   ,QFUND3_TTOC_STATUS = 'FINISHED'
			   ,QFUND3_TTOC_COMPLETION_DATE = CURRENT_DATE
			   ,QFUND3_FAI_STATUS = 'FINISHED'
			   ,QFUND3_FAI_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEALTRANSACTION_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
QUIT;