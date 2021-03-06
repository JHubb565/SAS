%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND1_INSTALL_STATUS = 'UPLOADING'
			   ,QFUND1_INSTALL_RUN_DATE = CURRENT_DATE
			   ,QFUND2_INSTALL_STATUS = 'UPLOADING'
			   ,QFUND2_INSTALL_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub17);

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_MASTER_UPLOAD.SAS";
%DEALMERGEINTO(QF1QF2)

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET QFUND1_INSTALL_STATUS = 'FINISHED'
			   ,QFUND1_INSTALL_COMPLETION_DATE = CURRENT_DATE
			   ,QFUND2_INSTALL_STATUS = 'FINISHED'
			   ,QFUND2_INSTALL_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub17);
