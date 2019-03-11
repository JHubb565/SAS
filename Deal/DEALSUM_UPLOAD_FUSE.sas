%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET FUSE_STATUS = 'UPLOADING'
			   ,FUSE_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub28);

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_MASTER_UPLOAD.SAS";
%DEALMERGEINTO(FUSE)

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET FUSE_STATUS = 'FINISHED'
			   ,FUSE_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DEAL_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub28);