%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET ONLINE_STATUS = 'UPLOADING'
			   ,ONLINE_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub29);

/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_MASTER.SAS";
%DAILMERGEINTO(OL)


/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET ONLINE_STATUS = 'FINISHED'
			   ,ONLINE_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub29);
