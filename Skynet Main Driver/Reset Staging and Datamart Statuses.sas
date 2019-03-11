/*UPDATE BIOR.DATAMART_STATUS TABLE WITH "NOT COMPLETE" VALUES TO SHOW THE START OF THE LOOP*/
PROC SQL;
	UPDATE BIOR.DATAMART_STATUS
	SET
	CURRENT_DATE = DATETIME()
	,EADV_STATUS = "NOT_COMPLETE"
	,EADV_RUN_DATE = .
	,EADV_COMPLETION_DATE = .
	,QFUND1_INSTALL_STATUS = "NOT_COMPLETE"
	,QFUND1_INSTALL_RUN_DATE = .
	,QFUND1_INSTALL_COMPLETION_DATE = .
	,QFUND1_TITLE_STATUS = "NOT_COMPLETE"
	,QFUND1_TITLE_RUN_DATE = .
	,QFUND1_TITLE_COMPLETION_DATE = .
	,QFUND2_INSTALL_STATUS = "NOT_COMPLETE"
	,QFUND2_INSTALL_RUN_DATE = .
	,QFUND2_INSTALL_COMPLETION_DATE = .
	,QFUND3_TTOC_STATUS = "NOT_COMPLETE"
	,QFUND3_TTOC_RUN_DATE = .
	,QFUND3_TTOC_COMPLETION_DATE = .
	,QFUND3_TXTITLE_STATUS = "NOT_COMPLETE"
	,QFUND3_TXTITLE_RUN_DATE = .
	,QFUND3_TXTITLE_COMPLETION_DATE = .
	,QFUND3_TETL_STATUS = "NOT_COMPLETE"
	,QFUND3_TETL_RUN_DATE = .
	,QFUND3_TETL_COMPLETION_DATE = .
	,QFUND3_FAI_STATUS = "NOT_COMPLETE"
	,QFUND3_FAI_RUN_DATE = .
	,QFUND3_FAI_COMPLETION_DATE = .
	,QFUND4_PAYDAY_STATUS = "NOT_COMPLETE"
	,QFUND4_PAYDAY_RUN_DATE = .
	,QFUND4_PAYDAY_COMPLETION_DATE = .
	,QFUND4_TITLE_STATUS = "NOT_COMPLETE"
	,QFUND4_TITLE_RUN_DATE = .
	,QFUND4_TITLE_COMPLETION_DATE = .
	,QFUND5_PAYDAY_STATUS = "NOT_COMPLETE"
	,QFUND5_PAYDAY_RUN_DATE = .
	,QFUND5_PAYDAY_COMPLETION_DATE = .
	,QFUND5_INSTALL_STATUS = "NOT_COMPLETE"
	,QFUND5_INSTALL_RUN_DATE = .
	,QFUND5_INSTALL_COMPLETION_DATE = .
	,QFUND5_TITLE_STATUS = "NOT_COMPLETE"
	,QFUND5_TITLE_RUN_DATE = .
	,QFUND5_TITLE_COMPLETION_DATE = .
	,NG_STATUS = "NOT_COMPLETE"
	,NG_RUN_DATE = .
	,NG_COMPLETION_DATE = .
	,ONLINE_STATUS = "NOT_COMPLETE"
	,ONLINE_RUN_DATE = .
	,ONLINE_COMPLETION_DATE = .
	,LOC_STATUS = "NOT_COMPLETE"
	,LOC_RUN_DATE = .
	,LOC_COMPLETION_DATE = .
	,FUSE_STATUS = "NOT_COMPLETE"
	,FUSE_RUN_DATE = .
	,FUSE_COMPLETION_DATE = .;
QUIT;


/*------------------------TRUNCATE STAGING TABLES-------------------------------------*/

	%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\TRUNCATE STAGING TABLES.SAS";
