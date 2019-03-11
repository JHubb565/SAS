/*TRUNCATE TABLES DAILY*/

/*EADV*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_EADV) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QFUND1*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF1) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND2*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF2) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND1 TITLE*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF1T) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND3 TXTITLE*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF3) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND3 TETL*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_TETL) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND3 TTOC*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_TTOC) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND3 FAI*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_FAI) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND4*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF4) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND5 INSTALLMENT*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF5I) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND5 TITLE*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF5T) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*QFUND5 PAYDAY*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_QF5P) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*NEXTGEN*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_NG) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
/*ONLINE*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_DATAMART_OL) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;