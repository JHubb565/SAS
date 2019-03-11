/*--------------------------TRUNCATE STAGING TABLES--------------------------


	PURPOSE OF THIS PROGRAM IS TRUNCATE THE CUSTOMER STAGING TABLES
	BEFORE THE DAYS RUN. THIS TRUNCATION WILL OCCUR AT THE START OF THE
	DATAMART DRIVER


-------------------------------------------------------------------------------*/

/*EADV*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_EADV) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*NG*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_NG) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*ONLINE*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_OL) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF1QF2*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_QF1QF2) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF3*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_QF3) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF4*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_QF4) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF5*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.CUSTOMER_DATAMART_QF5) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*CUST NUM*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.DAILYSUM_CUST_NUM) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*CUST NUM*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(TRUNCATE TABLE SKYNET.TRANSPOSE_DAILY_SUMMARY_CUST) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

