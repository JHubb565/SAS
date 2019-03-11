/*TRAN STAGING TABLES*/
%include "E:\Shared\CADA\SAS Source Code\Development\nrochester\Libname_Statements.sas";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";

/*EADV*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_EADV
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_EADV
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_EADV ON SKYNET.TRAN_DATAMART_EADV (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF1I*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF1I
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF1I
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF1I ON SKYNET.TRAN_DATAMART_QF1I (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF1T*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF1T
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF1T
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF1 ON SKYNET.TRAN_DATAMART_QF1T (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF2*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF2
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF2
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF2 ON SKYNET.TRAN_DATAMART_QF2 (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF3*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF3
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF3
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF3 ON SKYNET.TRAN_DATAMART_QF3 (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF3TT*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF3TT
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF3TT
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF3TT ON SKYNET.TRAN_DATAMART_QF3TT (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF4P*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF4P
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF4P
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF4P ON SKYNET.TRAN_DATAMART_QF4P (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF4T*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF4T
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF4T
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF4T ON SKYNET.TRAN_DATAMART_QF4T (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*QF5*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_QF5
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_QF5
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_QF5 ON SKYNET.TRAN_DATAMART_QF5 (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*NG*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_NG
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_NG
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_NG ON SKYNET.TRAN_DATAMART_NG (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

/*ONLINE*/
PROC SQL;
	DROP TABLE SKY.TRAN_DATAMART_ONLINE
;
QUIT;
PROC SQL;
	CREATE TABLE SKY.TRAN_DATAMART_ONLINE
	LIKE TRANDM.TRANSACTION_TABLE_EADV_UPDATE
;
QUIT;

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (INSTANCE,DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWO_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (BUSINESSDT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_THR_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (CUSTNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FOU_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (DEAL_DT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_FIV_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (DEALNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SIX_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (INSTANCE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_SEV_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (LOCNBR)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_EIG_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (PRODUCT)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_NIN_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (POSTRANCD)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TEN_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (SSN)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ELE_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (STATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_TWL_TRAN_OL ON SKYNET.TRAN_DATAMART_ONLINE (TRANDATE)) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;








/*ADD JOINT INDEX ON ALL*/

PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_EADV_ALL ON SKYNET.TRAN_DATAMART_EADV (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF1I_ALL ON SKYNET.TRAN_DATAMART_QF1I (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF1T_ALL ON SKYNET.TRAN_DATAMART_QF1T (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF2_ALL ON SKYNET.TRAN_DATAMART_QF2 (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF3_ALL ON SKYNET.TRAN_DATAMART_QF3 (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF3TT_ALL ON SKYNET.TRAN_DATAMART_QF3TT (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF4P_ALL ON SKYNET.TRAN_DATAMART_QF4P (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF4T_ALL ON SKYNET.TRAN_DATAMART_QF4T (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_QF5_ALL ON SKYNET.TRAN_DATAMART_QF5 (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_NG_ALL ON SKYNET.TRAN_DATAMART_NG (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
	EXECUTE(CREATE INDEX IDX_O_ONE_TRAN_ONLINE_ALL ON SKYNET.TRAN_DATAMART_ONLINE (INSTANCE,DEALNBR,TITLE_DEALNBR,DEALTRANNBR,POSAPPLIEDCD,POSTRANCD,TRANDATE)) BY ORACLE;
DISCONNECT FROM ORACLE;
QUIT;