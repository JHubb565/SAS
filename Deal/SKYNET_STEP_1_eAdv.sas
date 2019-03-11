%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM\";

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub1);

OPTIONS MLOGIC MPRINT SYMBOLGEN;

LIBNAME BIOR ORACLE 
	USER=&USER 
	PASSWORD=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR DEFER=YES;

LIBNAME TMP_TBLS ORACLE 
	USER=&USER 
	PASSWORD=&PASSWORD
	PATH=BIOR
	SCHEMA=TEMPTABLES DEFER=YES;

LIBNAME EADV ORACLE
	USER=&USER
	PASSWORD=&PASSWORD
	PATH=EAPROD1
	SCHEMA=EADV DEFER=YES;

LIBNAME BIORDEV ORACLE
	USER = &USER
	PASSWORD = &PASSWORD
	PATH=BIOR
	SCHEMA=BIORDEV DEFER=YES;

DATA _NULL_;
    LENGTH FULL_RUN $ 1;
    FORMAT FULL_RUN $CHAR1.;
    INFORMAT FULL_RUN $CHAR1.;
    INFILE 'E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\SKYNET\WEEKLY INSERT DEV CODE\FULL_RUN.CSV'
        LRECL=1
        ENCODING="WLATIN1"
        TERMSTR=CRLF
        DLM='7F'x
        MISSOVER
        DSD ;
    INPUT FULL_RUN : $CHAR1.;
	IF _N_ ^= 1;
	CALL SYMPUTX('RUN_INDICATOR',FULL_RUN,'G');
	IF FULL_RUN = 'Y' THEN 
		DO;
			CALL SYMPUTX('LASTWEEK',DHMS(INTNX('YEAR',TODAY(),-6,'B'),00,00,00),'G');
		END;
	ELSE IF FULL_RUN = 'N' THEN 
		DO;
		 	CALL SYMPUTX('LASTWEEK',DHMS(INTNX('DAY',TODAY(),-5,'B'),00,00,00),'G');
		END;
%RUNQUIT(&job,&sub1);

%PUT &LASTWEEK;

%MACRO INITIAL_PULL();
%IF "&RUN_INDICATOR" = "N" %THEN 
	%DO;
		PROC SQL;
		   CREATE TABLE WORK.LAST_WEEK_UPDATE AS 
		   SELECT *
		      FROM EADV.DEALSUMMARY T1
		      WHERE T1.DEALDT < DHMS(INTNX('DAY',TODAY(),0,'B'),00,00,00) AND 
				   (T1.UPDATEDT >= &LASTWEEK OR T1.DEALDT >= &LASTWEEK)
		      ORDER BY t1.UPDATEDT DESC;
	%RUNQUIT(&job,&sub1);
	%END;
%ELSE %IF "&RUN_INDICATOR" = "Y" %THEN
	%DO;
		DATA WORK.LAST_WEEK_UPDATE;
			SET EADV.DEALSUMMARY(WHERE=(DEALDT >= &LASTWEEK AND DEALDT < DHMS(TODAY(),00,00,00)));
	%RUNQUIT(&job,&sub1);
	%END;
%MEND;

%INITIAL_PULL()

/* FORMAT AND SAVE LOCAL EADV DATA FOR UPLOAD */

PROC SQL;
   CREATE TABLE DEAL_SUM_INSERT_EADV AS 
   SELECT /* PRODUCT */
            (case
              when t1.PRODUCTCD = "A" or t1.PRODUCTCD = "C" then "PAYDAY" else "INSTALLMENT" end) LABEL="PRODUCT" AS 
            PRODUCT, 
          /* POS */
            ("EADVANTAGE") LABEL="POS" AS POS, 
          /* INSTANCE */
            ("EAPROD1") LABEL="INSTANCE" AS INSTANCE,
		  'STOREFRONT'					AS CHANNELCD, 
          t2.BRND_CD AS BRANDCD, 
          /* BANKMODEL */
            (case
              when t2.ST_PVC_CD ne 'TX' then 'STANDARD' else 'CSO'
            end) LABEL="BANKMODEL" AS BANKMODEL, 
          t2.CTRY_CD AS COUNTRYCD, 
          t2.ST_PVC_CD AS STATE, 
          t2.ADR_CITY_NM AS CITY, 
          t2.MAIL_CD AS ZIP, 
          t2.HIER_ZONE_NBR AS ZONENBR, 
          t2.HIER_ZONE_NM AS ZONENAME, 
          t2.HIER_RGN_NBR AS REGIONNBR, 
          t2.HIER_RDO_NM AS REGIONRDO, 
          t2.HIER_DIV_NBR AS DIVISIONNBR, 
          t2.HIER_DDO_NM AS DIVISIONDDO, 
          /* BUSINESS_UNIT */
            (COMPRESS(PUT(T2.BUSN_UNIT_ID,BEST9.))) AS BUSINESS_UNIT, 
          t1.LOCNBR, 
          t2.OPEN_DT AS LOC_OPEN_DT, 
          t2.CLS_DT AS LOC_CLOSE_DT, 
          /* DEAL_DT */
            DHMS(DATEPART(T1.DEALDT),00,00,00) FORMAT=DATETIME20. LABEL="DEAL_DT" AS DEAL_DT, 
          /* DEAL_DTTM */
            (t1.DEALDT) AS DEAL_DTTM, 
          /* LAST_REPORT_DT */
            (DHMS(INTNX('DAY',TODAY(),-1,'B'),00,00,00)) FORMAT=DATETIME20. AS LAST_REPORT_DT, 
          /* DEALNBR */
            (COMPRESS(PUT(t1.DEALNBR,15.))) AS DEALNBR, 
          /* TITLE_DEALNBR */
            ('0') AS TITLE_DEALNBR, 
          /* CUSTNBR */
            (COMPRESS(PUT(t1.CUSTNBR,15.))) AS CUSTNBR, 
          t3.SSN,
		  ''	AS OMNINBR,
          t1.ADVAMT, 
          t1.FEEAMT,
		  .			AS CUSTOMARYFEE, 
          t1.NSFFEEAMT, 
          t1.LATEFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.WAIVEDFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALCNT AS CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.WRITEOFFDT, 
          t1.DEPOSITDT, 
          t1.RETURNDT AS DEFAULTDT, 
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          /* ACHSTATUSCD */
            (case when t1.CUSTCHECKNBR = "ACH" THEN "Y" ELSE "N" END) AS ACHSTATUSCD, 
          t1.RETURNREASONCD, 
          /* COLLATERAL_TYPE */
            (CASE     WHEN UPCASE(T1.CUSTCHECKNBR)='DC' AND t4.CARDCOLLATERALTYPE = "PC" THEN "PPC"
					  WHEN ANYDIGIT(T1.CUSTCHECKNBR) = 1 THEN "CHECK"
                      WHEN UPCASE(T1.CUSTCHECKNBR) = "ACH" THEN "ACH"
                      WHEN UPCASE(T1.CUSTCHECKNBR) = "NONE" THEN "NONE"
                      WHEN UPCASE(T1.CUSTCHECKNBR) = "DC" THEN "DEBIT CARD"
                      WHEN UPCASE(T1.CUSTCHECKNBR) = "ETF" THEN "ACH"
                      WHEN UPCASE(T1.CUSTCHECKNBR) = "SIG" THEN "NONE"
                      WHEN UPCASE(T1.CUSTCHECKNBR) = "PPC" THEN "PPC"
                      ELSE UPCASE(T1.CUSTCHECKNBR) 
            END) 																		AS COLLATERAL_TYPE,
		  t1.CUSTCHECKNBR,
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT,
		  . AS OUTSTANDING_DRAW_AMT,
		/* 8Jan2018 - Haritha : Added this Flag identifying the loan as Under Collateralized */
		(CASE 
		  	WHEN (DATEPART(T1.DEALDT) >= DATEPART(T5.DEALDATE)  AND T5.DEALDATE ^= .) 
					THEN "Y" 
					ELSE "N" 
			end) as UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM WORK.LAST_WEEK_UPDATE t1
	/* 8Jan2018 - Haritha : The table used below is coded in Step 1 of Daily Summary */
	  LEFT JOIN Skynet.TX_UC_DatebyLoc t5
	  	on t1.LOCNBR = t5.LOCNBR
	  LEFT JOIN EADV.DCZEROAUTHDETAILS t4
	  	ON (t1.DEALNBR = t4.DEALNBR)
	  INNER JOIN EDW.D_LOCATION t2
	  	ON (t1.LOCNBR = t2.LOC_NBR)
	  INNER JOIN EADV.CUSTOMER t3
	  	ON (t1.CUSTNBR = t3.CUSTNBR)
      WHERE  t2.ST_PVC_CD NOT IN 
           (
           'AB',
           'BC',
           'CO',
           'MB',
           'ND',
           'MT',
           'NH',
           'AZ',
           'NM',
           'OR',
           'AR',
           'PA')
		   AND T1.DEALDT >= DHMS(INTNX('YEAR',TODAY(),-6,'B'),00,00,00)
/*      ORDER BY t1.DEALNBR;*/
;
%RUNQUIT(&job,&sub1);

DATA UNION_TABLE;
SET TMP_TBLS.UNION_TABLE;
RUN;

PROC SQL;
	CREATE TABLE WORK.DEAL_SUM_DAILY_UPDATE_PRE AS
	SELECT *
	FROM UNION_TABLE
	UNION ALL CORR
	SELECT *
	FROM DEAL_SUM_INSERT_EADV
;
QUIT;

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
RUN;

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\EADV",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\EADV\DIR2",'G');
%RUNQUIT(&job,&sub1);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_EADV (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE
	WHERE STATE NOT IN ('TN');
%RUNQUIT(&job,&sub1);


/*UPLOAD EADV*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_EADV.SAS";