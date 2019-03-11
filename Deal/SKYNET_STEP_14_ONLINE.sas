%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
LIBNAME AA_LG OLEDB DATASOURCE='RPTDB02.AEAONLINE.NET\AANET' PROVIDER=SQLOLEDB DBMAX_TEXT=32767 
               USER="&USER" PASSWORD=&PASSWORD
               PROPERTIES=("INITIAL CATALOG"=LGV4) SCHEMA=DBO DEFER=YES;

LIBNAME AA_BTAG OLEDB DATASOURCE='RPTDB02.AEAONLINE.NET\AANET' PROVIDER=SQLOLEDB DBMAX_TEXT=32767 
               USER="&USER" PASSWORD=&PASSWORD
               PROPERTIES=("INITIAL CATALOG"=BTAGCOMMON) SCHEMA=DBO DEFER=YES;

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DEAL\DEAL_ERROR_INPUTS.SAS";


LIBNAME BIOR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR DEFER=YES;

DATA _NULL_;
	/*DEAL*/
	CALL SYMPUTX('DEAL_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DEAL_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DEAL",'G');
%RUNQUIT(&job,&sub14);

/*HOW FAR BACK THE DATA GOES IN YEARS*/
%LET YEARS = 6;

LIBNAME AANET ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=AANETSOURCE DEFER=YES;

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
	IF FULL_RUN = 'Y' THEN 
		DO;
			CALL SYMPUTX('LASTWEEK',DHMS(INTNX('YEAR',TODAY(),-6,'B'),00,00,00),'G');
		END;
	ELSE IF FULL_RUN = 'N' THEN 
		DO;
		 	CALL SYMPUTX('LASTWEEK',DHMS(INTNX('DAY',TODAY(),-5,'B'),00,00,00),'G');
		END;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_DEALS AS 
   SELECT T1.LOANID, 
          T1.WEBAPPID, 
          T1.ACCTID, 
          T1.RENEWAL, 
          T1.PRODUCTKITID, 
          T1.APPDATE, 
          T1.ORIGINATORUSERPROFILEID, 
          T1.ORIGINATIONDATE, 
          T1.LOANGUID, 
          T1.FUNDINGDATE, 
          T1.LOANAMT, 
          T1.CUSTTYPEID, 
          T1.REVIEWERUSERPROFILEID, 
          T1.LOANSTATUSID, 
          T1.LOANACCOUNTINGSTATUSID, 
          T1.LEADPROVIDERNAME, 
          T1.TOKENLEVEL, 
          T1.ADDRID, 
          T1.BANKID, 
          T1.CUSTIDENTITYID, 
          T1.EMAILID, 
          T1.EMPID, 
          T1.PAYID, 
          T1.HOMEPHONEID, 
          T1.MOBILEPHONEID, 
          T1.WORKPHONEID, 
          T1.REFID, 
          T1.FOLLOWUPDATE, 
          T1.LOANTYPEID, 
          T1.PYMTPAYFREQID, 
          T1.LEADPROVIDERID, 
          T1.ISMANUALFUNDING, 
          T1.CAMPAIGN, 
          T1.SEGMENTID, 
          T1.DENIALREASONID, 
          T1.DENIALBYUSERPROFILEID, 
          T1.CORPORATIONID, 
          T1.COMPANYID, 
          T1.PRODUCTID, 
          T1.LOANOWNERID, 
          T1.ACHSPLITTYPEID, 
          T1.VERITECSTATUSID, 
          T1.CREDITCARDID, 
          T1.PYMTTYPEID, 
          T1.ISORGANICLEAD, 
          T1.LOANMASTERID, 
          T1.ROLLOVERNBR, 
          T1.ISKSL, 
          T1.CONFIGSCOREMODELSETID, 
          T1.DATEMODIFIED, 
          /* BEGINDT */
            (DATEPART(&LASTWEEK)) FORMAT=MMDDYY10. AS BEGINDT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'B')) FORMAT=MMDDYY10. AS ENDDT, 
          /* DEAL_DT */
            (DATEPART(CASE WHEN T1.ORIGINATIONDATE = . THEN DHMS(INPUT(T1.FUNDINGDATE,YYMMDD10.),00,00,00) ELSE 
            T1.ORIGINATIONDATE END)) FORMAT=MMDDYY10. AS DEAL_DT
      FROM AA_LG.LOAN T1
      WHERE T1.DATEMODIFIED >= &LASTWEEK;
%RUNQUIT(&job,&sub14);

PROC SORT DATA=AA_LG.LOANRENEWAL OUT=DUEDT;
BY LOANID RENEWAL;
%RUNQUIT(&job,&sub14);

DATA LASTDUEDT;
SET DUEDT;
BY LOANID RENEWAL;
IF LAST.LOANID;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_BASE_METRICS AS 
   SELECT /* PRODUCT */
            (CASE WHEN t4.LoanTypeCode = "PD" THEN "PAYDAY" 
                      WHEN t4.LoanTypeCode = "INS" THEN "INSTALLMENT" ELSE "" END
            ) AS PRODUCT, 
          /* POS */
            ("ONLINE") AS POS, 
          /* INSTANCE */
            ("AANET") AS INSTANCE, 
          t1.CompanyID AS LOCNBR, 
          t6.LicensedStateProvinceCode LENGTH=3 AS STATE, 
          t1.DEAL_DT, 
          /* DEAL_DTTM */
            (case when t1.OriginationDate = . then DHMS(input(t1.FundingDate,YYMMDD10.),00,00,00) else 
            t1.ORIGINATIONDATE end) FORMAT=DATETIME20. AS DEAL_DTTM, 
		  DHMS(INPUT(T7.DUEDATE,YYMMDD10.),0,0,0) FORMAT=DATETIME20. AS DUEDT,
          t1.LoanID AS DEALNBR, 
          t1.LoanAmt AS ADVAMT, 
          t2.CustAcctNbr AS CUSTNBR, 
		  t1.LoanAccountingStatusID, 
          t3.SSN, 
          /* PRODUCT_TYPE */
            (CASE WHEN t4.LoanTypeCode = "PD" THEN "PDL" 
                      WHEN t4.LoanTypeCode = "INS" THEN "ILP" ELSE "" END
            ) AS PRODUCT_TYPE, 
          t5.LoanStatusName AS DEALSTATUSCD
      FROM WORK.ONLINE_DEALS t1
           LEFT JOIN AA_LG.Acct t2 ON (t1.AcctID = t2.AcctID)
           LEFT JOIN AA_LG.CustIdentity t3 ON (t1.CustIdentityID = t3.CustIdentityID)
           LEFT JOIN AA_LG.LoanType t4 ON (t1.LoanTypeID = t4.LoanTypeID)
           LEFT JOIN AA_LG.LoanStatus t5 ON (t1.LoanStatusID = t5.LoanStatusID)
           LEFT JOIN AA_BTAG.Company t6 ON (t1.CompanyID = t6.CompanyID)
		   LEFT JOIN LASTDUEDT t7 on (t1.LoanID=t7.LoanID)
      WHERE t5.LoanStatusName NOT = 'Withdrawn' AND t4.LOANTYPEID NOT = 3;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ADV_AMT AS 
   SELECT T1.LOANID, 
          T1.EFFECTIVEDATE,
	  SUM(T1.PRINCIPAL) AS PRINCIPAL 
      FROM AA_LG.VW_TRN T1
      WHERE (UPCASE(T1.TRNTYPENAME)) IN 
           (
           "FUNDING",
           "REFI TRANSFER"
           ) AND (UPCASE(T1.TRNDIRECTIONNAME)) = "DEBIT"
      GROUP BY T1.LOANID,
               T1.EFFECTIVEDATE
      ORDER BY T1.LOANID,
               T1.EFFECTIVEDATE;
%RUNQUIT(&job,&sub14);

DATA ADVAMT_FINAL;
	SET WORK.ADV_AMT;
	BY LoanID;
	IF FIRST.LOANID;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.LAST_TRAN_PRE AS 
   SELECT T1.LOANID, 
          T1.TRNID, 
          T1.EFFECTIVEDATE, 
          /* TRAN_DT */
            (DHMS(INPUT(T1.EFFECTIVEDATE,YYMMDD10.),00,00,00)) FORMAT=DATETIME20. AS TRAN_DT
      FROM AA_LG.VW_TRN T1
      ORDER BY T1.LOANID,
               T1.TRNID;
%RUNQUIT(&job,&sub14);

DATA LAST_TRAN;
	SET LAST_TRAN_PRE;
	BY LoanID TrnID;
	IF LAST.LOANID;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.CHARGED AS 
   SELECT T1.LOANID, 
          /* FEES_CHARGED */
            (SUM(CASE WHEN ((UPCASE(T1.TRNTYPENAME) = "FULLY EARNED FINANCE CHARGE") OR (UPCASE(T1.TRNTYPENAME) = 
            "DATABASE VERIFICATION FEE")) AND UPCASE(T1.TRNDIRECTIONNAME) = "DEBIT" THEN T1.FEE ELSE 0 END)) AS 
            FEES_CHARGED, 
          /* ADVAMT */
            (SUM(CASE WHEN (
                      UPCASE(T1.TRNTYPENAME) = "FUNDING" OR
                      UPCASE(T1.TRNTYPENAME) = "REFI TRANSFER" 
                                  )
                      AND UPCASE(T1.TRNDIRECTIONNAME) = "DEBIT"
                      THEN T1.PRINCIPAL 
                      ELSE 0 
            END)) AS ADVAMT, 
          /* INTEREST_CHARGED */
            (SUM(CASE WHEN UPCASE(T1.TRNDIRECTIONNAME) = "DEBIT" THEN T1.INTEREST ELSE 0 END)) AS INTEREST_CHARGED, 
          /* NSFFEEAMT */
            (SUM(CASE WHEN UPCASE(T1.TRNTYPENAME) = "NSF FEE" AND UPCASE(T1.TRNDIRECTIONNAME) = "DEBIT" THEN T1.FEE 
            ELSE 0 END)) AS NSFFEEAMT, 
          /* FEE_ADJ */
            (SUM(CASE WHEN UPCASE(T1.TRNTYPENAME) = "ADJUSTMENT" THEN T1.FEE ELSE 0 END)) AS FEE_ADJ, 
          /* NSFFEE */
            (SUM(CASE WHEN UPCASE(T1.TRNTYPENAME) = "NSF FEE" THEN T1.FEE ELSE 0 END)) AS NSFFEE, 
          /* CSO_FEE */
            (SUM(CASE WHEN UPCASE(T1.TRNTYPENAME) = "CSO FEE" AND UPCASE(T1.TRNDIRECTIONNAME) = "DEBIT" THEN 
            T1.PRINCIPAL ELSE 0 END)) AS CSO_FEE
      FROM AA_LG.VW_TRN T1
      GROUP BY T1.LOANID;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.MONETARY_ADDITIONS AS 
   SELECT T1.PRODUCT, 
          T1.POS, 
          T1.INSTANCE, 
          T1.LOCNBR, 
          T1.STATE, 
          T1.DEAL_DTTM, 
          T1.DEAL_DT,
		  T1.DUEDT, 
          T1.DEALNBR, 
          T1.CUSTNBR, 
          T1.SSN, 
/*		  T1.ADVAMT,*/
          T4.PRINCIPAL AS ADVAMT, 
          /* FEES_CHARGED */
            (SUM(T2.FEES_CHARGED,T2.CSO_FEE)) AS FEES_CHARGED, 
          T2.NSFFEE AS NSFFEEAMT, 
          T2.INTEREST_CHARGED, 
          T3.TOTAL AS TOTALOWED, 
          T1.PRODUCT_TYPE, 
          T1.DEALSTATUSCD,
		  T1.LOANACCOUNTINGSTATUSID
      FROM WORK.ONLINE_BASE_METRICS T1
           INNER JOIN AA_LG.VW_LOANBALANCE T3 ON (T1.DEALNBR = T3.LOANID)
           INNER JOIN WORK.CHARGED T2 ON (T1.DEALNBR = T2.LOANID)
           LEFT JOIN WORK.ADVAMT_FINAL T4 ON (T1.DEALNBR = T4.LOANID);
%RUNQUIT(&job,&sub14);

PROC SQL;
	CREATE TABLE TOTAL_PAID AS
		SELECT LOANID
			  ,-ROUND(SUM(TOTAL),.01) AS TOTAL_PAID
		FROM AA_LG.VW_TRN
	WHERE (UPCASE(TRNTYPENAME) CONTAINS "PAYMENT" AND UPCASE(TRNTYPENAME) NOT CONTAINS "FEE") OR
		  (TRNTYPEID IN(7,6,13))
	GROUP BY LOANID
;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.LOAN_EVENT_DATES AS 
   SELECT t1.LoanID, 
          t2.LoanAccountingStatusID, 
          t2.LoanAccountingStatusName, 
          /* EFFECTIVE_DATE */
            (DHMS(input(t1.EffectiveDate,YYMMDD10.),00,00,00)) FORMAT=DATETIME20. AS EFFECTIVE_DATE
      FROM AA_LG.LoanAccountingStatusLog t1
           INNER JOIN AA_LG.LoanAccountingStatus t2 ON (t1.ToLoanAccountingStatusID = t2.LoanAccountingStatusID);
%RUNQUIT(&job,&sub14);

DATA COMPLIANT_PRE
	 CHARGE_OFF_PRE
	 DEFAULT_PRE;
	 SET WORK.LOAN_EVENT_DATES;
	 IF LOANACCOUNTINGSTATUSID = 1 THEN OUTPUT COMPLIANT_PRE;
	 ELSE IF LOANACCOUNTINGSTATUSID = 2 THEN OUTPUT CHARGE_OFF_PRE;
	 ELSE IF LOANACCOUNTINGSTATUSID = 3 THEN OUTPUT DEFAULT_PRE;
%RUNQUIT(&job,&sub14);

/* -------------------------------------------------------------------
   Run the SORT procedure
   ------------------------------------------------------------------- */
PROC SORT DATA=WORK.DEFAULT_PRE(FIRSTOBS=1 )
	OUT=WORK.DEFAULT(LABEL="Sorted WORK.DEFAULT_PRE")
	NODUPKEY
	;
	BY LoanID;

%RUNQUIT(&job,&sub14);

PROC SORT DATA=WORK.CHARGE_OFF_PRE
	OUT=WORK.CHARGE_OFF(LABEL="Sorted WORK.CHARGE_OFF_PRE")
	NODUPKEY
	;
	BY LoanID;

%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_DEALSUMMARY_PRE AS 
   SELECT DISTINCT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.LOCNBR, 
          t1.STATE, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          t1.CUSTNBR, 
          t1.SSN, 
          t1.ADVAMT, 
          t1.FEES_CHARGED AS FEEAMT, 
          /* LATEFEEAMT */
            (.) AS LATEFEEAMT, 
          t1.NSFFEEAMT, 
          /* OTHERFEEAMT */
            (.) AS OTHERFEEAMT, 
          /* REBATEAMT */
            (.) AS REBATEAMT, 
          /* COUPONAMT */
            (.) AS COUPONAMT, 
          t1.INTEREST_CHARGED AS INTERESTFEE, 
          /* TOTALPAID */
            (CASE 
               WHEN . = t6.TOTAL_PAID THEN 0
               ELSE t6.TOTAL_PAID END) AS TOTALPAID, 
          t1.TOTALOWED AS TOTALOWED, 
          /* CONSECUTIVEDEALFLG */
            (.) AS CONSECUTIVEDEALFLG, 
          /* CASHAGNCNT */
            (.) AS CASHAGNCNT, 
          /* DUEDT */
          T1.DUEDT FORMAT=DATETIME20. AS DUEDT, 
          /* DEALENDDT */
            (CASE WHEN t1.TOTALOWED = 0 THEN t5.TRAN_DT ELSE . END
                 ) FORMAT=DATETIME20. AS DEALENDDT, 
          t4.EFFECTIVE_DATE AS WRITEOFFDT, 
          t3.EFFECTIVE_DATE AS DEFAULTDT, 
          /* ACHSTATUSCD */
            ('') AS ACHSTATUSCD, 
          /* CHECKSTATUSCD */
            ("") AS CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          /* COLLATERAL_TYPE */
            ("") AS COLLATERAL_TYPE, 
          /* ETLDT */
            (''DT) FORMAT=DATETIME20. AS ETLDT, 
          t1.PRODUCT_TYPE AS PRODUCTCD, 
          /* PREVDEALNBR */
            (.) AS PREVDEALNBR, 
          /* ACHAUTHFLG */
            ("") AS ACHAUTHFLG, 
          /* UPDATEDT */
            (''DT) FORMAT=DATETIME20. AS UPDATEDT,
			T1.LOANACCOUNTINGSTATUSID
      FROM WORK.MONETARY_ADDITIONS t1
           LEFT JOIN WORK.DEFAULT t3 ON (t1.DEALNBR = t3.LoanID)
           LEFT JOIN WORK.CHARGE_OFF t4 ON (t1.DEALNBR = t4.LoanID)
           LEFT JOIN WORK.LAST_TRAN t5 ON (t1.DEALNBR = t5.LoanID)
           LEFT JOIN WORK.TOTAL_PAID t6 ON (t1.DEALNBR = t6.LoanID);
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE ONLINE_DAILY_UPDATE AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          /* BRANDCD */
            ('AA') AS BRANDCD, 
          /* COUNTRYCD */
            ('USA') AS COUNTRYCD, 
          t1.STATE, 
          /* CITY */
            ('') AS CITY, 
          /* ZIP */
            ('') AS ZIP, 
          /* ZONENBR */
            (0) AS ZONENBR, 
          /* ZONENAME */
            ('') AS ZONENAME, 
          /* REGIONNBR */
            (0) AS REGIONNBR, 
          /* REGIONRDO */
            ('') AS REGIONRDO, 
          /* DIVISIONNBR */
            (0) AS DIVISIONNBR, 
          /* DIVISIONDDO */
            ('') AS DIVISIONDDO, 
          /* LOC_OPEN_DT */
            (''DT) FORMAT=DATETIME20. AS LOC_OPEN_DT, 
          /* LOC_CLOSE_DT */
            (''DT) FORMAT=DATETIME20. AS LOC_CLOSE_DT, 
          /* BUSINESS_UNIT */
            (CASE WHEN t2.BUSINESS_UNIT = . THEN 0 ELSE t2.BUSINESS_UNIT END) AS BUSINESS_UNIT, 
          t1.LOCNBR, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          t1.DEALNBR, 
          /* TITLE_DEALNBR */
            . AS TITLE_DEALNBR, 
          t1.CUSTNBR, 
          t1.SSN, 
          t1.ADVAMT LABEL='', 
          t1.FEEAMT, 
          t1.LATEFEEAMT, 
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.INTERESTFEE, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
          t1.CHECKSTATUSCD, 
          /* DEALSTATUSCD */
            (CASE WHEN T1.LOANACCOUNTINGSTATUSID=2 THEN 'WO'
				  WHEN t1.DEALSTATUSCD IN('Adjusted In Full','Paid in Full','Settled In Full - DNL','Refinanced') THEN 
            'CLO'
                       WHEN t1.DEALSTATUSCD IN('Collections', 'Open', 'Past Due', 'Pending NC Refi Out', 'Right To Cure'
            , 'Workout') THEN 'OPN'
                       WHEN t1.DEALSTATUSCD = 'Fraud Loan' AND t1.TOTALOWED = 0 THEN 'CLO'
                       WHEN t1.DEALSTATUSCD = 'Fraud Loan' AND t1.TOTALOWED ^= 0 THEN 'OPN'
                       WHEN t1.DEALSTATUSCD IN('Rescinded', 'Void') THEN 'V'
                       WHEN t1.DEALSTATUSCD = 'Write Off' THEN 'WO'
                       WHEN t1.DEALSTATUSCD = 'Bankruptcy' THEN 'WOB'
            ELSE '' 
            END) AS DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
          t1.ETLDT, 
          t1.PRODUCTCD, 
          t1.PREVDEALNBR, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT, 
          /* BEGINDT */
            (INTNX('YEAR',TODAY(),-&YEARS,'B')) FORMAT=MMDDYY10. AS BEGINDT, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'B')) FORMAT=MMDDYY10. AS ENDDT
      FROM WORK.ONLINE_DEALSUMMARY_PRE t1
           LEFT JOIN BIOR.ONLINE_BU t2 ON (t1.STATE = t2.STATE);
/*      WHERE t1.DEAL_DT BETWEEN (CALCULATED BEGINDT) AND (CALCULATED ENDDT);*/
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.DEAL_SUMMARY_TMP AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          /* BUSINESS_UNIT */
            (COMPRESS(PUT(T1.BUSINESS_UNIT,BEST9.))) AS BUSINESS_UNIT, 
          t1.LOCNBR, 
          /* DEAL_DT */
            (dhms(t1.DEAL_DT,00,00,00)) FORMAT=DATETIME20. AS DEAL_DT, 
          t1.DEAL_DTTM, 
          /* DEALNBR */
            (COMPRESS(PUT(DEALNBR,30.))) AS DEALNBR, 
          t1.TITLE_DEALNBR, 
          t1.CUSTNBR, 
          t1.SSN, 
          t1.ADVAMT, 
          t1.FEEAMT, 
          t1.LATEFEEAMT, 
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.INTERESTFEE, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD, 
		  '' AS RETURNREASONCD LENGTH=5 FORMAT=$5.,
          t1.CHECKSTATUSCD, 
          t1.DEALSTATUSCD, 
          t1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          t1.ETLDT, 
          t1.PRODUCTCD, 
          t1.PREVDEALNBR, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT,
		  . AS OUTSTANDING_DRAW_AMT,
		  '' AS UNDER_COLLATERALIZED LENGTH=1 FORMAT=$1.
      FROM ONLINE_DAILY_UPDATE t1;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.DEAL_SUM_DAILY_UPDATE_PRE AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE,
		  'ONLINE'					AS CHANNELCD,  
          t1.BRANDCD, 
          ''					AS BANKMODEL, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          /* BUSINESS_UNIT */
          BUSINESS_UNIT, 
          t1.LOCNBR, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          /* DEAL_DT */
          DEAL_DT, 
          t1.DEAL_DTTM, 
          /* LAST_REPORT_DT */
            (dhms(today()-1,0,0,0)) FORMAT=datetime20. LABEL="LAST_REPORT_DT" AS LAST_REPORT_DT, 
          /* DEALNBR */
            DEALNBR, 
          /* TITLE_DEALNBR */
            (COMPRESS(PUT((CASE 
               WHEN . = t1.TITLE_DEALNBR THEN 0
               ELSE t1.TITLE_DEALNBR
            END),30.))) AS TITLE_DEALNBR, 
          /* CUSTNBR */
          CUSTNBR, 
          t1.SSN,
		  ''	AS OMNINBR, 
          t1.ADVAMT, 
          t1.FEEAMT,
		  .			AS CUSTOMARYFEE,  
          t1.NSFFEEAMT, 
          t1.OTHERFEEAMT, 
          t1.LATEFEEAMT, 
          .								AS WAIVEDFEEAMT, 
          t1.REBATEAMT, 
          t1.COUPONAMT, 
          t1.TOTALPAID, 
          t1.TOTALOWED, 
          t1.CONSECUTIVEDEALFLG, 
          t1.CASHAGNCNT, 
          t1.DUEDT, 
          t1.DEALENDDT, 
          ' 'DT						FORMAT=DATETIME20.		AS DEPOSITDT, 
          t1.WRITEOFFDT, 
          t1.DEFAULTDT, 
          t1.ACHSTATUSCD,
		  '' AS RETURNREASONCD LENGTH=5 FORMAT=$5.,
          t1.DEALSTATUSCD, 
          t1.CHECKSTATUSCD, 
          t1.COLLATERAL_TYPE, 
		  '' AS CUSTCHECKNBR LENGTH=15 FORMAT=$15.,
          t1.ETLDT, 
          t1.PREVDEALNBR, 
          t1.PRODUCTCD, 
          t1.INTERESTFEE, 
          t1.ACHAUTHFLG, 
          t1.UPDATEDT,
		  T1.OUTSTANDING_DRAW_AMT,
		  t1.UNDER_COLLATERALIZED
      FROM WORK.DEAL_SUMMARY_TMP t1;
%RUNQUIT(&job,&sub14);

DATA UNION_TABLE;
SET TMP_TBLS.UNION_TABLE;
RUN;

PROC SQL;
	CREATE TABLE WORK.DEAL_SUM_DAILY_UPDATE_PRE AS
	SELECT *
	FROM UNION_TABLE
	UNION ALL CORR
	SELECT *
	FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE
;
QUIT;

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub14);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;

/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\ONLINE",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DEAL\ONLINE\DIR2\",'G');
%RUNQUIT(&job,&sub14);

PROC SQL;
    INSERT INTO SKY.DEALSUM_DATAMART_OL (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM WORK.DEAL_SUM_DAILY_UPDATE_PRE;
%RUNQUIT(&job,&sub14);

/*UPLOAD ONLINE*/
%INCLUDE "&DEAL_FILE_PATH.\DEALSUM_UPLOAD_ONLINE.SAS";

