%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";

LIBNAME AA_LG OLEDB DATASOURCE='RPTDB02.AEAONLINE.NET\AANET' PROVIDER=SQLOLEDB DBMAX_TEXT=32767 
               USER="&USER" PASSWORD=&PASSWORD
               PROPERTIES=("INITIAL CATALOG"=LGV4) SCHEMA=DBO DEFER=YES;

LIBNAME AA_BTAG OLEDB DATASOURCE='RPTDB02.AEAONLINE.NET\AANET' PROVIDER=SQLOLEDB DBMAX_TEXT=32767 
               USER="&USER" PASSWORD=&PASSWORD
               PROPERTIES=("INITIAL CATALOG"=BTAGCOMMON) SCHEMA=DBO DEFER=YES;

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";

LIBNAME BIOR ORACLE
	USER=&USER
	PW=&PASSWORD
	SCHEMA=BIOR
	PATH=BIOR DEFER=YES;

LIBNAME AA_PD OLEDB DATASOURCE='RPTDB02.AEAONLINE.NET\AANET' PROVIDER=SQLOLEDB DBMAX_TEXT=32767 
              USER="&USER" PASSWORD=&PASSWORD
              PROPERTIES=("INITIAL CATALOG"=PRIMEDECK) SCHEMA=DBO;

DATA _NULL_;
	CALL SYMPUTX('DAILY_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DAILY_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DAILY",'G');
	CALL SYMPUTX('WEEKDAY_TODAY',WEEKDAY(DATE()),'G');
%RUNQUIT(&job,&sub14);

%PUT &WEEKDAY_TODAY;

/*CHECK TO SEE IF DATA IN TABLE IS READY*/

%MACRO CHECKFORDATA();

	%DO %UNTIL (%EVAL(&FINISHED_DATA. = 1));
		
		PROC SQL;
			CREATE TABLE DATA_CHECK_ONLINE AS
			SELECT MAX(DATEPART(DHMS(INPUT(T1.DATECREATED,YYMMDD10.),00,00,00))) FORMAT MMDDYY10. AS BUSINESSDT
				  ,PRODUCTNAME
				  ,STATE
			FROM AA_PD.LOANRECEIVABLEBYSTATE T1
			GROUP BY PRODUCTNAME
					,STATE
		;
		QUIT;

		PROC SQL;
			SELECT COUNT(*) INTO: REC_COUNT
			FROM DATA_CHECK_ONLINE
		;
		QUIT;

		%PUT REC_COUNT;

		DATA CHECK;
			SET DATA_CHECK_ONLINE;
			IF BUSINESSDT >= TODAY()-1 AND WEEKDAY(DATE()) ^= 7 AND HOUR(DATETIME()) >= 7 AND MINUTE(DATETIME()) >= 30 THEN 
				DO;
					GOOD_TO_GO = 1;
					CALL SYMPUTX("FINISHED_DATA",1,'G');
				END;
			ELSE IF BUSINESSDT ^= TODAY()-1 AND WEEKDAY(DATE()) ^= 7 THEN
				DO;
					GOOD_TO_GO = 0;
					CALL SYMPUTX("FINISHED_DATA",0,'G');
				END;
			ELSE IF BUSINESSDT ^= TODAY()-1 AND WEEKDAY(DATE()) = 7 AND HOUR(DATETIME()) >= 7 AND MINUTE(DATETIME()) >= 30 THEN
				DO;
					GOOD_TO_GO = 1;
					CALL SYMPUTX("FINISHED_DATA",1,'G');
				END;
			ELSE
				DO;
					CALL SYMPUTX("FINISHED_DATA",0,'G');
				END;
		RUN;

		%IF %EVAL(&FINISHED_DATA. ^= 1) %THEN 
			%DO;
				/*SLEEPS FOR 300 SECONDS (5 MINUTES)*/
				DATA SLEEP;
					CALL SLEEP(300,1);
				RUN;
			%END;
	%END;

%MEND;

%CHECKFORDATA
		
	
PROC SQL;
   CREATE TABLE WORK.RECV AS 
   SELECT t1.State AS STATE, 
          t2.CompanyID AS LOCNBR, 
          /* PRODUCT */
            (UPCASE(t1.ProductName)) AS PRODUCT, 
          /* BUSINESSDT */
            (DHMS(input(t1.DateCreated,YYMMDD10.),00,00,00)) FORMAT=DATETIME20. AS BUSINESSDT, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (SUM(CASE WHEN UPCASE( t1.LoanAccountingStatusName) = 'COMPLIANT' THEN t1.LoanCount ELSE 0 END)) AS 
            COMPLIANT_LOANS_OUTSTANDING, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (SUM(CASE WHEN UPCASE(t1.LoanAccountingStatusName) = 'DEFAULT' THEN t1.LoanCount ELSE 0 END)) AS 
            DEFAULT_LOANS_OUTSTANDING, 
          /* TOTADVRECV */
            (SUM(CASE WHEN UPCASE( t1.LoanAccountingStatusName) = 'COMPLIANT' THEN t1.Principal ELSE 0 END)) AS 
            TOTADVRECV, 
          /* TOTDEFAULTRECV */
            (SUM(CASE WHEN UPCASE( t1.LoanAccountingStatusName) = 'DEFAULT' THEN t1.Principal ELSE 0 END)) AS 
            TOTDEFAULTRECV
      FROM AA_PD.LoanReceivableByState t1
           LEFT JOIN AA_BTAG.Company t2 ON (t1.State = t2.LicensedStateProvinceCode)
      WHERE (CALCULATED PRODUCT) NOT = 'LINEOFCREDIT'
      GROUP BY t1.State,
               t2.CompanyID,
               (CALCULATED PRODUCT),
               (CALCULATED BUSINESSDT)
      ORDER BY t1.State,
               BUSINESSDT;
%RUNQUIT(&job,&sub14);

PROC SQL;
	CREATE TABLE ACCT_REV AS
		SELECT  
			CASE WHEN GE.AcctNbr IN ('4075', '5404')
					THEN 'PAYDAY'
				 WHEN GE.AcctNbr IN ('4052', '5428')
					THEN 'INSTALLMENT'
			END AS PRODUCT,
			CASE WHEN GE.AcctNbr IN ('4075', '4052')
					THEN 'GROSSREVENUE'
				 WHEN GE.AcctNbr IN ('5404', '5428')
					THEN 'BADDEBT'
			END AS ACCT,
			GE.EffectiveDate,
			L.CompanyID AS LOCNBR,
		    C.CompanyCode AS STATE,
			SUM(amt) AS AMOUNT
	FROM AA_LG.GeneralLedgerExtract GE LEFT JOIN
		 AA_LG.Loan L ON L.LoanID = GE.LoanID LEFT JOIN
		 AA_BTAG.Company C ON C.CompanyID = L.CompanyID
	WHERE AcctNbr IN ('4075','4052','5404','5428')
	GROUP BY CALCULATED PRODUCT, CALCULATED ACCT, GE.EffectiveDate, 
		     C.CompanyCode, L.CompanyID
;
%RUNQUIT(&job,&sub14);

PROC SQL;
	CREATE TABLE REVENUE AS
		SELECT UPCASE(PRODUCT) AS PRODUCT,
			   DHMS(input(EffectiveDate,YYMMDD10.),00,00,00) AS BUSINESSDT FORMAT DATETIME20.,
			   LOCNBR,
			   STATE,
			   SUM(CASE WHEN ACCT = 'GROSSREVENUE' then AMOUNT*-1 ELSE 0 END) AS GROSS_REVENUE,
			   SUM(CASE WHEN ACCT = 'BADDEBT' THEN AMOUNT ELSE 0 END) AS WOAMTSUM
		FROM ACCT_REV
		GROUP BY CALCULATED PRODUCT, CALCULATED BUSINESSDT, STATE, LOCNBR
		ORDER BY STATE, LOCNBR, CALCULATED BUSINESSDT, PRODUCT;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.COMB_METRICS AS 
   SELECT /* PRODUCT */
            (CASE 
               WHEN '' = t1.PRODUCT THEN t2.PRODUCT
               ELSE t1.PRODUCT
            END) AS PRODUCT, 
          /* POS */
            ("ONLINE") AS POS, 
          /* INSTANCE */
            ("AANET") AS INSTANCE, 
          /* BRANDCD */
            ("AA") AS BRANDCD, 
          /* BANKMODEL */
            ("STANDARD") AS BANKMODEL, 
          /* COUNTRYCD */
            ("USA") AS COUNTRYCD, 
          /* STATE */
            (CASE 
               WHEN '' = t1.STATE THEN t2.STATE
               ELSE t1.STATE
            END) FORMAT=$20. AS STATE, 
          /* CITY */
            ("") AS CITY, 
          /* ZIP */
            ("") AS ZIP, 
          /* ZONENBR */
            (0) AS ZONENBR, 
          /* ZONENAME */
            ("ONLINE") AS ZONENAME, 
          /* REGIONNBR */
            (0) AS REGIONNBR, 
          /* REGIONRDO */
            ("ONLINE") AS REGIONRDO, 
          /* DIVISIONNBR */
            (0) AS DIVISIONNBR, 
          /* DIVISIONDDO */
            ("ONLINE") AS DIVISIONDDO, 
          /* LOCNBR */
            (CASE 
               WHEN . = t1.LOCNBR THEN T2.LOCNBR
               ELSE t1.LOCNBR
            END) FORMAT=6. AS LOCNBR, 
          /* BUSINESSDT */
            (CASE 
               WHEN . = t1.BUSINESSDT THEN t2.BUSINESSDT
               ELSE t1.BUSINESSDT
            END) FORMAT=DATETIME20. AS BUSINESSDT, 
          t2.TOTADVRECV, 
          t2.COMPLIANT_LOANS_OUTSTANDING, 
          t2.TOTDEFAULTRECV, 
          t2.DEFAULT_LOANS_OUTSTANDING, 
          t1.GROSS_REVENUE, 
          t1.WOAMTSUM
      FROM WORK.REVENUE t1
           FULL JOIN WORK.RECV t2 ON (t1.STATE = t2.STATE) AND (t1.PRODUCT = t2.PRODUCT) AND (t1.BUSINESSDT = 
          t2.BUSINESSDT)
      ORDER BY STATE,
               BUSINESSDT;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_DEALS AS 
   SELECT t1.LoanID, 
          t1.WebAppID, 
          t1.AcctID, 
          t1.Renewal, 
          t1.ProductKitID, 
          t1.AppDate, 
          t1.OriginatorUserProfileID, 
          t1.OriginationDate, 
          t1.LoanGUID, 
          t1.FundingDate, 
          t1.LoanAmt, 
          t1.CustTypeID, 
          t1.ReviewerUserProfileID, 
          t1.LoanStatusID, 
          t1.LoanAccountingStatusID, 
          t1.LeadProviderName, 
          t1.TokenLevel, 
          t1.AddrID, 
          t1.BankID, 
          t1.CustIdentityID, 
          t1.EmailID, 
          t1.EmpID, 
          t1.PayID, 
          t1.HomePhoneID, 
          t1.MobilePhoneID, 
          t1.WorkPhoneID, 
          t1.RefID, 
          t1.FollowUpDate, 
          t1.LastLoanStatusChangeByUserProfil, 
          t1.LoanTypeID, 
          t1.PymtPayFreqID, 
          t1.LeadProviderID, 
          t1.Campaign, 
          t1.IsManualFunding, 
          t1.SegmentID, 
          t1.DenialReasonID, 
          t1.DenialByUserProfileID, 
          t1.CorporationID, 
          t1.CompanyID, 
          t1.ProductID, 
          t1.LoanOwnerID, 
          t1.ACHSplitTypeID, 
          t1.VeritecStatusID, 
          t1.CreditCardID, 
          t1.PymtTypeID, 
          t1.IsOrganicLead, 
          t1.LoanMasterID, 
          t1.RolloverNbr, 
          t1.IsKSL, 
          t1.ConfigScoreModelSetID, 
          t1.AppEffectiveDate, 
          /* ENDDT */
            (INTNX('DAY',TODAY(),-1,'B')) FORMAT=MMDDYY10. AS ENDDT, 
          /* BEGINDT */
            (INTNX('MONTH',TODAY(),-24,'B')) FORMAT=MMDDYY10. AS BEGINDT, 
          /* DEAL_DT */
/*            (DATEPART(t1.OriginationDate)) FORMAT=MMDDYY10. AS DEAL_DT*/
		    (case when t1.OriginationDate = .
/*			      then DHMS(input(t1.FundingDate,YYMMDD10.),00,00,00)*/
			      then input(t1.FundingDate,YYMMDD10.)
			 else datepart(t1.ORIGINATIONDATE) 
			      end) FORMAT=MMDDYY10. AS DEAL_DT
      FROM AA_LG.Loan t1
      WHERE (CALCULATED DEAL_DT) BETWEEN (CALCULATED BEGINDT) AND (CALCULATED ENDDT);
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.CHARGED AS 
   SELECT t1.LoanID, 
          /* FEES_CHARGED */
            (SUM(CASE WHEN ((UPCASE(t1.TrnTypeName) = "FULLY EARNED FINANCE CHARGE") OR 
                                   (UPCASE(t1.TrnTypeName) = "DATABASE VERIFICATION FEE")) 
                                  AND UPCASE(t1.TrnDirectionName) = "DEBIT" THEN t1.Fee 
                      WHEN UPCASE(t1.TrnTypeName) = "CSO FEE" AND UPCASE(t1.TrnDirectionName) = "DEBIT" THEN 
            t1.Principal
            ELSE 0 END)) AS FEES_CHARGED, 
          /* ADVAMT */
/*            (SUM(CASE WHEN UPCASE(t1.TrnTypeName) = "FUNDING" THEN t1.Principal ELSE 0 END)) AS ADVAMT, */
/*            (SUM(CASE WHEN (UPCASE(t1.TrnTypeName) in ( "FUNDING"*/
/*													   ,"REFI TRANSFER"*/
/*												      )) and UPCASE(t1.TrnDirectionName) = "DEBIT" */
/*													  THEN t1.Principal ELSE 0 END)) AS ADVAMT, */
          /* INTEREST_CHARGED */
            (SUM(CASE WHEN UPCASE(t1.TrnDirectionName) = "DEBIT" THEN t1.Interest ELSE 0 END)) AS INTEREST_CHARGED, 
          /* NSFFEEAMT */
            (SUM(CASE WHEN UPCASE(t1.TrnTypeName) = "NSF FEE" AND UPCASE(t1.TrnDirectionName) = "DEBIT" THEN t1.Fee 
            ELSE 0 END)) AS NSFFEEAMT, 
          /* FEE_ADJ */
            (SUM(CASE WHEN UPCASE(t1.TrnTypeName) = "ADJUSTMENT" THEN t1.Fee ELSE 0 END)) AS FEE_ADJ, 
          /* NSFFEE */
            (SUM(CASE WHEN UPCASE(t1.TrnTypeName) = "NSF FEE" THEN t1.Fee ELSE 0 END)) AS NSFFEE
      FROM AA_LG.VW_Trn t1
      GROUP BY t1.LoanID;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ADV_AMT_1 AS 
   SELECT t1.LoanID, 
          t1.Principal, 
          t1.EffectiveDate, 
          t1.TrnTypeName, 
          t1.TrnDirectionName, 
          /* TRANCD */
            (UPCASE(T1.TrnTypeName)) AS TRANCD, 
          /* UPDIR */
            (UPCASE(t1.TrnDirectionName)) AS UPDIR
      FROM AA_LG.VW_Trn t1
      WHERE (CALCULATED TRANCD) IN 
           (
           "FUNDING",
           "REFI TRANSFER"
           ) AND (CALCULATED UPDIR) = "DEBIT"
      ORDER BY t1.LoanID,
               t1.EffectiveDate;
%RUNQUIT(&job,&sub14);

proc sql;
	create table ADV_AMT_2 as
		select t1.loanid
			   ,t1.effectivedate
			   ,sum(t1.principal) as ADVAMT
		from ADV_AMT_1 t1
		group by t1.loanid
			   ,t1.effectivedate
		order by t1.loanid
				,t1.effectivedate
	;
%RUNQUIT(&job,&sub14);

DATA ADVAMT_FINAL;
	SET WORK.ADV_AMT_2;
	BY LoanID;
	IF FIRST.LOANID;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.SKYNET_ONLINE_METRICS AS 
   	  SELECT t1.LoanID, 
             t3.CompanyID AS LOCNBR, 
             t3.CompanyCode AS STATE, 
             t1.DEAL_DT,
		     dhms(t1.deal_dt,00,00,00) format=datetime20. as DEAL_DTTM,
/*           t1.OriginationDate AS DEAL_DTTM, */
          /* ADVCNT */
            (COUNT(t1.LoanID)) AS ADVCNT, 
          /* ADVAMT */
/*            (SUM(t4.ADVAMT)) AS ADVAMT, */
			(SUM(t6.ADVAMT)) AS ADVAMT,
          /* ADVFEEAMT */
            (SUM(t4.FEES_CHARGED)) AS ADVFEEAMT, 
          /* DEALSTATUSCD */
            (UPCASE(COMPRESS(t2.LoanStatusName))) AS DEALSTATUSCD, 
          /* PRODUCT */
            (CASE WHEN t1.LoanTypeID = 1 THEN UPCASE(t5.LoanTypeName)
                      WHEN t1.LoanTypeID = 2 THEN UPCASE(t5.LoanTypeName)
            ELSE '' END) AS PRODUCT
      FROM WORK.ONLINE_DEALS t1
           LEFT JOIN AA_LG.LoanStatus t2 ON (t1.LoanStatusID = t2.LoanStatusID)
           LEFT JOIN WORK.CHARGED t4 ON (t1.LoanID = t4.LoanID)
		   LEFT JOIN WORK.ADVAMT_FINAL t6 ON (t1.LoanID = t6.LoanID)
           LEFT JOIN AA_LG.LoanType t5 ON (t1.LoanTypeID = t5.LoanTypeID)
           LEFT JOIN AA_BTAG.Company t3 ON (t1.CompanyID = t3.CompanyID)
      WHERE (CALCULATED DEALSTATUSCD) NOT IN 
           (
           'FRAUDAPPLICATION',
           'WITHDRAWN',
           'VOID',
           'RESCINDED'
           ) AND t1.LoanTypeID NOT = 3
      GROUP BY t1.LoanID,
               t3.CompanyID,
               t3.CompanyCode,
               t1.DEAL_DT,
/*               t1.OriginationDate,*/
			   (CALCULATED DEAL_DTTM),
               (CALCULATED DEALSTATUSCD),
               (CALCULATED PRODUCT);
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ADV_CNT_AMT AS 
   SELECT t1.PRODUCT, 
          t1.LOCNBR, 
          t1.STATE, 
          t1.DEAL_DT, 
          t1.DEAL_DTTM, 
          /* ADVCNT */
            (SUM(t1.ADVCNT)) AS ADVCNT, 
          /* ADVAMT */
            (SUM(t1.ADVAMT)) AS ADVAMT, 
          /* ADVFEEAMT */
            (SUM(t1.ADVFEEAMT)) AS ADVFEEAMT
      FROM WORK.SKYNET_ONLINE_METRICS t1
      GROUP BY t1.PRODUCT,
               t1.LOCNBR,
               t1.STATE,
               t1.DEAL_DT,
               t1.DEAL_DTTM;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.CHARGE_OFF AS 
   SELECT t1.LoanID, 
          /* EFFECTIVE_DATE */
            (DHMS(input(t1.EffectiveDate,YYMMDD10.),00,00,00)) FORMAT=DATETIME20. AS EFFECTIVE_DATE
      FROM AA_LG.LoanAccountingStatusLog t1
           INNER JOIN AA_LG.LoanAccountingStatus t2 ON (t1.ToLoanAccountingStatusID = t2.LoanAccountingStatusID)
      WHERE t2.LoanAccountingStatusID = 2;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_WOR_PRE AS 
   SELECT t1.LoanID, 
          /* BUSINESS_DT */
            (DHMS(input(t2.EffectiveDate,YYMMDD10.),00,00,00)) FORMAT=DATETIME20. AS BUSINESS_DT, 
          /* WORAMTSUM */
            (SUM(t2.Total)) FORMAT=23.2 AS WORAMTSUM
      FROM WORK.CHARGE_OFF t1
           INNER JOIN AA_LG.VW_Trn t2 ON (t1.LoanID = t2.LoanID)
      WHERE (CALCULATED BUSINESS_DT) > t1.EFFECTIVE_DATE AND ( t2.IsPymt = 1 OR t2.IsPymtReturn = 1 )
      GROUP BY t1.LoanID,
               (CALCULATED BUSINESS_DT)
/*      ORDER BY t2.LoanID,*/
/*               t2.TrnID,*/
/*               BUSINESS_DT*/
	;

%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_WOR AS 
   SELECT /* PRODUCT */
            (UPCASE(t3.LoanTypeName)) AS PRODUCT, 
/*          t1.LoanID, */
          t1.BUSINESS_DT,
		  T2.COMPANYID		AS LOCNBR, 
          /* WORAMTSUM */
            SUM(WORAMTSUM*-1) AS WORAMTSUM
      FROM WORK.ONLINE_WOR_PRE t1
           INNER JOIN AA_LG.Loan t2 ON (t1.LoanID = t2.LoanID)
           INNER JOIN AA_LG.LoanType t3 ON (t2.LoanTypeID = t3.LoanTypeID)
      WHERE t2.LoanTypeID NOT = 3
	  GROUP BY CALCULATED PRODUCT
			  ,T1.BUSINESS_DT
			  ,T2.COMPANYID;
%RUNQUIT(&job,&sub14);

PROC SQL;
	CREATE TABLE WORK.WOR AS 
		SELECT T1.PRODUCT,
			   T1.LOCNBR, 
			   T3.COMPANYCODE AS STATE, 
			   T1.BUSINESS_DT,
			   WORAMTSUM
	FROM WORK.ONLINE_WOR T1
	LEFT JOIN AA_BTAG.COMPANY T3
		ON T1.LOCNBR = T3.COMPANYID;
%RUNQUIT(&job,&sub14);

PROC SQL;
	CREATE TABLE WORAMTSUM_TIME AS
	SELECT YEAR(DATEPART(BUSINESS_DT))			AS YEAR
		  ,MONTH(DATEPART(BUSINESS_DT))		AS MONTH
		  ,SUM(WORAMTSUM)					AS WORAMTSUM
	FROM WORK.WOR
	GROUP BY CALCULATED YEAR
			,CALCULATED MONTH
	ORDER BY CALCULATED YEAR
			,CALCULATED MONTH
;
QUIT;

PROC SQL;
   CREATE TABLE WORK.ONLINE_DAILYSUMMARY_PRE AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.BANKMODEL, 
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
          t1.LOCNBR, 
          /* LOCATION_NAME */
            ('ONLINE') AS LOCATION_NAME, 
          /* LOC_OPEN_DT */
            (''DT) FORMAT=DATETIME20. AS LOC_OPEN_DT, 
          /* LOC_CLOSE_DT */
            (''DT) FORMAT=DATETIME20. AS LOC_CLOSE_DT, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESSDT)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* NEW_ORIGINATIONS */
            (CASE 
               WHEN . = t3.ADVCNT THEN 0
               ELSE t3.ADVCNT
            END) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (CASE 
               WHEN . = t3.ADVAMT THEN 0
               ELSE t3.ADVAMT
            END) AS NEW_ADV_AMT, 
          t1.GROSS_REVENUE, 
          t1.TOTADVRECV, 
          t1.COMPLIANT_LOANS_OUTSTANDING, 
          t1.TOTDEFAULTRECV, 
          t1.DEFAULT_LOANS_OUTSTANDING, 
          t1.WOAMTSUM, 
          t2.WORAMTSUM
      FROM WORK.COMB_METRICS t1
           LEFT JOIN WORK.ADV_CNT_AMT t3 ON (t1.PRODUCT = t3.PRODUCT) AND (t1.LOCNBR = t3.LOCNBR) AND (t1.STATE = 
          t3.STATE) AND (t1.BUSINESSDT = t3.DEAL_DTTM)
           LEFT JOIN WORK.WOR t2 ON (t1.PRODUCT = t2.PRODUCT) AND (t1.STATE = t2.STATE) AND (t1.LOCNBR = t2.LOCNBR) AND 
          (t1.BUSINESSDT = t2.BUSINESS_DT)
      WHERE (CALCULATED BUSINESSDT) BETWEEN (INTNX('MONTH',TODAY(),-24,'B')) AND (INTNX('DAY',TODAY(),-1,'B'));
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE ONLINE_DAILYSUMMARY AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.BANKMODEL, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          /* BUSINESS_UNIT */
            (CASE WHEN t2.BUSINESS_UNIT ^= . THEN t2.BUSINESS_UNIT ELSE 0 END) AS BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BUSINESSDT, 
          /* NEW_ORIGINATIONS */
            (ROUND(t1.NEW_ORIGINATIONS,.01)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (ROUND(t1.NEW_ADV_AMT,.01)) AS NEW_ADV_AMT, 
          /* GROSS_REVENUE */
            (ROUND(t1.GROSS_REVENUE,.01)) AS GROSS_REVENUE, 
          /* TOTADVRECV */
            (ROUND(t1.TOTADVRECV,.01)) AS TOTADVRECV, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (ROUND(t1.COMPLIANT_LOANS_OUTSTANDING,.01)) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (ROUND(t1.TOTDEFAULTRECV,.01)) AS TOTDEFAULTRECV, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (ROUND(t1.DEFAULT_LOANS_OUTSTANDING,.01)) AS DEFAULT_LOANS_OUTSTANDING, 
          /* WOAMTSUM */
            (ROUND(t1.WOAMTSUM,.01)) AS WOAMTSUM, 
          /* WORAMTSUM */
            (ROUND(t1.WORAMTSUM,.01)) AS WORAMTSUM
      FROM WORK.ONLINE_DAILYSUMMARY_PRE t1
           LEFT JOIN BIOR.ONLINE_BU t2 ON (t1.STATE = t2.STATE);
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.ONLINE_NEW_ORIGINATIONS AS 
   SELECT t1.PRODUCT, 
          t1.POS, 
          t1.INSTANCE, 
          t1.BRANDCD, 
          t1.BANKMODEL, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.LOCATION_NAME, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BUSINESSDT, 
          t1.NEW_ORIGINATIONS, 
          t1.NEW_ADV_AMT, 
          t1.GROSS_REVENUE, 
          t1.TOTADVRECV, 
          t1.COMPLIANT_LOANS_OUTSTANDING, 
          t1.TOTDEFAULTRECV, 
          t1.DEFAULT_LOANS_OUTSTANDING, 
          t1.WOAMTSUM, 
          t1.WORAMTSUM, 
          /* GROSS_WRITE_OFF */
            (SUM(t1.WOAMTSUM,WORAMTSUM)) AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (t1.WOAMTSUM) AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (sum(t1.GROSS_REVENUE,-t1.WOAMTSUM)) AS NET_REVENUE, 
          /* PRODUCT_DESC */
            (CASE WHEN PRODUCT = 'INSTALLMENT' THEN 'AANET INSTALLMENT'
                       WHEN PRODUCT = 'PAYDAY' THEN 'AANET PAYDAY'
            ELSE ''
            END) AS PRODUCT_DESC
      FROM ONLINE_DAILYSUMMARY t1;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE RU1_LENDINGPRODUCTS_ROLLUP AS 
   SELECT T1.PRODUCT, 
          T1.PRODUCT_DESC, 
          T1.POS, 
          T1.INSTANCE, 
          T1.BRANDCD, 
          T1.BANKMODEL, 
          T1.COUNTRYCD, 
          T1.STATE, 
          T1.CITY, 
          T1.ZIP, 
          T1.BUSINESS_UNIT, 
          T1.ZONENBR, 
          T1.ZONENAME, 
          T1.REGIONNBR, 
          T1.REGIONRDO, 
          T1.DIVISIONNBR, 
          T1.DIVISIONDDO, 
          T1.LOCNBR, 
          T1.LOCATION_NAME, 
          T1.LOC_OPEN_DT, 
          T1.LOC_CLOSE_DT, 
          T1.BUSINESSDT, 
          /* NEW_ORIGINATIONS */
            (SUM(T1.NEW_ORIGINATIONS)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (SUM(NEW_ADV_AMT)) AS NEW_ADV_AMT, 
          /* NEW_ADVFEE_AMT */
            (SUM(0)) AS NEW_ADVFEE_AMT, 
          /* TOTADVRECV */
            (SUM(T1.TOTADVRECV)) FORMAT=12.2 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (SUM(0)) FORMAT=10.2 AS TOTADVFEERECV, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (SUM(T1.COMPLIANT_LOANS_OUTSTANDING)) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (SUM(T1.DEFAULT_LOANS_OUTSTANDING)) AS DEFAULT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (SUM(T1.TOTDEFAULTRECV)) FORMAT=12.2 AS TOTDEFAULTRECV, 
          /* TOTDEFAULTFEERECV */
            (SUM(0)) FORMAT=10.2 AS TOTDEFAULTFEERECV, 
          /* NSF_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_AMOUNT, 
          /* NSF_PAYMENT_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_PAYMENT_AMOUNT, 
          /* NSF_PREPAYMENT_AMOUNT */
            (SUM(0)) FORMAT=10.2 AS NSF_PREPAYMENT_AMOUNT, 
          /* WOCNT */
            (SUM(0)) AS WOCNT, 
          /* WOAMTSUM */
            (SUM(WOAMTSUM)) FORMAT=14.2 AS WOAMTSUM, 
          /* WOBAMTSUM */
            (SUM(0)) FORMAT=10.2 AS WOBAMTSUM, 
          /* WOBCNT */
            (SUM(0)) AS WOBCNT, 
          /* WORCNT */
            (SUM(0)) AS WORCNT, 
          /* WORAMTSUM */
            (SUM(WORAMTSUM)) FORMAT=10.2 AS WORAMTSUM, 
          /* CASHAGAIN_COUNT */
            (SUM(0)) AS CASHAGAIN_COUNT, 
          /* BUYBACK_COUNT */
            (SUM(0)) AS BUYBACK_COUNT, 
          /* DEPOSIT_COUNT */
            (SUM(0)) AS DEPOSIT_COUNT, 
          /* BEGIN_PWO_AMT */
            (SUM(0)) AS BEGIN_PWO_AMT, 
          /* CURRENT_PWO_AMT */
            (SUM(0)) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (SUM(0)) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (SUM(0)) AS NEXT_2_MONTH_PWO_AMT, 
          /* DEFAULT_PMT */
            (SUM(0)) FORMAT=10.2 AS DEFAULT_PMT, 
          /* DEFAULT_CNT */
            (SUM(0)) AS DEFAULT_CNT, 
          /* DEFAULT_AMT */
            (SUM(0)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* GROSS_REVENUE */
            (SUM(T1.GROSS_REVENUE)) FORMAT=10.2 AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (SUM(T1.GROSS_WRITE_OFF)) FORMAT=10.2 AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (SUM(T1.NET_WRITE_OFF)) FORMAT=10.2 AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (SUM(T1.NET_REVENUE)) FORMAT=10.2 AS NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            (SUM(0)) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (SUM(0)) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (SUM(0)) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (SUM(0)) AS ACTUAL_DURATION_FEES, 
          /* AVGDURATIONDAYS */
            (SUM(0)) AS AVGDURATIONDAYS, 
          /* AVGDURATIONCNT */
            (SUM(0)) AS AVGDURATIONCNT, 
          /* HELDCNT */
            (SUM(0)) AS HELDCNT, 
          /* PASTDUECNT_1 */
            (SUM(0)) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (SUM(0)) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* OVERSHORTAMT */
            (SUM(0)) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (SUM(0)) AS HOLDOVERAMT, 
          /* ADVAMTSUM */
            (SUM(0)) FORMAT=14.2 AS ADVAMTSUM, 
          /* AGNADVSUM */
            (SUM(0)) FORMAT=14.2 AS AGNADVSUM, 
          /* REPMTPLANCNT */
            (SUM(0)) AS REPMTPLANCNT, 
          /* ADVCNT */
            (SUM(0)) AS ADVCNT, 
          /* AVGADVAMT */
            (SUM(0)) FORMAT=10.2 AS AVGADVAMT, 
          /* AVGDURATION */
            (SUM(0)) FORMAT=10.2 AS AVGDURATION, 
          /* AVGFEEAMT */
            (SUM(0)) FORMAT=10.2 AS AVGFEEAMT, 
          /* PASTDUEAMT_2 */
            (SUM(0)) FORMAT=12.2 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (SUM(0)) FORMAT=11. AS PASTDUECNT_2, 
          /* REFINANCE_CNT */
            (SUM(0)) AS REFINANCE_CNT, 
          /* AGNCNT */
            (SUM(0)) AS AGNCNT, 
          /* POSSESSION_AMT */
            (SUM(0)) FORMAT=21.4 AS POSSESSION_AMT, 
          /* POSSESSION_CNT */
            (SUM(0)) AS POSSESSION_CNT, 
          /* PASTDUEAMT_3 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (SUM(0)) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (SUM(0)) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (SUM(0)) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (SUM(0)) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (SUM(0)) AS PASTDUECNT_6, 
          /* BLACK_BOOK_VALUE */
            (SUM(0)) AS BLACK_BOOK_VALUE, 
          /* SOLD_AMOUNT */
            (SUM(0)) FORMAT=21.4 AS SOLD_AMOUNT, 
          /* AGNAMTSUM */
            (SUM(0)) AS AGNAMTSUM, 
          /* RCC_IN_PROCESS */
            (SUM(0)) AS RCC_IN_PROCESS, 
          /* RCC_INELIGIBLE */
            (SUM(0)) FORMAT=11. AS RCC_INELIGIBLE, 
          /* ADVAMT */
            (SUM(0)) FORMAT=12.2 AS ADVAMT, 
          /* CASHAGAIN_AMOUNT */
            (SUM(0)) FORMAT=12.2 AS CASHAGAIN_AMOUNT, 
          /* SOLD_COUNT */
            (SUM(0)) FORMAT=12.2 AS SOLD_COUNT, 
          /* NET_WRITE_OFF_NEW */
            (SUM(0)) AS NET_WRITE_OFF_NEW, 
          /* GROSS_REVENUE_NEW */
            (SUM(0)) AS GROSS_REVENUE_NEW, 
          /* GROSS_WRITE_OFF_NEW */
            (SUM(0)) FORMAT=12.2 AS GROSS_WRITE_OFF_NEW, 
          /* NET_REVENUE_NEW */
            (SUM(0)) AS NET_REVENUE_NEW, 
          /* WORAMTSUM_OLD */
            (SUM(0)) FORMAT=12.2 AS WORAMTSUM_OLD, 
          /* FIRST_PRESENTMENT_CNT */
            (SUM(0)) AS FIRST_PRESENTMENT_CNT, 
          /* SATISFIED_PAYMENT_CNT */
            (SUM(0)) AS SATISFIED_PAYMENT_CNT, 
          /* DEL_RECV_AMT */
            (SUM(0)) AS DEL_RECV_AMT, 
          /* DEL_RECV_CNT */
            (SUM(0)) AS DEL_RECV_CNT
      FROM WORK.ONLINE_NEW_ORIGINATIONS t1
      GROUP BY t1.Product,
               t1.PRODUCT_DESC,
               t1.pos,
               t1.INSTANCE,
               t1.brandcd,
               t1.bankmodel,
               t1.COUNTRYCD,
               t1.STATE,
               t1.CITY,
               t1.ZIP,
               t1.BUSINESS_UNIT,
               t1.ZONENBR,
               t1.ZONENAME,
               t1.REGIONNBR,
               t1.REGIONRDO,
               t1.DIVISIONNBR,
               t1.DIVISIONDDO,
               t1.LOCNBR,
               t1.Location_Name,
               t1.LOC_OPEN_DT,
               t1.LOC_CLOSE_DT,
               t1.BusinessDt;
%RUNQUIT(&job,&sub14);

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
LIBNAME NORECV "E:\SHARED\CADA\SAS DATA\DATAMART\SCOCHRAN";

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub14);

LIBNAME BIOR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR;

data thursdaydates_tmp1;
	do i = "1JAN2000"d to today();
		businessdt = i;
		dayname = compress(put(businessdt,downame.));
		output;
	end;
	format businessdt mmddyy10.;
%RUNQUIT(&job,&sub14);

proc sql;
	create table thursdaydates_tmp2 as
		select t1.*, t2.holidayname
		  from thursdaydates_tmp1 t1
			LEFT JOIN bior.i_holidays t2 on (t1.businessdt = datepart(t2.holiday_dt))
		 order by t1.businessdt desc;
%RUNQUIT(&job,&sub14);

data thursdaydates_tmp3;
	set thursdaydates_tmp2;
	priordayholiday = lag1(holidayname);
	priordate = lag1(businessdt);
	if dayname = 'Thursday'
		AND businessdt ~= intnx('month',businessdt,0,'end')
		AND holidayname = ''
		THEN ThursdayWeek = 'Y';
	ELSE
		if dayname = 'Wednesday'
			AND (priordayholiday ~= ''
			 OR priordate = intnx('month',businessdt,0,'end'))
			THEN ThursdayWeek = 'Y';
	ELSE
		ThursdayWeek = 'N';
	format priordate mmddyy10.;
%RUNQUIT(&job,&sub14);

data daily_summary_all_tmp2;
	set RU1_LENDINGPRODUCTS_ROLLUP;
		last_report_dt = intnx('day',today(),-1);
		lastthursdayofmonth = intnx('week.5',intnx('month',businessdt,0,'end'),0);
		if lastthursdayofmonth = intnx('month',businessdt,0,'end') 
		   	or lastthursdayofmonth = holiday('veteransusg',year(businessdt))
			or lastthursdayofmonth = holiday('veterans',year(businessdt))
			or lastthursdayofmonth = holiday('thanksgiving',year(businessdt))
			or lastthursdayofmonth = holiday('christmas',year(businessdt)) 
				then lastthursdayofmonth = intnx('day',lastthursdayofmonth,-1);
		format lastthursdayofmonth mmddyy10.;
	if businessdt = lastthursdayofmonth then lastthursday = 'Y';
			else lastthursday = 'N';
	drop lastthursdayofmonth;
	format last_report_dt mmddyy10.;
%RUNQUIT(&job,&sub14);

PROC SQL;
	CREATE TABLE WORK.DAILY_SUMMARY_ALL_TMP3 AS
		SELECT T1.*, T2.THURSDAYWEEK
          FROM WORK.DAILY_SUMMARY_ALL_TMP2 T1, WORK.THURSDAYDATES_TMP3 T2
		 WHERE T1.BUSINESSDT = T2.BUSINESSDT;
%RUNQUIT(&JOB,&SUB14);

PROC SORT DATA=DAILY_SUMMARY_ALL_TMP3;
	BY LOCNBR BUSINESSDT;
%RUNQUIT(&JOB,&SUB14);

DATA LAST_REPORT_DATE;
	SET DAILY_SUMMARY_ALL_TMP3;
	BY LOCNBR BUSINESSDT;
	LOC_LAST_REPORTED_DT = BUSINESSDT;
	IF LAST.LOCNBR THEN OUTPUT;
	KEEP LOCNBR LOC_LAST_REPORTED_DT;
	FORMAT LOC_LAST_REPORTED_DT MMDDYY10.;
%RUNQUIT(&JOB,&SUB14);

PROC SQL;
   CREATE TABLE WORK.HOLIDAYS(LABEL="HOLIDAYS") AS 
   SELECT /* HOLIDAYDT */
            (DATEPART(T1.HOLIDAY_DT)) FORMAT=MMDDYY10. LABEL="HOLIDAYDT" AS HOLIDAYDT, 
          T1.HOLIDAYNAME
      FROM BIOR.I_HOLIDAYS T1;
%RUNQUIT(&JOB,&SUB14);

PROC SQL;
   CREATE TABLE WORK.DAILY_SUMMARY_ALL_TMP4 AS 
   SELECT T1.PRODUCT, 
          T1.PRODUCT_DESC, 
          T1.POS, 
          T1.INSTANCE, 
          T1.BRANDCD, 
          T1.BANKMODEL, 
          T1.COUNTRYCD, 
          T1.STATE, 
          T1.ZIP, 
          T1.CITY, 
          T1.BUSINESS_UNIT, 
          T1.ZONENBR, 
          T1.ZONENAME, 
          T1.REGIONNBR, 
          T1.REGIONRDO, 
          T1.DIVISIONNBR, 
          T1.DIVISIONDDO, 
          T1.LOCNBR, 
          T1.LOCATION_NAME, 
          T1.LOC_OPEN_DT, 
          T1.LOC_CLOSE_DT, 
          T1.BUSINESSDT, 
          T1.LASTTHURSDAY, 
          T2.HOLIDAYNAME, 
          T1.THURSDAYWEEK, 
          T1.LAST_REPORT_DT, 
          T1.NEW_ORIGINATIONS, 
          T1.NEW_ADV_AMT, 
          T1.NEW_ADVFEE_AMT, 
          T1.TOTADVRECV, 
          T1.TOTADVFEERECV, 
          T1.COMPLIANT_LOANS_OUTSTANDING, 
          T1.DEFAULT_LOANS_OUTSTANDING, 
          T1.TOTDEFAULTRECV, 
          T1.TOTDEFAULTFEERECV, 
          T1.NSF_AMOUNT, 
          T1.NSF_PAYMENT_AMOUNT, 
          T1.NSF_PREPAYMENT_AMOUNT, 
          T1.WOAMTSUM, 
          T1.WOCNT, 
          T1.WOBAMTSUM, 
          T1.WOBCNT, 
          T1.WORAMTSUM, 
          T1.WORCNT, 
          T1.CASHAGAIN_COUNT, 
          T1.BUYBACK_COUNT, 
          T1.DEPOSIT_COUNT, 
          T1.GROSS_REVENUE, 
          T1.GROSS_WRITE_OFF, 
          T1.NET_WRITE_OFF, 
          T1.NET_REVENUE, 
          T1.BEGIN_PWO_AMT, 
          T1.CURRENT_PWO_AMT, 
          T1.NEXT_MONTH_PWO_AMT, 
          T1.NEXT_2_MONTH_PWO_AMT, 
          T1.RCC_IN_PROCESS, 
          T1.RCC_INELIGIBLE, 
          T1.DEL_RECV_AMT, 
          T1.DEL_RECV_CNT, 
          T1.DEFAULT_PMT, 
          T1.DEFAULT_CNT, 
          T1.DEFAULT_AMT, 
          T1.ACTUAL_DURATION_COUNT, 
          T1.ACTUAL_DURATION_DAYS, 
          T1.ACTUAL_DURATION_ADVAMT, 
          T1.ACTUAL_DURATION_FEES, 
          T1.BLACK_BOOK_VALUE, 
          T1.PASTDUECNT_1, 
          T1.PASTDUEAMT_1, 
          T1.PASTDUEAMT_2, 
          T1.PASTDUECNT_2, 
          T1.PASTDUEAMT_3, 
          T1.PASTDUECNT_3, 
          T1.PASTDUEAMT_4, 
          T1.PASTDUECNT_4, 
          T1.PASTDUEAMT_5, 
          T1.PASTDUECNT_5, 
          T1.PASTDUEAMT_6, 
          T1.PASTDUECNT_6, 
          T1.REFINANCE_CNT, 
          T1.OVERSHORTAMT, 
          T1.HOLDOVERAMT, 
          T1.FIRST_PRESENTMENT_CNT, 
          T1.SATISFIED_PAYMENT_CNT, 
          T1.POSSESSION_AMT, 
          T1.POSSESSION_CNT, 
          T1.SOLD_AMOUNT, 
          T1.SOLD_COUNT, 
          T1.REPMTPLANCNT AS REPMTPLANCNT1, 
          T1.ADVCNT, 
          T1.AVGADVAMT, 
          T1.AVGDURATION, 
          T1.AVGFEEAMT, 
          T1.ADVAMTSUM, 
          T1.AVGDURATIONDAYS, 
          T1.AVGDURATIONCNT, 
          T1.HELDCNT, 
          T1.REPMTPLANCNT, 
          T1.AGNCNT
      FROM WORK.DAILY_SUMMARY_ALL_TMP3 T1
           LEFT JOIN WORK.HOLIDAYS T2 ON (T1.BUSINESSDT = T2.HOLIDAYDT);
%RUNQUIT(&JOB,&SUB14);

PROC SQL;
   CREATE TABLE WORK.DAILY_SUMMARY_ALL_PreLoad1_pre AS 
   SELECT DISTINCT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.BusinessDt, 
          /* LAST_REPORT_DT */
            (CASE WHEN WEEKDAY(TODAY()) = 2 THEN TODAY()-2 ELSE TODAY()-1 END) FORMAT=MMDDYY10. AS LAST_REPORT_DT, 
          t2.loc_last_reported_dt, 
          t3.Latitude, 
          t3.Longitude, 
          t1.HOLIDAYNAME, 
          t1.lastthursday, 
          t1.ThursdayWeek, 
          /* NEW_ADV_AMT */
            (CASE 
               WHEN . = t1.NEW_ADV_AMT THEN 0
               ELSE t1.NEW_ADV_AMT
            END) AS NEW_ADV_AMT, 
          /* NEW_ORIGINATIONS */
            (CASE 
               WHEN . = t1.NEW_ORIGINATIONS THEN 0
               ELSE t1.NEW_ORIGINATIONS
            END) AS NEW_ORIGINATIONS, 
          /* NEW_ADVFEE_AMT */
            (CASE 
               WHEN . = t1.NEW_ADVFEE_AMT THEN 0
               ELSE t1.NEW_ADVFEE_AMT
            END) AS NEW_ADVFEE_AMT, 
          /* TOTADVRECV */
            (CASE 
               WHEN . = t1.TOTADVRECV THEN 0
               ELSE t1.TOTADVRECV
            END) FORMAT=22.2 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (CASE 
               WHEN . = t1.TOTADVFEERECV THEN 0
               ELSE t1.TOTADVFEERECV
            END) FORMAT=10.2 AS TOTADVFEERECV, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (CASE 
               WHEN . = t1.DEFAULT_LOANS_OUTSTANDING THEN 0
               ELSE t1.DEFAULT_LOANS_OUTSTANDING
            END) AS DEFAULT_LOANS_OUTSTANDING, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (CASE 
               WHEN . = t1.COMPLIANT_LOANS_OUTSTANDING THEN 0
               ELSE t1.COMPLIANT_LOANS_OUTSTANDING
            END) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (CASE 
               WHEN . = t1.TOTDEFAULTRECV THEN 0
               ELSE t1.TOTDEFAULTRECV
            END) FORMAT=22.2 AS TOTDEFAULTRECV, 
          /* TOTDEFAULTFEERECV */
            (CASE 
               WHEN . = t1.TOTDEFAULTFEERECV THEN 0
               ELSE t1.TOTDEFAULTFEERECV
            END) FORMAT=10.2 AS TOTDEFAULTFEERECV, 
          /* NSF_AMOUNT */
            (CASE 
               WHEN . = t1.NSF_AMOUNT THEN 0
               ELSE t1.NSF_AMOUNT
            END) FORMAT=10.2 AS NSF_AMOUNT, 
          /* NSF_PAYMENT_AMOUNT */
            (CASE 
               WHEN . = t1.NSF_PAYMENT_AMOUNT THEN 0
               ELSE t1.NSF_PAYMENT_AMOUNT
            END) FORMAT=10.2 AS NSF_PAYMENT_AMOUNT, 
          /* NSF_PREPAYMENT_AMOUNT */
            (CASE 
               WHEN . = t1.NSF_PREPAYMENT_AMOUNT THEN 0
               ELSE t1.NSF_PREPAYMENT_AMOUNT
            END) FORMAT=10.2 AS NSF_PREPAYMENT_AMOUNT, 
          /* WOAMTSUM */
            (CASE 
               WHEN . = t1.WOAMTSUM THEN 0
               ELSE t1.WOAMTSUM
            END) FORMAT=22.2 AS WOAMTSUM, 
          /* WOCNT */
            (CASE 
               WHEN . = t1.WOCNT THEN 0
               ELSE t1.WOCNT
            END) AS WOCNT, 
          /* WOBAMTSUM */
            (CASE 
               WHEN . = t1.WOBAMTSUM THEN 0
               ELSE t1.WOBAMTSUM
            END) FORMAT=10.2 AS WOBAMTSUM, 
          /* WOBCNT */
            (CASE 
               WHEN . = t1.WOBCNT THEN 0
               ELSE t1.WOBCNT
            END) AS WOBCNT, 
          /* WORAMTSUM */
            (CASE 
               WHEN . = t1.WORAMTSUM THEN 0
               ELSE t1.WORAMTSUM
            END) FORMAT=22.2 AS WORAMTSUM, 
          /* WORCNT */
            (CASE 
               WHEN . = t1.WORCNT THEN 0
               ELSE t1.WORCNT
            END) AS WORCNT, 
          /* CASHAGAIN_COUNT */
            (CASE 
               WHEN . = t1.CASHAGAIN_COUNT THEN 0
               ELSE t1.CASHAGAIN_COUNT
            END) AS CASHAGAIN_COUNT, 
          /* BUYBACK_COUNT */
            (CASE 
               WHEN . = t1.BUYBACK_COUNT THEN 0
               ELSE t1.BUYBACK_COUNT
            END) AS BUYBACK_COUNT, 
          /* DEPOSIT_COUNT */
            (CASE 
               WHEN . = t1.DEPOSIT_COUNT THEN 0
               ELSE t1.DEPOSIT_COUNT
            END) AS DEPOSIT_COUNT, 
          /* GROSS_REVENUE */
            (CASE 
               WHEN . = t1.GROSS_REVENUE THEN 0
               ELSE t1.GROSS_REVENUE
            END) FORMAT=22.2 AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (CASE 
               WHEN . = t1.GROSS_WRITE_OFF THEN 0
               ELSE t1.GROSS_WRITE_OFF
            END) FORMAT=22.2 AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (CASE 
               WHEN . = t1.NET_WRITE_OFF THEN 0
               ELSE t1.NET_WRITE_OFF
            END) FORMAT=22.2 AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (CASE 
               WHEN . = t1.NET_REVENUE THEN 0
               ELSE t1.NET_REVENUE
            END) FORMAT=22.2 AS NET_REVENUE, 
          /* BEGIN_PWO_AMT */
            (CASE 
               WHEN . = t1.BEGIN_PWO_AMT THEN 0
               ELSE t1.BEGIN_PWO_AMT
            END) AS BEGIN_PWO_AMT, 
          /* CURRENT_PWO_AMT */
            (CASE 
               WHEN . = t1.CURRENT_PWO_AMT THEN 0
               ELSE t1.CURRENT_PWO_AMT
            END) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (CASE 
               WHEN . = t1.NEXT_MONTH_PWO_AMT THEN 0
               ELSE t1.NEXT_MONTH_PWO_AMT
            END) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (CASE 
               WHEN . = t1.NEXT_2_MONTH_PWO_AMT THEN 0
               ELSE t1.NEXT_2_MONTH_PWO_AMT
            END) AS NEXT_2_MONTH_PWO_AMT, 
          /* RCC_IN_PROCESS */
            (CASE 
               WHEN . = t1.RCC_IN_PROCESS THEN 0
               ELSE t1.RCC_IN_PROCESS
            END) AS RCC_IN_PROCESS, 
          /* RCC_INELIGIBLE */
            (CASE 
               WHEN . = t1.RCC_INELIGIBLE THEN 0
               ELSE t1.RCC_INELIGIBLE
            END) FORMAT=11. AS RCC_INELIGIBLE, 
          /* DEL_RECV_AMT */
            (CASE 
               WHEN . = t1.DEL_RECV_AMT THEN 0
               ELSE t1.DEL_RECV_AMT
            END) AS DEL_RECV_AMT, 
          /* DEL_RECV_CNT */
            (CASE 
               WHEN . = t1.DEL_RECV_CNT THEN 0
               ELSE t1.DEL_RECV_CNT
            END) AS DEL_RECV_CNT, 
          /* DEFAULT_PMT */
            (CASE 
               WHEN . = t1.DEFAULT_PMT THEN 0
               ELSE t1.DEFAULT_PMT
            END) FORMAT=10.2 AS DEFAULT_PMT, 
          /* DEFAULT_CNT */
            (CASE 
               WHEN . = t1.DEFAULT_CNT THEN 0
               ELSE t1.DEFAULT_CNT
            END) AS DEFAULT_CNT, 
          /* DEFAULT_AMT */
            (CASE 
               WHEN . = t1.DEFAULT_AMT THEN 0
               ELSE t1.DEFAULT_AMT
            END) FORMAT=10.2 AS DEFAULT_AMT, 
          /* ACTUAL_DURATION_COUNT */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_COUNT THEN 0
               ELSE t1.ACTUAL_DURATION_COUNT
            END) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_DAYS THEN 0
               ELSE t1.ACTUAL_DURATION_DAYS
            END) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_ADVAMT THEN 0
               ELSE t1.ACTUAL_DURATION_ADVAMT
            END) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (CASE 
               WHEN . = t1.ACTUAL_DURATION_FEES THEN 0
               ELSE t1.ACTUAL_DURATION_FEES
            END) AS ACTUAL_DURATION_FEES, 
          /* AVGDURATIONDAYS */
            (CASE 
               WHEN . = t1.AVGDURATIONDAYS THEN 0
               ELSE t1.AVGDURATIONDAYS
            END) AS AVGDURATIONDAYS, 
          /* AVGDURATIONCNT */
            (CASE 
               WHEN . = t1.AVGDURATIONCNT THEN 0
               ELSE t1.AVGDURATIONCNT
            END) AS AVGDURATIONCNT, 
          /* BLACK_BOOK_VALUE */
            (CASE 
               WHEN . = t1.BLACK_BOOK_VALUE THEN 0
               ELSE t1.BLACK_BOOK_VALUE
            END) AS BLACK_BOOK_VALUE, 
          /* PASTDUECNT_1 */
            (CASE 
               WHEN . = t1.PASTDUECNT_1 THEN 0
               ELSE t1.PASTDUECNT_1
            END) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_1 THEN 0
               ELSE t1.PASTDUEAMT_1
            END) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* PASTDUEAMT_2 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_2 THEN 0
               ELSE t1.PASTDUEAMT_2
            END) FORMAT=12.2 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (CASE 
               WHEN . = t1.PASTDUECNT_2 THEN 0
               ELSE t1.PASTDUECNT_2
            END) FORMAT=11. AS PASTDUECNT_2, 
          /* PASTDUEAMT_3 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_3 THEN 0
               ELSE t1.PASTDUEAMT_3
            END) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (CASE 
               WHEN . = t1.PASTDUECNT_3 THEN 0
               ELSE t1.PASTDUECNT_3
            END) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_4 THEN 0
               ELSE t1.PASTDUEAMT_4
            END) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (CASE 
               WHEN . = t1.PASTDUECNT_4 THEN 0
               ELSE t1.PASTDUECNT_4
            END) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_5 THEN 0
               ELSE t1.PASTDUEAMT_5
            END) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (CASE 
               WHEN . = t1.PASTDUECNT_5 THEN 0
               ELSE t1.PASTDUECNT_5
            END) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (CASE 
               WHEN . = t1.PASTDUEAMT_6 THEN 0
               ELSE t1.PASTDUEAMT_6
            END) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (CASE 
               WHEN . = t1.PASTDUECNT_6 THEN 0
               ELSE t1.PASTDUECNT_6
            END) AS PASTDUECNT_6, 
          /* REFINANCE_CNT */
            (CASE 
               WHEN . = t1.REFINANCE_CNT THEN 0
               ELSE t1.REFINANCE_CNT
            END) AS REFINANCE_CNT, 
          /* OVERSHORTAMT */
            (CASE 
               WHEN . = t1.OVERSHORTAMT THEN 0
               ELSE t1.OVERSHORTAMT
            END) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (CASE 
               WHEN . = t1.HOLDOVERAMT THEN 0
               ELSE t1.HOLDOVERAMT
            END) AS HOLDOVERAMT, 
          /* FIRST_PRESENTMENT_CNT */
            (CASE 
               WHEN . = t1.FIRST_PRESENTMENT_CNT THEN 0
               ELSE t1.FIRST_PRESENTMENT_CNT
            END) AS FIRST_PRESENTMENT_CNT, 
          /* SATISFIED_PAYMENT_CNT */
            (CASE 
               WHEN . = t1.SATISFIED_PAYMENT_CNT THEN 0
               ELSE t1.SATISFIED_PAYMENT_CNT
            END) AS SATISFIED_PAYMENT_CNT, 
          /* POSSESSION_AMT */
            (CASE 
               WHEN . = t1.POSSESSION_AMT THEN 0
               ELSE t1.POSSESSION_AMT
            END) FORMAT=21.4 AS POSSESSION_AMT, 
          /* POSSESSION_CNT */
            (CASE 
               WHEN . = t1.POSSESSION_CNT THEN 0
               ELSE t1.POSSESSION_CNT
            END) AS POSSESSION_CNT, 
          /* SOLD_AMOUNT */
            (CASE 
               WHEN . = t1.SOLD_AMOUNT THEN 0
               ELSE t1.SOLD_AMOUNT
            END) FORMAT=21.4 AS SOLD_AMOUNT, 
          /* SOLD_COUNT */
            (CASE 
               WHEN . = t1.SOLD_COUNT THEN 0
               ELSE t1.SOLD_COUNT
            END) FORMAT=12.2 AS SOLD_COUNT, 
          /* ADVCNT */
            (CASE 
               WHEN . = t1.ADVCNT THEN 0
               ELSE t1.ADVCNT
            END) AS ADVCNT, 
          /* AVGADVAMT */
            (CASE 
               WHEN . = t1.AVGADVAMT THEN 0
               ELSE t1.AVGADVAMT
            END) FORMAT=10.2 AS AVGADVAMT, 
          /* AVGDURATION */
            (CASE 
               WHEN . = t1.AVGDURATION THEN 0
               ELSE t1.AVGDURATION
            END) FORMAT=10.2 AS AVGDURATION, 
          /* AVGFEEAMT */
            (CASE 
               WHEN . = t1.AVGFEEAMT THEN 0
               ELSE t1.AVGFEEAMT
            END) FORMAT=10.2 AS AVGFEEAMT, 
          /* ADVAMTSUM */
            (CASE 
               WHEN . = t1.ADVAMTSUM THEN 0
               ELSE t1.ADVAMTSUM
            END) FORMAT=14.2 AS ADVAMTSUM, 
          /* HELDCNT */
            (CASE 
               WHEN . = t1.HELDCNT THEN 0
               ELSE t1.HELDCNT
            END) AS HELDCNT, 
          /* REPMTPLANCNT */
            (CASE 
               WHEN . = t1.REPMTPLANCNT THEN 0
               ELSE t1.REPMTPLANCNT
            END) AS REPMTPLANCNT, 
          /* AGNCNT */
            (CASE 
               WHEN . = t1.AGNCNT THEN 0
               ELSE t1.AGNCNT
            END) AS AGNCNT
      FROM WORK.DAILY_SUMMARY_ALL_TMP4 t1
           INNER JOIN WORK.LAST_REPORT_DATE t2 ON (t1.LOCNBR = t2.LOCNBR)
           LEFT JOIN SKYNET.LOCATION_LATLONG t3 ON (t1.LOCNBR = t3.locnbr)
           LEFT JOIN SKYNET.PS2_LENDING_REVEXP t5 ON (t1.Product = t5.Product) AND (t1.pos = t5.pos) AND (t1.INSTANCE = 
          t5.INSTANCE) AND (t1.LOCNBR = t5.locnbr) AND (t1.BusinessDt = t5.BusinessDt)
      WHERE t1.BusinessDt BETWEEN INTNX('MONTH',TODAY(),-24,'B') AND TODAY()-1
      ORDER BY t1.LOCNBR,
               t1.BusinessDt,
               t1.Product,
               t1.INSTANCE,
               t1.PRODUCT_DESC;
%RUNQUIT(&job,&sub14);

DATA DAILY_SUMMARY_ALL_PRE;
	SET WORK.DAILY_SUMMARY_ALL_PRELOAD1_PRE;
		IF PRODUCT_DESC ^= "AL ETL" AND SUM(NEW_ADV_AMT,
		   NEW_ORIGINATIONS,
		   NEW_ADVFEE_AMT,
		   TOTADVRECV,
		   TOTADVFEERECV,
		   COMPLIANT_LOANS_OUTSTANDING,
		   DEFAULT_LOANS_OUTSTANDING,
		   TOTDEFAULTRECV,
		   TOTDEFAULTFEERECV,
		   NSF_AMOUNT,
		   NSF_PAYMENT_AMOUNT,
		   NSF_PREPAYMENT_AMOUNT,
		   WOAMTSUM,
		   WOBAMTSUM,
		   WORAMTSUM,
		   CASHAGAIN_COUNT,
		   BUYBACK_COUNT,
		   DEPOSIT_COUNT,
		   GROSS_REVENUE,
		   GROSS_WRITE_OFF,
		   NET_WRITE_OFF,
		   NET_REVENUE,
		   BEGIN_PWO_AMT,
		   CURRENT_PWO_AMT,
		   NEXT_MONTH_PWO_AMT,
		   NEXT_2_MONTH_PWO_AMT,
		   RCC_IN_PROCESS,
		   RCC_INELIGIBLE,
		   DEFAULT_PMT,
		   DEFAULT_CNT,
		   DEFAULT_AMT,
		   ACTUAL_DURATION_COUNT,
		   ACTUAL_DURATION_DAYS,
		   ACTUAL_DURATION_ADVAMT,
		   ACTUAL_DURATION_FEES,
		   BLACK_BOOK_VALUE,
		   PASTDUECNT_1,
	   	   PASTDUEAMT_1,
		   PASTDUEAMT_2,
		   PASTDUECNT_2,
		   PASTDUEAMT_3,
		   PASTDUECNT_3,
		   PASTDUEAMT_4,
		   PASTDUECNT_4,
		   PASTDUEAMT_5,
		   PASTDUECNT_5,
		   PASTDUEAMT_6,
		   PASTDUECNT_6,
		   SOLD_AMOUNT,
		   SOLD_COUNT,		
		   ADVCNT,
		   AVGADVAMT,
		   AVGDURATION,
		   AVGFEEAMT,
		   ADVAMTSUM,
		   ADVAMTSUM,
		   HELDCNT,
		   AGNCNT
		   ) = 0 THEN DELETE;
%RUNQUIT(&job,&sub14);


%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
LIBNAME BIOR ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=BIOR;

LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";

/*WAITFOR CUST LIFECYCLE TO BE READY FOR THE DAY*/

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET ONLINE_STATUS = 'WAITING_CL'
			   ,LOC_STATUS = 'WAITING_CL'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub14);

%MACRO WAITFORCUSTLIFE();


	%DO %UNTIL (%EVAL(&COUNT_R. >= 1));

		PROC SQL;
			CREATE TABLE CUST_LIFE_CHECK_TODAY AS
			SELECT INSTANCE
				  ,MAX(BUSINESS_DATE)	AS BUSINESSDT
			FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE INSTANCE = 'AANET'
			GROUP BY INSTANCE
		;
		QUIT;

		DATA _NULL_;
			FORMAT WEEKDAY $20.;
			DAYOFWEEK = WEEKDAY(DATE());
			IF DAYOFWEEK = 1 THEN WEEKDAY = 'SUNDAY';
			ELSE IF DAYOFWEEK = 2 THEN WEEKDAY = 'MONDAY';
			ELSE IF DAYOFWEEK = 3 THEN WEEKDAY = 'TUESDAY';
			ELSE IF DAYOFWEEK = 4 THEN WEEKDAY = 'WEDNESDAY';
			ELSE IF DAYOFWEEK = 5 THEN WEEKDAY = 'THURSDAY';
			ELSE IF DAYOFWEEK = 6 THEN WEEKDAY = 'FRIDAY';
			ELSE IF DAYOFWEEK = 7 THEN WEEKDAY = 'SATURDAY';
			CALL SYMPUTX("DAYOFWEEK",WEEKDAY,'G');
		RUN;

		%IF &DAYOFWEEK. = SUNDAY
			OR &DAYOFWEEK. = TUESDAY
			OR &DAYOFWEEK. = WEDNESDAY
			OR &DAYOFWEEK. = THURSDAY
			OR &DAYOFWEEK. = FRIDAY
			OR &DAYOFWEEK. = SATURDAY %THEN 
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'AANET' AND BUSINESSDT >= DHMS(TODAY()-1,00,00,00)
					;
					QUIT;

				%END;
		%ELSE %IF &DAYOFWEEK. = MONDAY %THEN
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'AANET' AND BUSINESSDT >= DHMS(TODAY()-2,00,00,00)
					;
					QUIT;
				%END;
					
		%PUT THE COUNT IS EQUAL TO : &COUNT_R;

		%IF %EVAL(&COUNT_R. < 1) %THEN 
			%DO;
				/*SLEEPS FOR 300 SECONDS (5 MINUTES) UNTIL IT FINDS 16 FINISHED TABLES, IT WILL LOOP FOREVER UNTIL THE 16 FINISHED TABLES*/
				DATA SLEEP;
					CALL SLEEP(300,1);
				RUN;
			%END;
	%END;

%MEND;

%WAITFORCUSTLIFE

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET ONLINE_STATUS = 'RUNNING'
			   ,LOC_STATUS = 'RUNNING'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE WORK.PROD_DESC_CHANGE AS 
   SELECT /* BUSINESS_DATE */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESS_DATE, 
          t1.LOCATION_NBR, 
          t1.INSTANCE, 
          t1.PRODUCT, 
          /* PRODUCT_DESC */
            (CASE WHEN (INSTANCE = 'EAPROD1' AND T1.BUSINESS_DATE >= T2.DEALDATE  AND T2.DEALDATE ^= .) 
												    THEN "TX CSO Cash Advance" 
				  WHEN PRODUCTDESC = 'TX CSO' 		THEN 'EADV PAYDAY'
                  WHEN PRODUCTDESC = 'TEXAS TITLE' THEN 'TX TITLE' ELSE PRODUCTDESC END) AS PRODUCT_DESC, 
          /* NEW_CUST_CNT */
            (SUM(t1.NEW_CUST_CNT)) AS NEW_CUST_CNT, 
          /* REDEEM_CUST_CNT */
            (SUM(t1.REDEEM_CUST_CNT)) AS REDEEM_CUST_CNT, 
          /* NEW_REPEAT_CUST_CNT */
            (SUM(t1.NEW_REPEAT_CUST_CNT)) AS NEW_REPEAT_CUST_CNT, 
          /* REACTIVE_CUST_CNT */
            (SUM(t1.REACTIVE_CUST_CNT)) AS REACTIVE_CUST_CNT, 
          /* ACTIVE_CUST_CNT */
            (SUM(t1.ACTIVE_CUST_CNT)) AS ACTIVE_CUST_CNT, 
          /* INACTIVE_CUST_CNT */
            (SUM(t1.INACTIVE_CUST_CNT)) AS INACTIVE_CUST_CNT
      FROM BIOR.CUST_CATEGORY_DAILY_COUNT T1
	  LEFT JOIN SKYNET.TX_UC_DATEBYLOC AS T2
			ON (T1.LOCATION_NBR = T2.LOCNBR)
	  WHERE T1.INSTANCE = 'AANET' AND T1.BUSINESS_DATE >= DHMS(TODAY()-5,00,00,00)
      GROUP BY (CALCULATED BUSINESS_DATE),
               t1.LOCATION_NBR,
               t1.INSTANCE,
               t1.PRODUCT,
               (CALCULATED PRODUCT_DESC)
;
%RUNQUIT(&job,&sub14);

PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_OL_1 AS 
   SELECT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.CITY, 
          t1.ZIP, 
          t1.BUSINESS_UNIT, 
          t1.ZONENBR, 
          t1.ZONENAME, 
          t1.REGIONNBR, 
          t1.REGIONRDO, 
          t1.DIVISIONNBR, 
          t1.DIVISIONDDO, 
          t1.LOCNBR, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          /* BUSINESSDT */
            (DHMS(t1.BusinessDt,00,00,00)) FORMAT=DATETIME20. AS BUSINESSDT, 
          /* LAST_REPORT_DT */
            (DHMS(DATE()-1,00,00,00)) FORMAT=DATETIME20. AS LAST_REPORT_DT, 
          /* LOC_LAST_REPORTED_DT */
            (DHMS(t1.loc_last_reported_dt,00,00,00)) FORMAT=DATETIME20. AS LOC_LAST_REPORTED_DT, 
          t1.Latitude, 
          t1.Longitude, 
          t1.HOLIDAYNAME, 
          t1.lastthursday, 
          t1.ThursdayWeek, 
          t1.NEW_ADV_AMT, 
          t1.NEW_ORIGINATIONS, 
          t1.NEW_ADVFEE_AMT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          t1.DEFAULT_LOANS_OUTSTANDING, 
          t1.COMPLIANT_LOANS_OUTSTANDING, 
          t1.TOTDEFAULTRECV, 
          t1.TOTDEFAULTFEERECV, 
          t1.NSF_AMOUNT, 
          t1.NSF_PAYMENT_AMOUNT, 
          t1.NSF_PREPAYMENT_AMOUNT, 
          t1.WOAMTSUM, 
          t1.WOCNT, 
          t1.WOBAMTSUM, 
          t1.WOBCNT, 
          t1.WORAMTSUM, 
          t1.WORCNT, 
          t1.CASHAGAIN_COUNT, 
          t1.BUYBACK_COUNT, 
          t1.DEPOSIT_COUNT, 
          t1.GROSS_REVENUE, 
          t1.GROSS_WRITE_OFF, 
          t1.NET_WRITE_OFF, 
          t1.NET_REVENUE, 
          t1.BEGIN_PWO_AMT, 
          t1.CURRENT_PWO_AMT, 
          t1.NEXT_MONTH_PWO_AMT, 
          t1.NEXT_2_MONTH_PWO_AMT, 
          t1.RCC_IN_PROCESS, 
          t1.RCC_INELIGIBLE, 
          t1.DEL_RECV_AMT, 
          t1.DEL_RECV_CNT, 
          t1.DEFAULT_PMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_AMT, 
          /* NEWCUSTCNTCOMPANY */
            (CASE WHEN SUM(t2.NEW_CUST_CNT,t2.NEW_REPEAT_CUST_CNT) = . THEN 0 ELSE 
            SUM(t2.NEW_CUST_CNT,t2.NEW_REPEAT_CUST_CNT) END) AS NEWCUSTCNTCOMPANY, 
          /* REDEEM_CUSTOMER_CNT */
            (CASE 
               WHEN . = t2.REDEEM_CUST_CNT THEN 0
               ELSE t2.REDEEM_CUST_CNT
            END) AS REDEEM_CUSTOMER_CNT, 
          /* REACTIVE_CUSTOMER_CNT */
            (CASE 
               WHEN . = t2.REACTIVE_CUST_CNT THEN 0
               ELSE t2.REACTIVE_CUST_CNT
            END) AS REACTIVE_CUSTOMER_CNT, 
          /* ACTIVE_CUST_CNT */
            (CASE 
               WHEN . = t2.ACTIVE_CUST_CNT THEN 0
               ELSE t2.ACTIVE_CUST_CNT
            END) AS ACTIVE_CUST_CNT, 
          /* INACTIVE_CUST_CNT */
            (CASE 
               WHEN . = t2.INACTIVE_CUST_CNT THEN 0
               ELSE t2.INACTIVE_CUST_CNT
            END) AS INACTIVE_CUST_CNT, 
          t1.ACTUAL_DURATION_COUNT, 
          t1.ACTUAL_DURATION_DAYS, 
          t1.ACTUAL_DURATION_ADVAMT, 
          t1.ACTUAL_DURATION_FEES, 
          t1.AVGDURATIONDAYS, 
          t1.AVGDURATIONCNT, 
          t1.BLACK_BOOK_VALUE, 
          t1.PASTDUECNT_1, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUEAMT_2, 
          t1.PASTDUECNT_2, 
          t1.PASTDUEAMT_3, 
          t1.PASTDUECNT_3, 
          t1.PASTDUEAMT_4, 
          t1.PASTDUECNT_4, 
          t1.PASTDUEAMT_5, 
          t1.PASTDUECNT_5, 
          t1.PASTDUEAMT_6, 
          t1.PASTDUECNT_6, 
          t1.REFINANCE_CNT, 
          t1.OVERSHORTAMT, 
          t1.HOLDOVERAMT, 
          t1.FIRST_PRESENTMENT_CNT, 
          t1.SATISFIED_PAYMENT_CNT, 
          t1.POSSESSION_AMT, 
          t1.POSSESSION_CNT, 
          t1.SOLD_AMOUNT, 
          t1.SOLD_COUNT, 
          t1.ADVCNT, 
          t1.REPMTPLANCNT, 
          t1.AVGADVAMT, 
          t1.AVGDURATION, 
          t1.AVGFEEAMT, 
          t1.ADVAMTSUM, 
          t1.HELDCNT, 
          t1.AGNCNT,
		  "ONLINE"				AS CHANNELCD
      FROM DAILY_SUMMARY_ALL_PRE t1
           LEFT JOIN WORK.PROD_DESC_CHANGE t2 ON (t1.INSTANCE = t2.INSTANCE) AND (t1.PRODUCT_DESC = t2.PRODUCT_DESC) 
          AND (T1.PRODUCT = t2.PRODUCT) AND (T1.BUSINESSDT = T2.BUSINESS_DATE) AND (t1.LOCNBR = t2.LOCATION_NBR);
%RUNQUIT(&job,&sub14);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub14);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\ONLINE",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\ONLINE\DIR2\",'G');
%RUNQUIT(&job,&sub14);

PROC SQL;
    INSERT INTO SKY.DAILYSUM_DATAMART_OL (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM DAILY_SUMMARY_ALL_OL_1
	WHERE BUSINESSDT >= DHMS(TODAY()-5,00,00,00);
%RUNQUIT(&job,&sub14);

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub14);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
RUN;

%PUT &TIMESTAMP;


/*KICK OFF OL_DAILY UPLOAD*/
SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
				 '&DAILY_FILE_PATH.\TRANSPOSE OL.SAS'
				 -LOG '&DAILY_LOGPATH.\TRANSPOSE_OL_&TIMESTAMP..LOG'
				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
TASKNAME=TRANSPOSE_OL
STATUS=TRANSPOSE_OL;

/*UPLOAD ONLINE*/
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_OL.SAS";


PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.O_DAILY_SUMMARY_ALL
		    SET LAST_REPORT_DT = TO_DATE(TO_CHAR(CURRENT_DATE-1, 'MM/DD/YYYY'), 'MM/DD/YYYY')
			WHERE INSTANCE = 'AANET'
			)
	BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

WAITFOR _ALL_ TRANSPOSE_OL;


/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%abort cancel;
	%END;

%MEND;

%STOPPROGRAM