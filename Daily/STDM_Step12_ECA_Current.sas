%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";


%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub9);

DATA _NULL_;
	CALL SYMPUTX('DAILY_LOGPATH',"E:\SHARED\CADA\LOGS\SKYNET V2",'G');
	CALL SYMPUTX('DAILY_FILE_PATH',"E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SKYNET V2\SKYNET REDESIGN\DATAMART REDESIGN\DAILY",'G');
%RUNQUIT(&job,&sub9);

LIBNAME ECA ORACLE
	PATH=EDWPRD
	SCHEMA=ECA
	USER=&USER
	PASSWORD=&PASSWORD DEFER=YES;

LIBNAME BIOR ORACLE
	 USER=&USER
	 PW=&PASSWORD
	 PATH=BIOR
	 SCHEMA=BIOR DEFER=YES;

 LIBNAME CADA ORACLE
	 USER=&USER
	 PW=&PASSWORD
	 PATH=BIOR
	 SCHEMA=CADA DEFER=YES;

 LIBNAME BIORDEV ORACLE
 	USER=&USER
	PW=&PASSWORD
	PATH=BIOR
	SCHEMA=BIORDEV DEFER=YES;

PROC SQL;
	CREATE TABLE QF4_PWO_PRE AS
		SELECT 
			CASE WHEN XREF.LOCATION_AA = . THEN PWO.STORE_NBR
  	  	 		 ELSE XREF.LOCATION_AA
			END AS LOCNBR
		   ,DHMS(DATEPART(AS_OF_DATE),00,00,00) AS BUSINESSDT FORMAT DATETIME20.
		   ,SUM(CASE WHEN AS_OF_DATE = DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),0,'B'),00,00,00)
			  	     AND PROJ_CHG_OFF_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),0,'B'),00,00,00) 
												   AND DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),0,'E'),00,00,00)
					 	THEN TOTAL_DUE
					 ELSE 0
				END) AS BEGIN_PWO_AMT_PRE
		   ,SUM(CASE WHEN PROJ_CHG_OFF_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),0,'B'),00,00,00) 
													AND DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),0,'E'),00,00,00)
		   				THEN TOTAL_DUE
					 ELSE 0
				END) AS CURRENT_PWO_AMT
		   ,SUM(CASE WHEN PROJ_CHG_OFF_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),1,'B'),00,00,00) 
											   	 	AND DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),1,'E'),00,00,00)
			     	  	THEN TOTAL_DUE
					  ELSE 0
				 END) AS NEXT_MONTH_PWO_AMT
		   ,SUM(CASE WHEN PROJ_CHG_OFF_DATE BETWEEN DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),2,'B'),00,00,00) 
									  				AND DHMS(INTNX('MONTH',DATEPART(AS_OF_DATE),2,'E'),00,00,00)
					  	THEN TOTAL_DUE
					  ELSE 0
				 END) AS NEXT_2_MONTH_PWO_AMT
		   ,'QFUND4' AS INSTANCE
		   ,CASE WHEN TYPE = 'TP' THEN 'ECA TITLE'
                 WHEN TYPE = 'PDL' THEN 'ECA PAYDAY'
            END AS PRODUCT_DESC
		   ,CASE WHEN TYPE = 'TP' THEN 'TITLE'
		         WHEN TYPE = 'PDL' THEN 'PAYDAY'
			END AS PRODUCT
		FROM ECA.QF_PWO_REPORT PWO
		LEFT JOIN
		CADA.ECA_LOCATION_XREF XREF
		ON(PWO.STORE_NBR=XREF.BRANCH_ECA)
	WHERE CALCULATED BUSINESSDT >= DHMS(INTNX('MONTH',TODAY(),-24,'B'),00,00,00)
		  AND TYPE IN('TP','PDL')
	GROUP BY 
		CALCULATED LOCNBR
	   ,CALCULATED BUSINESSDT
	   ,CALCULATED PRODUCT_DESC
	   ,CALCULATED PRODUCT
	ORDER BY
		CALCULATED LOCNBR
	   ,CALCULATED PRODUCT_DESC
	   ,CALCULATED BUSINESSDT

;
%RUNQUIT(&job,&sub9);

DATA PWO_QFUND4;
	SET QF4_PWO_PRE;
	BY LOCNBR PRODUCT_DESC;
	IF LENGTH(COMPRESS(PUT(LOCNBR,10.))) = 5 THEN LOCNBR = LOCNBR/100;
	ELSE LOCNBR = LOCNBR;
	IF FIRST.PRODUCT_DESC THEN IND = 'Y';
	ELSE IND = 'N';
	IF DAY(DATEPART(BUSINESSDT)) = 1 OR IND = 'Y'
		THEN 
			DO;
				BEGIN_PWO_AMT = CURRENT_PWO_AMT;
				RETAIN BEGIN_PWO_AMT;
			END;
	BUSINESSDT = DATEPART(BUSINESSDT);
	FORMAT BUSINESSDT MMDDYY10.;
	DROP BEGIN_PWO_AMT_PRE IND;
%RUNQUIT(&job,&sub9);

DATA BEGIN_PWO_AMT;
	SET WORK.PWO_QFUND4;
		MONTH = MONTH(BUSINESSDT);
		YEAR = YEAR(BUSINESSDT);
	WHERE DAY(BUSINESSDT) = 1;
	KEEP LOCNBR BUSINESSDT BEGIN_PWO_AMT MONTH YEAR PRODUCT;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.TBL_TITLEPLEDGE_TMP AS 
   SELECT /* INSTANCE */
            ('QFUND4') LABEL="INSTANCE" AS INSTANCE, 
          t1.STD_ID, 
          /* storenumber */
            (INPUT(t1.STORENUMBER, best32.)) LABEL="storenumber" AS storenumber, 
          /* businessdt */
            (datepart(t1.DATED)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* begindt */
            (intnx('month',today(),-24,'beginning')) FORMAT=mmddyy10. LABEL="begindt" AS begindt, 
          t1.ACTIVEITEMS, 
          t1.NEWLOANS, 
          t1.NEWPRINCIPAL, 
          t1.NEWINTEREST, 
          t1.NEWLATEFEES, 
          t1.NEWTITLELIENFEES, 
          t1.NEWOTHERFEES, 
          t1.ORIGINALLOANAMT, 
          t1.BALANCEPRINCIPAL, 
          t1.BALANCEINTEREST, 
          t1.BALANCEFEES, 
          t1.PASTDUEMIN1, 
          t1.PASTDUEALL1, 
          t1.PASTDUECNT1, 
          t1.PASTDUEMIN2, 
          t1.PASTDUEALL2, 
          t1.PASTDUECNT2, 
          t1.PASTDUEMIN3, 
          t1.PASTDUEALL3, 
          t1.PASTDUECNT3, 
          t1.PASTDUEMIN4, 
          t1.PASTDUEALL4, 
          t1.PASTDUECNT4, 
          t1.PASTDUEMIN5, 
          t1.PASTDUEALL5, 
          t1.PASTDUECNT5, 
          t1.PASTDUEMIN6, 
          t1.PASTDUEALL6, 
          t1.PASTDUECNT6, 
          t1.POSSESSION, 
          t1.POSSESSIONCNT, 
          t1.CHARGEOFF, 
          t1.CHARGEOFFCNT, 
          t1.CHECKSWRITTEN, 
          t1.COLLECTEDFEES, 
          t1.COLLECTEDINTEREST, 
          t1.COLLECTEDPRINCIPAL, 
          t1.TOREPO, 
          t1.TOREPOCNT, 
          t1.DEFAULTAMT, 
          t1.DEFAULTCNT, 
          t1.ONTIMEPMT, 
          t1.DUETODAY, 
          t1.BKWRITEOFFS, 
          t1.BKPAID, 
          t1.NEWCUSTOMERS, 
          t1.TOTALACTIVEPMTS, 
          t1.TOTALDURATIONDAYS, 
          t1.PAIDINFULLCNT, 
          t1.CHARGEOFFRECOVERY, 
          t1.RENEWALCNT, 
          t1.RENEWALPRINCIPAL, 
          t1.ONTIMERENEWALCNT, 
          t1.ONTIMERENEWALPRINCIPAL, 
          t1.TOREPOPRINAMT, 
          t1.DEFAULTPRINAMT, 
          t1.TOREPOFEEAMT, 
          t1.DEFAULTFEEAMT, 
          t1.SALVAGEVALUE, 
          t1.WRITEOFFRECOVERYBRANCH, 
          t1.WRITEOFFRECOVERYADMIN, 
          t1.CHARGEOFFPRIN, 
          t1.BKCHARGEOFFPRIN, 
          t1.NEWCUSTCNT, 
          t1.REFINANCECNT, 
          t1.REFINANCEPRINCIPAL, 
          t1.REFINANCEINTEREST, 
          t1.REFINANCELIENFEE, 
          t1.ONTIMEREFINANCECNT, 
          t1.ONTIMEREFINANCEPRINCIPAL, 
          t1.PASTDUEREFINANCECNT, 
          t1.PASTDUEREFINANCEPRINCIPAL, 
          t1.SALETOTAL, 
          t1.SALEPRINCIPAL, 
          t1.OTHERFEEDISBURSEMENTS, 
          t1.PASSTHROUGHFEES, 
          t1.IB_PMT_CASH, 
          t1.IB_PMT_CC_MO, 
          t1.IB_PMT_CHECK, 
          t1.IB_PMT_DC, 
          t1.IB_PMT_APPLIED, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          t1.ACTIVE_ITEMS_BALANCE_PRIN, 
          t1.ACTIVE_ITEMS_BALANCE_FEES, 
          /* Enddt */
            (intnx('day',TODAY(),-1,'beginning')) FORMAT=mmddyy10. LABEL="Enddt" AS Enddt
      FROM ECA.TBL_TITLEPLEDGE t1
      WHERE (CALCULATED businessdt) BETWEEN (CALCULATED begindt) AND (CALCULATED Enddt);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ecatitledailysummary_tmp1(label="ecatitledailysummary_tmp1") AS 
   SELECT /* Product */
            ("TITLE") LABEL="Product" AS Product, 
          /* pos */
            ("QFUND") LABEL="pos" AS pos, 
          t1.INSTANCE, 
          /* bankmodel */
            ("STANDARD") LABEL="bankmodel" AS bankmodel, 
          /* locnbr */
            (CASE WHEN LENGTH(COMPRESS(PUT(case
              when t2.LOCATION_AA = . then t1.storenumber
              else t2.location_aa
            end,10.))) = 5 THEN (case
              when t2.LOCATION_AA = . then t1.storenumber
              else t2.location_aa
            end)/100 ELSE (case
              when t2.LOCATION_AA = . then t1.storenumber
              else t2.location_aa
            end) END) LABEL="locnbr" AS locnbr, 
          t1.businessdt, 
          t1.begindt, 
          /* ADVCNT */
            (SUM(t1.NEWLOANS)) AS ADVCNT, 
          /* ADVAMTSUM */
            (SUM(t1.NEWPRINCIPAL)) FORMAT=21.4 AS ADVAMTSUM, 
          /* ADVFEEAMT */
            (SUM(sum(t1.NEWINTEREST,t1.NEWOTHERFEES,t1.NEWTITLELIENFEES))) AS ADVFEEAMT, 
          /* AGNCNT */
            (SUM(sum(t1.RENEWALCNT,t1.REFINANCECNT,t1.ONTIMERENEWALCNT))) LABEL="agncnt" AS AGNCNT, 
          /* AGNAMTSUM */
            (SUM(sum(t1.RENEWALPRINCIPAL, t1.REFINANCEPRINCIPAL))) AS AGNAMTSUM, 
          /* AGNFEEAMT */
            (SUM(sum(t1.REFINANCEINTEREST, t1.REFINANCELIENFEE))) AS AGNFEEAMT, 
          /* TOTADVRECV */
            (SUM(CASE WHEN t1.businessdt >= '18JUL2017'D THEN t1.ACTIVE_ITEMS_BALANCE_PRIN ELSE t1.BALANCEPRINCIPAL 
            END)) AS TOTADVRECV, 
          /* TOTADVRECV_OLD */
            (SUM(t1.BALANCEPRINCIPAL)) AS TOTADVRECV_OLD, 
          /* TOTADVRECV_NEW */
            (SUM(t1.ACTIVE_ITEMS_BALANCE_PRIN)) FORMAT=21.4 AS TOTADVRECV_NEW, 
          /* TOTADVFEERECV */
            (SUM(sum(t1.BALANCEINTEREST,t1.BALANCEFEES))) LABEL="totadvfeeredv" AS TOTADVFEERECV, 
          /* HELDCNT */
            (SUM(t1.ACTIVEITEMS)) AS HELDCNT, 
          /* TOTDEFAULTRECV */
            (SUM(t1.DEFAULTAMT)) FORMAT=21.4 AS TOTDEFAULTRECV, 
          /* DEFAULTCNT */
            (SUM(t1.DEFAULTCNT)) AS DEFAULTCNT, 
          /* WOAMTSUM */
            (SUM(t1.CHARGEOFF)) FORMAT=21.4 AS WOAMTSUM, 
          /* WORAMTSUM */
            (SUM(sum(t1.WRITEOFFRECOVERYADMIN,t1.WRITEOFFRECOVERYBRANCH))) LABEL="woramtsum" AS WORAMTSUM, 
          /* WOBAMTSUM */
            (SUM(t1.BKWRITEOFFS)) FORMAT=21.4 AS WOBAMTSUM, 
          /* OPS_EARNEDFEES */
            
            (SUM(sum(t1.NEWINTEREST,NEWLATEFEES,NEWOTHERFEES,NEWTITLELIENFEES,REFINANCEINTEREST,REFINANCELIENFEE,-PASSTHROUGHFEES))) 
            AS OPS_EARNEDFEES, 
          /* EARNEDFEES */
            (SUM(sum(t1.COLLECTEDFEES,t1.COLLECTEDINTEREST))) LABEL="EARNEDFEES" AS EARNEDFEES, 
          /* POSSESSIONAMT */
            (SUM(t1.POSSESSION)) FORMAT=21.4 AS POSSESSIONAMT, 
          /* POSSESSIONCNT */
            (SUM(t1.POSSESSIONCNT)) AS POSSESSIONCNT, 
          /* REFINANCE_CNT */
            (SUM(t1.REFINANCECNT)) AS REFINANCE_CNT, 
          /* SALETOTAL */
            (SUM(t1.SALETOTAL)) FORMAT=21.4 AS SALETOTAL, 
          /* PASTDUEAMT_1 */
            (SUM(t1.PASTDUEALL1)) FORMAT=21.4 AS PASTDUEAMT_1, 
          /* PASTDUECNT_1 */
            (SUM(t1.PASTDUECNT1)) AS PASTDUECNT_1, 
          /* PASTDUEAMT_2 */
            (SUM(t1.PASTDUEALL2)) FORMAT=21.4 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (SUM(t1.PASTDUECNT2)) FORMAT=21.4 AS PASTDUECNT_2, 
          /* PASTDUEAMT_3 */
            (SUM(t1.PASTDUEALL3)) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (SUM(t1.PASTDUECNT3)) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (SUM(t1.PASTDUEALL4)) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (SUM(t1.PASTDUECNT4)) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (SUM(t1.PASTDUEALL5)) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (SUM(t1.PASTDUECNT5)) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (SUM(t1.PASTDUEALL6)) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (SUM(t1.PASTDUECNT6)) AS PASTDUECNT_6
      FROM WORK.TBL_TITLEPLEDGE_TMP t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.storenumber = t2.BRANCH_ECA)
      GROUP BY (CALCULATED Product),
               (CALCULATED pos),
               t1.INSTANCE,
               (CALCULATED bankmodel),
               (CALCULATED locnbr),
               t1.businessdt,
               t1.begindt;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECATITLEDAILYSUMMARY_TM1(label="ECATITLEDAILYSUMMARY_TMP") AS 
   SELECT /* Product */
            ("TITLE") LABEL="Product" AS Product, 
          /* pos */
            ("QFUND") LABEL="pos" AS pos, 
          t4.INSTANCE, 
          t3.BRND_CD AS BRANDCD, 
          /* bankmodel */
            ("STANDARD") LABEL="bankmodel" AS bankmodel, 
          t3.CTRY_CD AS COUNTRYCD, 
          t3.ST_PVC_CD AS STATE, 
          t3.ADR_CITY_NM AS CITY, 
          t3.MAIL_CD AS ZIP, 
          t3.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t3.HIER_ZONE_NBR AS ZONENBR, 
          t3.HIER_ZONE_NM AS ZONENAME, 
          t3.HIER_RGN_NBR AS REGIONNBR, 
          t3.HIER_RDO_NM AS REGIONRDO, 
          t3.HIER_DIV_NBR AS DIVISIONNBR, 
          t3.HIER_DDO_NM AS DIVISIONDDO, 
          /* LOCNBR */
            (CASE WHEN LENGTH(COMPRESS(PUT(LOCNBR, 32.))) = 5 THEN LOCNBR/100 ELSE LOCNBR END) AS LOCNBR, 
          t3.LOC_NM AS Location_Name, 
          t3.OPEN_DT AS LOC_OPEN_DT, 
          t3.CLS_DT AS LOC_CLOSE_DT, 
          t4.businessdt, 
          t4.begindt, 
          /* ADVCNT */
            (SUM(t4.advcnt)) AS ADVCNT, 
          /* ADVAMTSUM */
            (SUM(t4.advamtsum)) FORMAT=21.4 AS ADVAMTSUM, 
          /* ADVFEEAMT */
            (SUM(t4.ADVFEEAMT)) AS ADVFEEAMT, 
          /* AGNCNT */
            (SUM(t4.agncnt)) AS AGNCNT, 
          /* AGNAMTSUM */
            (SUM(t4.agnamtsum)) AS AGNAMTSUM, 
          /* AGNFEEAMT */
            (SUM(t4.AGNFEEAMT)) AS AGNFEEAMT, 
          /* TOTADVRECV */
            (SUM(t4.totadvrecv)) FORMAT=21.4 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (SUM(t4.totadvfeerecv)) AS TOTADVFEERECV, 
          /* HELDCNT */
            (SUM(t4.heldcnt)) AS HELDCNT, 
          /* DEFAULTCNT */
            (SUM(t4.DEFAULTCNT)) AS DEFAULTCNT, 
          /* EARNEDFEES */
            (SUM(t4.EARNEDFEES)) AS EARNEDFEES, 
          /* OPS_EARNEDFEES */
            (SUM(t4.OPS_EARNEDFEES)) AS OPS_EARNEDFEES, 
          /* TOTDEFAULTRECV */
            (SUM(t4.totdefaultrecv)) FORMAT=21.4 AS TOTDEFAULTRECV, 
          /* WOAMTSUM */
            (SUM(t4.woamtsum)) FORMAT=21.4 AS WOAMTSUM, 
          /* WOBAMTSUM */
            (SUM(t4.wobamtsum)) FORMAT=21.4 AS WOBAMTSUM, 
          /* WORAMTSUM */
            (SUM(t4.woramtsum)) AS WORAMTSUM, 
          /* substituterow */
            ('N') AS substituterow, 
          /* REFINANCE_CNT */
            (SUM(t4.REFINANCE_CNT)) AS REFINANCE_CNT, 
          /* POSSESSIONAMT */
            (SUM(t4.POSSESSIONAMT)) FORMAT=21.4 AS POSSESSIONAMT, 
          /* POSSESSIONCNT */
            (SUM(t4.POSSESSIONCNT)) AS POSSESSIONCNT, 
          /* PASTDUEAMT_1 */
            (SUM(t4.PASTDUEAMT_1)) FORMAT=21.4 AS PASTDUEAMT_1, 
          /* PASTDUECNT_1 */
            (SUM(t4.PASTDUECNT_1)) AS PASTDUECNT_1, 
          /* PASTDUEAMT_2 */
            (SUM(t4.PASTDUEAMT_2)) FORMAT=21.4 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (SUM(t4.PASTDUECNT_2)) FORMAT=21.4 AS PASTDUECNT_2, 
          /* PASTDUEAMT_3 */
            (SUM(t4.PASTDUEAMT_3)) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (SUM(t4.PASTDUECNT_3)) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (SUM(t4.PASTDUEAMT_4)) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (SUM(t4.PASTDUECNT_4)) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (SUM(t4.PASTDUEAMT_5)) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (SUM(t4.PASTDUECNT_5)) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (SUM(t4.PASTDUEAMT_6)) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (SUM(t4.PASTDUECNT_6)) AS PASTDUECNT_6, 
          /* SALETOTAL */
            (SUM(t4.SALETOTAL)) FORMAT=21.4 AS SALETOTAL
      FROM EDW.D_LOCATION t3
           RIGHT JOIN WORK.ECATITLEDAILYSUMMARY_TMP1 t4 ON (t3.LOC_NBR = t4.locnbr)
      GROUP BY (CALCULATED Product),
               (CALCULATED pos),
               t4.INSTANCE,
               t3.BRND_CD,
               (CALCULATED bankmodel),
               t3.CTRY_CD,
               t3.ST_PVC_CD,
               t3.ADR_CITY_NM,
               t3.MAIL_CD,
               t3.BUSN_UNIT_ID,
               t3.HIER_ZONE_NBR,
               t3.HIER_ZONE_NM,
               t3.HIER_RGN_NBR,
               t3.HIER_RDO_NM,
               t3.HIER_DIV_NBR,
               t3.HIER_DDO_NM,
               (CALCULATED LOCNBR),
               t3.LOC_NM,
               t3.OPEN_DT,
               t3.CLS_DT,
               t4.businessdt,
               t4.begindt,
               (CALCULATED substituterow);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.BB_VALUES AS 
   SELECT t1.BRANCH_NBR AS locnbr, 
          /* businessdt */
            (datepart(t1.LOAN_DATE)) FORMAT=mmddyy10. AS businessdt, 
          /* SUM_of_BB_Value */
            (SUM(t1.VALUE_USED*(t1.PERCENT_USED*.01))) AS SUM_of_BB_Value
      FROM ECA.QF_TP_LOAN_DATA t1
      GROUP BY t1.BRANCH_NBR,
               (CALCULATED businessdt);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.QF4_TLP_PNL AS 
   SELECT /* LOCNBR */
            (case
              when t2.LOCATION_AA = . then t1.store_number
              else t2.location_aa
            end) AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* GROSS_WRITE_OFF */
            (SUM(t1.BAD_DEBT)) FORMAT=12.2 AS GROSS_WRITE_OFF, 
          /* WOR */
            (SUM(t1.BADDEBT_PMT)) FORMAT=12.2 AS WOR, 
          /* GROSS_REVENUE */
            (SUM(t1.PNL_AMT)) FORMAT=12.2 AS GROSS_REVENUE
      FROM EDW.QF_BADDEBT_PNLAMT t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.STORE_NUMBER = t2.BRANCH_ECA)
      WHERE t1.SOURCE_SYSTEM = 'QFUND4' AND (COMPRESS(t1.PRODUCT_TYPE)) IN 
           (
           'TLP',
           'VATLP'
           )
      GROUP BY (CALCULATED LOCNBR),
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.QF4_PDL_PNL AS 
   SELECT /* LOCNBR */
            (case
              when t2.LOCATION_AA = . then t1.store_number
              else t2.location_aa
            end) AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESS_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* GROSS_WRITE_OFF */
            (SUM(t1.BAD_DEBT)) FORMAT=12.2 AS GROSS_WRITE_OFF, 
          /* WOR */
            (SUM(t1.BADDEBT_PMT)) FORMAT=12.2 AS WOR, 
          /* GROSS_REVENUE */
            (SUM(t1.PNL_AMT)) FORMAT=12.2 AS GROSS_REVENUE
      FROM EDW.QF_BADDEBT_PNLAMT t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.STORE_NUMBER = t2.BRANCH_ECA)
      WHERE t1.SOURCE_SYSTEM = 'QFUND4' AND (COMPRESS(t1.PRODUCT_TYPE)) = 'PDL'
      GROUP BY (CALCULATED LOCNBR),
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.tbl_main_tmp AS 
   SELECT /* INSTANCE */
            ('QFUND4') LABEL="INSTANCE" AS INSTANCE, 
          /* businessdt */
            (datepart(t1.DATED)) FORMAT=mmddyy10. LABEL="businessdt" AS businessdt, 
          /* begindt */
            (intnx('month',today(),-24,'beginning')) FORMAT=mmddyy10. LABEL="begindt" AS begindt, 
          /* storenumber */
            (input(t1.storenumber,best32.)) LABEL="storenumber" AS storenumber, 
          t1.STD_ID, 
          t1.DIVISION, 
          t1.ACCRUEDCHECKFEES, 
          t1.ACCRUEDTAX, 
          t1.ACTIVEITEMS, 
          t1.BEGINNINGBALANCE, 
          t1.CASHDRAWER, 
          t1.MISCREC, 
          t1.CASHIN, 
          t1.DATED, 
          t1.DEPOSIT, 
          t1.DISBURSEMENTS, 
          t1.DURATIONITEMCNT, 
          t1.EARNEDCHECKFEES, 
          t1.EARNEDFEESTAX, 
          t1.GROSSCASHEDCHECKS, 
          t1.GROSSDEPOSITCHECKS, 
          t1.LATE_1, 
          t1.LATE_2, 
          t1.LATE_3, 
          t1.LATE_4, 
          t1.LATE_5, 
          t1.LATECHARGE, 
          t1.NETBUYBACKS, 
          t1.NETCASHEDCHECKS, 
          t1.NETDEPOSITCHECKS, 
          t1.NSFPAID, 
          t1.NSFRECEIVABLES, 
          t1.NSFWRITEOFFS, 
          t1.OTHERFEES, 
          t1.OVERDUEITEMS, 
          t1.REBATEDFEESCK, 
          t1.SHORTOVER, 
          t1.TOTALBUYBACKS, 
          t1.TOTALCASHAGAINS, 
          t1.TOTALCASHEDCHECKS, 
          t1.TOTALCHECKSRECEIVABLE, 
          t1.TOTALDEPOSITCHECKS, 
          t1.TOTALDURATIONDAYS, 
          t1.TOTALFEESRECEIVABLE, 
          t1.TOTALOUTNSF, 
          t1.TOTALOVERDUE, 
          t1.TOTALTAXRECEIVABLE, 
          t1.UCLATECHARGE, 
          t1.WORKDAY, 
          t1.NEWCUSTOMERADVANCES, 
          t1.ONTIMEBB, 
          t1.DUETODAY, 
          t1.BANKRUPTCY, 
          t1.BKWRITEOFFS, 
          t1.BKRECEIVABLES, 
          t1.BKPAID, 
          t1.SALESTAX, 
          t1.NATIONALCITY, 
          t1.MANUALNSF, 
          t1.ACHDEPOSITCNT, 
          t1.ACHNETDEPOSIT, 
          t1.ACHGROSSDEPOSIT, 
          t1.ACHDEPOSITFEES, 
          t1.ACHRETURNDEPCNT, 
          t1.ACHRETURNDEPAMT, 
          t1.ACHAMOUNT, 
          t1.ACHRCKCNT, 
          t1.ACHGROSSRCK, 
          t1.ACHRETURNRCKCNT, 
          t1.ACHRETURNRCKAMT, 
          t1.TOTALACTIVEWRITEOFFS, 
          t1.ACHCLEARCNT, 
          t1.WRITEOFFRECOVERYBRANCH, 
          t1.WRITEOFFRECOVERYADMIN, 
          t1.RETURNFEESCHARGED, 
          t1.RETURNFEESCOLLECTED, 
          t1.RETURNFEESBALANCE, 
          t1.EODHOLDOVER, 
          t1.TOTALCASHRECEIVABLE, 
          t1.TOTALUNAPPNSFPREPAY, 
          t1.CAPITALONE, 
          t1.RETURNFEESCHARGEDOFF, 
          t1.NEWCUSTCNT, 
          t1.SETTLEMENTAMT, 
          t1.CHECK21CNT, 
          t1.CHECK21NET, 
          t1.CHECK21GROSS, 
          t1.CHECK21FEES, 
          t1.CHECK21RETURNEDCNT, 
          t1.CHECK21RETURNEDAMT, 
          t1.CHECK21CLEARCNT, 
          t1.CHECK21REJECTEDCNT, 
          t1.CHECK21REJECTEDAMT, 
          t1.IB_PMT_CASH, 
          t1.IB_PMT_CC_MO, 
          t1.IB_PMT_CHECK, 
          t1.IB_PMT_DC, 
          t1.IB_PMT_APPLIED, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          /* Enddt */
            (intnx('day',TODAY(),-1,'beginning')) FORMAT=mmddyy10. LABEL="Enddt" AS Enddt
      FROM ECA.TBL_MAIN t1
      WHERE (CALCULATED businessdt) BETWEEN (CALCULATED begindt) AND (CALCULATED Enddt);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECAPAYDAYDAILYSUMMARY_TMP1(label="ECAPAYDAYDAILYSUMMARY_TMP1") AS 
   SELECT /* Product */
            ("PAYDAY") LABEL="Product" AS Product, 
          /* pos */
            ("QFUND") LABEL="pos" AS pos, 
          t1.INSTANCE, 
          /* bankmodel */
            ("STANDARD") LABEL="bankmodel" AS bankmodel, 
          t3.BRND_CD AS BRANDCD, 
          t3.CTRY_CD AS COUNTRYCD, 
          t3.ST_PVC_CD AS STATE, 
          t3.ADR_CITY_NM AS CITY, 
          t3.MAIL_CD AS ZIP, 
          t3.BUSN_UNIT_ID AS BUSINESS_UNIT, 
          t3.HIER_ZONE_NBR AS ZONENBR, 
          t3.HIER_ZONE_NM AS ZONENAME, 
          t3.HIER_RGN_NBR AS REGIONNBR, 
          t3.HIER_RDO_NM AS REGIONRDO, 
          t3.HIER_DIV_NBR AS DIVISIONNBR, 
          t3.HIER_DDO_NM AS DIVISIONDDO, 
          t2.LOCATION_AA AS locnbr, 
          t3.LOC_NM AS Location_Name, 
          t3.OPEN_DT AS LOC_OPEN_DT, 
          t3.CLS_DT AS LOC_CLOSE_DT, 
          t1.businessdt, 
          t1.begindt, 
          t1.TOTALCASHEDCHECKS AS advcnt, 
          t1.TOTALCASHAGAINS AS agncnt, 
          t1.NETCASHEDCHECKS AS advamtsum, 
          t1.TOTALCASHRECEIVABLE AS totadvrecv, 
          t1.TOTALFEESRECEIVABLE AS totadvfeerecv, 
          /* LATE_ALL */
            (sum(t1.LATE_1,t1.LATE_2,t1.LATE_3,t1.LATE_4,t1.LATE_5)) AS LATE_ALL, 
          t1.ACTIVEITEMS AS heldcnt, 
          /* EARNEDFEES */
            (sum(t1.EARNEDCHECKFEES)) LABEL="EARNEDFEES" AS EARNEDFEES, 
          /* OPS_EARNEDFEES */
            (SUM(T1.ACCRUEDCHECKFEES,T1.RETURNFEESCHARGED,-REBATEDFEESCK)) AS OPS_EARNEDFEES, 
          t1.TOTALOUTNSF AS totdefaultrecv, 
          t1.RETURNFEESBALANCE AS totdefaultfeerecv, 
          t1.TOTALCASHAGAINS AS REFINANCE_CNT, 
          t1.NSFWRITEOFFS AS woamtsum, 
          t1.BKWRITEOFFS AS wobamtsum, 
          /* woramtsum */
            (sum(t1.WRITEOFFRECOVERYADMIN,t1.WRITEOFFRECOVERYBRANCH)) LABEL="woramtsum" AS woramtsum, 
          t1.TOTALBUYBACKS, 
          t1.TOTALCASHAGAINS, 
          t1.TOTALDEPOSITCHECKS
      FROM WORK.TBL_MAIN_TMP t1, CADA.ECA_LOCATION_XREF t2, EDW.D_LOCATION t3
      WHERE (t1.storenumber = t2.BRANCH_ECA AND t3.LOC_NBR = t2.LOCATION_AA) AND t3.ST_PVC_CD NOT IS MISSING
      GROUP BY (CALCULATED Product),
               (CALCULATED pos),
               t1.INSTANCE,
               (CALCULATED bankmodel),
               t3.BRND_CD,
               t3.CTRY_CD,
               t3.ST_PVC_CD,
               t3.ADR_CITY_NM,
               t3.MAIL_CD,
               t3.BUSN_UNIT_ID,
               t3.HIER_ZONE_NBR,
               t3.HIER_ZONE_NM,
               t3.HIER_RGN_NBR,
               t3.HIER_RDO_NM,
               t3.HIER_DIV_NBR,
               t3.HIER_DDO_NM,
               t2.LOCATION_AA,
               t3.LOC_NM,
               t3.OPEN_DT,
               t3.CLS_DT,
               t1.businessdt,
               t1.begindt,
               t1.TOTALCASHEDCHECKS,
               t1.TOTALCASHAGAINS,
               t1.NETCASHEDCHECKS,
               t1.TOTALCASHRECEIVABLE,
               t1.TOTALFEESRECEIVABLE,
               (CALCULATED LATE_ALL),
               t1.ACTIVEITEMS,
               (CALCULATED OPS_EARNEDFEES),
               t1.TOTALOUTNSF,
               t1.RETURNFEESBALANCE,
               t1.NSFWRITEOFFS,
               t1.BKWRITEOFFS,
               (CALCULATED woramtsum),
               t1.TOTALBUYBACKS,
               t1.TOTALDEPOSITCHECKS;
%RUNQUIT(&job,&sub9);

data WORK.eca_defaultedloans (keep =  branch_nbr loan_nbr transaction_date transaction_nbr defaultdt)
	 WORK.ECA_voidednsftrans (drop=transaction_nbr rename=(ref_tran_code = transaction_nbr) keep = branch_nbr loan_nbr transaction_date ref_tran_code transaction_nbr defaultdt);
	set ECA.QF_TRANSACTION_DATA (where=(transaction_type = 'NSF'));
	defaultdt = datepart(transaction_date);
	if void_flag = 'N' then output work.eca_defaultedloans;
	else output work.eca_voidednsftrans;
	format defaultdt mmddyy10.;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_nonvoidednsfloans AS 
   SELECT t1.BRANCH_NBR, 
          t1.LOAN_NBR, 
          t1.TRANSACTION_DATE, 
          t1.defaultdt, 
          t1.TRANSACTION_NBR
      FROM WORK.ECA_DEFAULTEDLOANS t1
           LEFT JOIN WORK.ECA_VOIDEDNSFTRANS t2 ON (t1.BRANCH_NBR = t2.BRANCH_NBR) AND (t1.LOAN_NBR = t2.LOAN_NBR) AND 
          (t1.TRANSACTION_NBR = t2.TRANSACTION_NBR)
      WHERE t2.BRANCH_NBR IS MISSING;
%RUNQUIT(&job,&sub9);

libname eca2 oracle schema=ECA user=svc_sasuser pw="October132007!!" path=EDWPRD dbsliceparm=(ALL,4);

data work.ecatrans;
	set eca2.QF_TRANSACTION_DATA;
	*keep CUSTOMER_NBR LOAN_NBR BRANCH_NBR TRANSACTION_DATE TRANSACTION_TYPE TOTAL_AMOUNT_DUE
		ADV_AMT ADV_FEE_AMT TRANSACTION_NBR VOID_FLAG REF_TRAN_CODE;
%RUNQUIT(&job,&sub9);

data work.ecatranstp;
	set eca2.QF_TP_TRANSACTION_DATA;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.npnpptrans AS 
   SELECT t1.CUSTOMER_NBR, 
          t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t2.LOCATION_AA, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.ADV_AMT, 
          t1.ADV_FEE_AMT, 
          t1.REBATE_AMT, 
          t1.WAIVE_FEE_AMT, 
          t1.NSF_AMT, 
          t1.NSF_FEE_AMT, 
          t1.WO_AMT, 
          t1.WO_FEE_AMT, 
          t1.NSF_PREPAYMENT_AMT, 
          t1.REF_AMT, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.INST_NBR, 
          t1.ADV_INT_AMT, 
          t1.ADV_MMF_AMT, 
          t1.LOAN_TYPE, 
          t1.INT_REBATE, 
          t1.MMF_REBATE, 
          t1.LATECHARGE, 
          t1.UCLATECHARGE, 
          t1.RTN_SOURCE, 
          t1.IS_CHECK21, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM, 
          /* begindt */
            (dhms(intnx('month',today(),-24,'beginning'),0,0,0)) FORMAT=datetime20. AS begindt
      FROM WORK.ECATRANS t1
           INNER JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
      WHERE t1.TRANSACTION_DATE >= (CALCULATED begindt) AND t1.TRANSACTION_TYPE IN 
           (
           'NP',
           'NPP',
           'NSF',
           'REF'
           );
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.npnppsummary AS 
   SELECT /* businessdt */
            (datepart(t1.transaction_date)) FORMAT=mmddyy10. AS businessdt, 
          t1.LOCATION_AA AS locnbr, 
          /* NSFAMTSUM */
            (SUM(case
             when transaction_type = "NSF" then nsf_amt
              else 0
            end)) AS NSFAMTSUM, 
          /* NPAMTSUM */
            (SUM(case
             when transaction_type = "NP" then nsf_amt * -1
              else 0
            end)) AS NPAMTSUM, 
          /* NPPAMTSUM */
            (SUM(case
             when transaction_type = "NPP" then nsf_prepayment_amt * -1
              else 0
            end)) AS NPPAMTSUM
      FROM WORK.NPNPPTRANS t1
      GROUP BY (CALCULATED businessdt),
               t1.LOCATION_AA
      ORDER BY businessdt,
               t1.LOCATION_AA;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_defaultloantrans AS 
   SELECT t2.CUSTOMER_NBR, 
          t2.LOAN_NBR, 
          t1.defaultdt, 
          t2.BRANCH_NBR, 
          t2.TRANSACTION_DATE, 
          t2.TRANSACTION_TYPE, 
          t2.TOTAL_AMOUNT_DUE, 
          t2.ADV_AMT, 
          t2.ADV_FEE_AMT, 
          t2.TRANSACTION_NBR, 
          t2.VOID_FLAG
      FROM WORK.ECA_NONVOIDEDNSFLOANS t1
           INNER JOIN WORK.ECATRANS t2 ON (t1.LOAN_NBR = t2.LOAN_NBR)
      WHERE t2.TRANSACTION_DATE >= t1.TRANSACTION_DATE
      ORDER BY t1.LOAN_NBR,
               t2.TRANSACTION_DATE,
               t2.TRANSACTION_NBR;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_defaultcountbyloan AS 
   SELECT DISTINCT t1.LOAN_NBR, 
          t1.defaultdt, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TRANSACTION_NBR, 
          t1.TOTAL_AMOUNT_DUE, 
          /* defaultcnt */
            (case
              when t1.TOTAL_AMOUNT_DUE <= 0 or t1.TRANSACTION_TYPE in ('WO', 'WOB', 'WOD', 'WOR', 'WOT') then 0 
              else 1 
            end) LABEL="defaultcnt" AS defaultcnt
      FROM WORK.ECA_DEFAULTLOANTRANS t1
      ORDER BY t1.LOAN_NBR,
               t1.TRANSACTION_DATE,
               t1.TRANSACTION_NBR;
%RUNQUIT(&job,&sub9);

data work.eca_defaultcountenddate;
	set work.eca_defaultcountbyloan (where=(datepart(transaction_date) >= intnx('month',today(),-24,'beginning')));
	by loan_nbr;
	output;
	if last.loan_nbr then do;
		if defaultcnt = 1 then do;
			transaction_type = "LAST";
			defaultcnt = 1;
			transaction_nbr = transaction_nbr + 1;
			transaction_date = dhms(today() - 1, 0,0,0);
			output;
		end;
	end;
%RUNQUIT(&job,&sub9);

proc sort data=work.eca_defaultcountenddate;
	by loan_nbr transaction_date;
%RUNQUIT(&job,&sub9);

data work.eca_defaultcountenddate_tmp1;
	set work.eca_defaultcountenddate;
	by loan_nbr transaction_date;
	trandt = datepart(transaction_date);
	if last.transaction_date;
	if trandt >= intnx('month',today(),-24,'beginning');
	format trandt mmddyy10.;
%RUNQUIT(&job,&sub9);

proc timeseries data=work.eca_defaultcountenddate_tmp1 out=work.defaultcountenddate_TS;
	id trandt interval=DAY ZEROMISS=NONE;
	VAR defaultcnt / ACCUMULATE=NONE SETMISSING=PREVIOUS;
	by LOAN_NBR;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_defaultcountbyday AS 
   SELECT t2.BRANCH_NBR, 
          t1.LOAN_NBR, 
          t1.trandt, 
          t1.defaultcnt
      FROM WORK.DEFAULTCOUNTENDDATE_TS t1
           INNER JOIN WORK.ECA_NONVOIDEDNSFLOANS t2 ON (t1.LOAN_NBR = t2.LOAN_NBR);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_defaultcountbyday_tmp1 AS 
   SELECT t1.BRANCH_NBR AS locnbr, 
          t1.trandt, 
          /* defaultcnt */
            (SUM(t1.defaultcnt)) LABEL="defaultcnt" AS defaultcnt
      FROM WORK.ECA_DEFAULTCOUNTBYDAY t1
      GROUP BY t1.BRANCH_NBR,
               t1.trandt;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_defaultedloans_tmp2 AS 
   SELECT t1.trandt, 
          t1.defaultcnt, 
          /* locnbr */
            (case when t2.location_aa = . then t1.locnbr
              else t2.location_aa
            end) LABEL="locnbr" AS locnbr
      FROM WORK.ECA_DEFAULTCOUNTBYDAY_TMP1 t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.locnbr = t2.BRANCH_ECA);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECAPAYDAYDAILYSUMMARY_TM1(label="ECAPAYDAYDAILYSUMMARY_TMP") AS 
   SELECT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.bankmodel, 
          t1.BRANDCD, 
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
          t1.locnbr, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.businessdt, 
          t1.begindt, 
          t1.advcnt, 
          t1.agncnt, 
          t1.advamtsum, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.LATE_ALL AS PASTDUECNT_1, 
          t1.heldcnt, 
          t1.REFINANCE_CNT, 
          t1.EARNEDFEES, 
          t1.OPS_EARNEDFEES, 
          t2.defaultcnt, 
          t1.totdefaultrecv, 
          t1.totdefaultfeerecv, 
          t1.woamtsum, 
          t1.wobamtsum, 
          t1.woramtsum, 
          t1.TOTALBUYBACKS, 
          t1.TOTALCASHAGAINS, 
          t1.TOTALDEPOSITCHECKS, 
          /* substituterow */
            ('N') AS substituterow
      FROM WORK.ECAPAYDAYDAILYSUMMARY_TMP1 t1
           LEFT JOIN WORK.ECA_DEFAULTEDLOANS_TMP2 t2 ON (t1.locnbr = t2.locnbr) AND (t1.businessdt = t2.trandt);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS_PRE AS 
   SELECT DISTINCT t1.CUSTOMER_NBR, 
          t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.ADV_AMT, 
          t1.ADV_FEE_AMT, 
          t1.REBATE_AMT, 
          t1.WAIVE_FEE_AMT, 
          t1.NSF_AMT, 
          t1.NSF_FEE_AMT, 
          t1.WO_AMT, 
          t1.WO_FEE_AMT, 
          t1.NSF_PREPAYMENT_AMT, 
          t1.REF_AMT, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.INST_NBR, 
          t1.ADV_INT_AMT, 
          t1.ADV_MMF_AMT, 
          t1.LOAN_TYPE, 
          t1.INT_REBATE, 
          t1.MMF_REBATE, 
          t1.LATECHARGE, 
          t1.UCLATECHARGE, 
          t1.RTN_SOURCE, 
          t1.IS_CHECK21, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM
      FROM WORK.ECATRANS t1
      WHERE t1.TRANSACTION_TYPE = 'NP';
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS AS 
   SELECT /* LOCNBR */
            (case
              when t2.LOCATION_AA = . then t1.BRANCH_NBR
              else t2.location_aa
            end) AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_PMT */
            (SUM(-t1.NSF_AMT)) AS DEFAULT_PMT
      FROM WORK.DEFAULT_PMTS_PRE t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
      GROUP BY (CALCULATED LOCNBR),
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT_PRE AS 
   SELECT t1.CUSTOMER_NBR, 
          t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.ADV_AMT, 
          t1.ADV_FEE_AMT, 
          t1.REBATE_AMT, 
          t1.WAIVE_FEE_AMT, 
          t1.NSF_AMT, 
          t1.NSF_FEE_AMT, 
          t1.WO_AMT, 
          t1.WO_FEE_AMT, 
          t1.NSF_PREPAYMENT_AMT, 
          t1.REF_AMT, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.INST_NBR, 
          t1.ADV_INT_AMT, 
          t1.ADV_MMF_AMT, 
          t1.LOAN_TYPE, 
          t1.INT_REBATE, 
          t1.MMF_REBATE, 
          t1.LATECHARGE, 
          t1.UCLATECHARGE, 
          t1.RTN_SOURCE, 
          t1.IS_CHECK21, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM
      FROM WORK.ECATRANS t1
      WHERE t1.TRANSACTION_TYPE = 'NSF'
      ORDER BY t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT AS 
   SELECT /* LOCNBR */
            (case
              when t2.LOCATION_AA = . then t1.BRANCH_NBR
              else t2.location_aa
            end) AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_AMT */
            (SUM(t1.TOTAL_AMOUNT_DUE)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* DEFAULT_CNT */
            (COUNT(t1.LOAN_NBR)) AS DEFAULT_CNT
      FROM WORK.DEFAULT_AMT_CNT_PRE t1
           LEFT JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
      GROUP BY (CALCULATED LOCNBR),
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS_PRE_TP AS 
   SELECT t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.CUSTOMER_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.BAL_PRINCIPAL_AMT, 
          t1.BAL_LIEN_FEE_AMT, 
          t1.BAL_INT_AMOUNT, 
          t1.REPO_CHARGE, 
          t1.STORAGE_COST, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.TITLE_LOAN_NBR, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.LATE_FEE, 
          t1.OTHER_FEE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM
      FROM WORK.ECATRANSTP t1
      WHERE t1.TRANSACTION_TYPE = 'DFP';
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_PMTS_TP AS 
   SELECT t1.BRANCH_NBR AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_PMT */
            (SUM(t1.TRANSACTION_AMT)) FORMAT=10.2 AS DEFAULT_PMT
      FROM WORK.DEFAULT_PMTS_PRE_TP t1
      GROUP BY t1.BRANCH_NBR,
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT_PRE_TP AS 
   SELECT t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.CUSTOMER_NBR, 
          t1.TRANSACTION_DATE, 
          t1.TRANSACTION_TYPE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.BAL_PRINCIPAL_AMT, 
          t1.BAL_LIEN_FEE_AMT, 
          t1.BAL_INT_AMOUNT, 
          t1.REPO_CHARGE, 
          t1.STORAGE_COST, 
          t1.CREATED_BY, 
          t1.DATE_CREATED, 
          t1.TRANSACTION_NBR, 
          t1.VOID_FLAG, 
          t1.REF_TRAN_CODE, 
          t1.TITLE_LOAN_NBR, 
          t1.IS_DECEASED, 
          t1.TRANSACTION_AMT, 
          t1.IS_CSR, 
          t1.LATE_FEE, 
          t1.OTHER_FEE, 
          t1.CREATE_DATE_TIME, 
          t1.UPDATE_DATE_TIME, 
          t1.CREATE_USER_NM, 
          t1.UPDATE_USER_NM, 
          t1.CREATE_PROGRAM_NM, 
          t1.UPDATE_PROGRAM_NM
      FROM WORK.ECATRANSTP t1
      WHERE t1.TRANSACTION_TYPE = 'DEF'
      ORDER BY t1.TRANSACTION_DATE;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DEFAULT_AMT_CNT_TP AS 
   SELECT t1.BRANCH_NBR AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.TRANSACTION_DATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* DEFAULT_AMT */
            (SUM(t1.TOTAL_AMOUNT_DUE)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* DEFAULT_CNT */
            (COUNT(t1.LOAN_NBR)) AS DEFAULT_CNT
      FROM WORK.DEFAULT_AMT_CNT_PRE_TP t1
      GROUP BY t1.BRANCH_NBR,
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

data work.qf_payday_loan_data;
	set eca2.QF_payday_loan_data (where=(datepart(loan_date) >= intnx('month',today(),-36,'beginning')));
%RUNQUIT(&job,&sub9);

data work.qf_tp_loan_data;
	set eca2.QF_tp_loan_data (where=(datepart(loan_date) >= intnx('month',today(),-36,'beginning')));
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_originations2years AS 
   SELECT t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.LOAN_DUE, 
          t1.LOAN_AMOUNT, 
          t1.LOAN_FEE, 
          t1.LOAN_TOTAL_DUE, 
          t1.LOAN_INTEREST, 
          t1.LOAN_MMF, 
          t1.PWO_AMT
      FROM WORK.QF_PAYDAY_LOAN_DATA t1
      WHERE datepart(t1.LOAN_DATE) >= (intnx('month',today(),-36,'beginning')) AND t1.LOAN_STATUS NOT = 'V';
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ADVFEEAMT AS 
   SELECT t2.LOCATION_AA AS locnbr, 
          /* businessdt */
            (datepart(t1.LOAN_DATE)) FORMAT=mmddyy10. AS businessdt, 
          /* ADVFEEAMT */
            (SUM(sum(t1.LOAN_FEE,t1.LOAN_INTEREST,t1.LOAN_MMF))) AS ADVFEEAMT
      FROM WORK.ECA_ORIGINATIONS2YEARS t1
           INNER JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
      GROUP BY t2.LOCATION_AA,
               (CALCULATED businessdt);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_transactions AS 
   SELECT t1.LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.LOAN_DUE, 
          t1.LOAN_AMOUNT, 
          t1.LOAN_FEE, 
          t1.LOAN_TOTAL_DUE, 
          t1.LOAN_INTEREST, 
          t1.LOAN_MMF, 
          t2.TRANSACTION_DATE, 
          /* trandt */
            (datepart(t2.TRANSACTION_DATE)) FORMAT=mmddyy10. LABEL="trandt" AS trandt, 
          t2.TRANSACTION_TYPE, 
          t2.VOID_FLAG, 
          t2.TRANSACTION_NBR, 
          t2.REF_TRAN_CODE, 
          t2.TOTAL_AMOUNT_DUE, 
          t1.PWO_AMT
      FROM WORK.ECA_ORIGINATIONS2YEARS t1
           INNER JOIN WORK.ECATRANS t2 ON (t1.LOAN_NBR = t2.LOAN_NBR)
      ORDER BY t1.LOAN_NBR,
               t2.TRANSACTION_DATE,
               t2.TRANSACTION_NBR;
%RUNQUIT(&job,&sub9);

data work.ecadurationdates;
	set work.eca_transactions;
	by loan_nbr;
	retain close_date deposit_date wob_date wo_date;
	if first.loan_nbr then do;
		close_date = .;
		deposit_date = .;
		wob_date = .;
		wo_date = .;
	end;
	if transaction_type in ('BUY','CAB') AND close_date = . then close_date = transaction_date;
	if transaction_type = 'WOB' AND wob_date = . then wob_date = transaction_date;
	if transaction_type = 'WO' AND wo_date = . then wo_date = transaction_date;
 	if transaction_type = 'DP' AND deposit_date = . or (transaction_type = 'ACHD' and deposit_date = .) then deposit_date = transaction_date;
	if total_amount_due = 0 and close_date = . and deposit_date = . and wob_date = . and wo_date = . then
		close_date = transaction_date;
	duration_event_date = min(close_date, deposit_date, wob_date, wo_date);
	if duration_event_date = close_date then close_duration_days = datepart(duration_event_date) - datepart(loan_date);
	if duration_event_date = deposit_date then deposit_duration_days = datepart(duration_event_date) - datepart(loan_date);
	if duration_event_date = wob_date then wob_duration_days = datepart(duration_event_date) - datepart(loan_date);
	if duration_event_date = wo_date then wo_duration_days = datepart(duration_event_date) - datepart(loan_date);
	businessdt = datepart(duration_event_date);
	format close_date deposit_date wob_date wo_date duration_event_date datetime20. businessdt mmddyy10.;
	if close_duration_days >= 1 then close_duration_cnt = 1;
	if deposit_duration_days >= 1 then deposit_duration_cnt = 1;
	if wob_duration_days >= 1 then wob_duration_cnt = 1;
	if wo_duration_days >= 1 then wo_duration_cnt = 1;
	duration = datepart(duration_event_date) - datepart(loan_date);
	if duration <= 365;
	if last.loan_nbr;
	if close_duration_days ~= 0;
	keep loan_nbr close_date deposit_date wob_date wo_date duration_event_date
		close_duration_days deposit_duration_days wob_duration_days wo_duration_days businessdt
		close_duration_cnt deposit_duration_cnt wob_duration_cnt wo_duration_cnt branch_nbr
		loan_amount loan_fee duration pwo_amt;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.eca_aggregate_duration AS 
   SELECT t1.businessdt, 
          t2.LOCATION_AA AS locnbr, 
          /* repaid_duration_count */
            (SUM(t1.close_duration_cnt)) AS repaid_duration_count, 
          /* repaid_duration_days */
            (SUM(t1.close_duration_days)) AS repaid_duration_days, 
          /* deposit_duration_count */
            (SUM(t1.deposit_duration_cnt)) AS deposit_duration_count, 
          /* deposit_duration_days */
            (SUM(t1.deposit_duration_days)) AS deposit_duration_days, 
          /* wob_duration_count */
            (SUM(t1.wob_duration_cnt)) AS wob_duration_count, 
          /* wob_duration_days */
            (SUM(t1.wob_duration_days)) AS wob_duration_days, 
          /* wo_duration_count */
            (SUM(t1.wo_duration_cnt)) AS wo_duration_count, 
          /* wo_duration_days */
            (SUM(t1.wo_duration_days)) AS wo_duration_days, 
          /* actual_duration_advamt */
            (SUM(t1.LOAN_AMOUNT)) FORMAT=16.2 AS actual_duration_advamt, 
          /* actual_duration_fees */
            (SUM(t1.LOAN_FEE)) FORMAT=16.2 AS actual_duration_fees, 
          /* SUM_of_PWO_AMT */
            (SUM(t1.PWO_AMT)) AS SUM_of_PWO_AMT
      FROM WORK.ECADURATIONDATES t1
           INNER JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.BRANCH_NBR = t2.BRANCH_ECA)
      WHERE t1.businessdt NOT IS MISSING
      GROUP BY t1.businessdt,
               t2.LOCATION_AA
      ORDER BY t1.businessdt;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECA_TP_ORIGINATIONS2YEARS AS 
   SELECT t1.LOAN_NBR, 
          t1.TITLE_LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.RENEWAL_DATE, 
          t1.TOTAL_DUE, 
          t1.RENEWAL_CHARGE, 
          t1.REPO_FEE_BALANCE
      FROM WORK.QF_TP_LOAN_DATA t1
      WHERE datepart(t1.LOAN_DATE) >= (intnx('month',today(),-36,'beginning')) AND t1.LOAN_STATUS NOT = 'V'
      ORDER BY t1.LOAN_NBR,
               t1.LOAN_DATE;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECA_TP_TRANSACTIONS AS 
   SELECT t1.LOAN_NBR, 
          t2.TITLE_LOAN_NBR, 
          /* BRANCH_NBR */
            (case
              when t3.LOCATION_AA = . then t1.BRANCH_NBR
              else t3.LOCATION_AA
            end) AS BRANCH_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.RENEWAL_DATE, 
          t1.TOTAL_DUE, 
          t1.RENEWAL_CHARGE, 
          t1.REPO_FEE_BALANCE, 
          t2.TRANSACTION_DATE, 
          /* trandt */
            (datepart(t2.TRANSACTION_DATE)) FORMAT=mmddyy10. LABEL="trandt" AS trandt, 
          t2.TRANSACTION_TYPE, 
          t2.VOID_FLAG, 
          t2.TRANSACTION_NBR, 
          t2.REF_TRAN_CODE, 
          t2.TOTAL_AMOUNT_DUE, 
          t2.BAL_PRINCIPAL_AMT, 
          t2.BAL_LIEN_FEE_AMT, 
          t2.BAL_INT_AMOUNT, 
          t2.REPO_CHARGE, 
          t2.STORAGE_COST, 
          t2.LATE_FEE, 
          t2.OTHER_FEE
      FROM WORK.ECA_TP_ORIGINATIONS2YEARS t1
           INNER JOIN WORK.ECATRANSTP t2 ON (t1.LOAN_NBR = t2.LOAN_NBR) AND (t1.TITLE_LOAN_NBR = t2.TITLE_LOAN_NBR)
           LEFT JOIN CADA.ECA_LOCATION_XREF t3 ON (t2.BRANCH_NBR = t3.BRANCH_ECA)
      ORDER BY t1.LOAN_NBR,
               t2.TITLE_LOAN_NBR,
               t2.TRANSACTION_DATE,
               t2.TRANSACTION_NBR;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECA_TP_TRANSACTIONS_TMP1 AS 
   SELECT t1.LOAN_NBR, 
          t1.TITLE_LOAN_NBR, 
          t1.BRANCH_NBR, 
          t1.LOAN_DATE, 
          t1.DUE_DATE, 
          t1.RENEWAL_DATE, 
          t1.TOTAL_DUE, 
          t1.RENEWAL_CHARGE, 
          t1.REPO_FEE_BALANCE, 
          t1.TRANSACTION_DATE, 
          t1.trandt, 
          t1.TRANSACTION_TYPE, 
          t1.VOID_FLAG, 
          t1.TRANSACTION_NBR, 
          t1.REF_TRAN_CODE, 
          t1.TOTAL_AMOUNT_DUE, 
          t1.BAL_PRINCIPAL_AMT, 
          t1.BAL_LIEN_FEE_AMT, 
          t1.BAL_INT_AMOUNT, 
          t1.REPO_CHARGE, 
          t1.STORAGE_COST, 
          t1.LATE_FEE, 
          t1.OTHER_FEE, 
          t2.ST_PVC_CD AS statecd
      FROM WORK.ECA_TP_TRANSACTIONS t1
           INNER JOIN EDW.D_LOCATION t2 ON (t1.BRANCH_NBR = t2.LOC_NBR)
      ORDER BY t1.LOAN_NBR,
               t1.TITLE_LOAN_NBR,
               t1.TRANSACTION_DATE,
               t1.TRANSACTION_NBR;
%RUNQUIT(&job,&sub9);

data work.ecatitle_durationdates;
	set Work.ECA_TP_TRANSACTIONS_TMP1;
	by loan_nbr title_loan_nbr;
	retain repaid_dt default_dt repo_dt wob_dt wo_dt actual_duration_advamt actual_duration_fees;
	if first.loan_nbr then do;
		actual_duration_advamt = bal_principal_amt;
		actual_duration_fees = sum(bal_lien_fee_amt, bal_int_amount, repo_charge, storage_cost, late_fee, other_fee);
		repaid_dt = .;
		default_dt = .;
		repo_dt = .;
		wob_dt = .;
		wo_dt = .;
		repaid_duration_count = .;
		repaid_duration_days = .;
		default_duration_count = .;
		default_duration_days = .;
		repo_duration_count = .;
		repo_duration_days = .;
		wob_duration_count = .;
		wob_duration_days = .;
		wo_duration_count = .;
		wo_duration_days = .;
		duration_event_date = .;
	end;

	if transaction_type in ('BUY', 'CAB') AND repaid_dt = . then repaid_dt = transaction_date;
	else if transaction_type = 'DEF' AND default_dt = . then default_dt = transaction_date;
	else if transaction_type = 'REPO' AND repo_dt = . then repo_dt = transaction_date;
	else if transaction_type = 'WOB' AND wob_dt = . then wob_dt = transaction_date;
	else if transaction_type = 'WO' AND wo_dt = . then wo_dt = transaction_date;

	duration_event_date = min(repaid_dt, default_dt, repo_dt, wob_dt, wo_dt);

	if duration_event_date ~= . and duration_event_date = repaid_dt then do;
		repaid_duration_count = 1;
		repaid_duration_days = datepart(duration_event_date) - datepart(loan_date);
	end;
	else if duration_event_date ~= . and duration_event_date = default_dt then do;
		default_duration_count = 1;
		default_duration_days = datepart(duration_event_date) - datepart(loan_date);
	end;
	else if duration_event_date ~= . and duration_event_date = repo_dt then do;
		repo_duration_count = 1;
		repo_duration_days = datepart(duration_event_date) - datepart(loan_date);
	end;
	else if duration_event_date ~= . and duration_event_date = wob_dt then do;
		wob_duration_count = 1;
		wob_duration_days = datepart(duration_event_date) - datepart(loan_date);
	end;

	duration_event_date = datepart(duration_event_date);

	duration = duration_event_date - datepart(loan_date);

	if last.loan_nbr;

	format repaid_dt default_dt repo_dt wob_dt wo_dt datetime20. duration_event_date mmddyy10.;


%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ecatitle_durationamounts AS 
   SELECT t1.BRANCH_NBR AS locnbr, 
          t1.duration_event_date AS businessdt, 
          /* repaid_duration_count */
            (SUM(t1.repaid_duration_count)) AS repaid_duration_count, 
          /* repaid_duration_days */
            (SUM(t1.repaid_duration_days)) AS repaid_duration_days, 
          /* default_duration_count */
            (SUM(t1.default_duration_count)) AS default_duration_count, 
          /* default_duration_days */
            (SUM(t1.default_duration_days)) AS default_duration_days, 
          /* repo_duration_count */
            (SUM(t1.repo_duration_count)) AS repo_duration_count, 
          /* repo_duration_days */
            (SUM(t1.repo_duration_days)) AS repo_duration_days, 
          /* wob_duration_count */
            (SUM(t1.wob_duration_count)) AS wob_duration_count, 
          /* wob_duration_days */
            (SUM(t1.wob_duration_days)) AS wob_duration_days, 
          /* wo_duration_count */
            (SUM(t1.wo_duration_count)) AS wo_duration_count, 
          /* wo_duration_days */
            (SUM(t1.wo_duration_days)) AS wo_duration_days, 
          /* actual_duration_advamt */
            (SUM(t1.actual_duration_advamt)) AS actual_duration_advamt, 
          /* actual_duration_fees */
            (SUM(t1.actual_duration_fees)) AS actual_duration_fees
      FROM WORK.ECATITLE_DURATIONDATES t1
      GROUP BY t1.BRANCH_NBR,
               t1.duration_event_date;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECATITLEDAILYSUMMARY_TMP AS 
   SELECT DISTINCT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.bankmodel, 
          t1.BRANDCD, 
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
          t1.locnbr, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.businessdt, 
          t1.begindt, 
          t1.advcnt, 
          t1.advamtsum, 
          t1.ADVFEEAMT, 
          t1.agncnt, 
          t1.agnamtsum, 
          t1.AGNFEEAMT, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.heldcnt, 
          t1.DEFAULTCNT, 
          t1.totdefaultrecv, 
          t1.OPS_EARNEDFEES AS EARNEDFEES, 
          t1.woamtsum, 
          t1.wobamtsum, 
          t1.woramtsum, 
          t4.DEFAULT_AMT, 
          t4.DEFAULT_CNT, 
          t5.DEFAULT_PMT, 
          t1.REFINANCE_CNT, 
          t1.substituterow, 
          t2.repaid_duration_count, 
          t2.repaid_duration_days, 
          t2.default_duration_count, 
          t2.default_duration_days, 
          t2.repo_duration_count, 
          t2.repo_duration_days, 
          t2.wob_duration_count, 
          t2.wob_duration_days, 
          t2.wo_duration_count, 
          t2.wo_duration_days, 
          t2.actual_duration_advamt, 
          t2.actual_duration_fees, 
          t3.SUM_of_BB_Value AS BLACKBOOK_VALUE, 
          t1.POSSESSIONAMT AS POSSESSION_AMT, 
          t1.POSSESSIONCNT AS POSSESSION_CNT, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUECNT_1, 
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
          t1.SALETOTAL, 
          t6.GROSS_WRITE_OFF, 
          t6.WOR, 
          t6.GROSS_REVENUE, 
          /* MONTH */
            (MONTH(T1.BUSINESSDT)) AS MONTH, 
          /* YEAR */
            (YEAR(T1.BUSINESSDT)) AS YEAR
      FROM WORK.ECATITLEDAILYSUMMARY_TM1 t1
           LEFT JOIN WORK.ECATITLE_DURATIONAMOUNTS t2 ON (t1.locnbr = t2.locnbr) AND (t1.businessdt = t2.businessdt)
           LEFT JOIN WORK.BB_VALUES t3 ON (t1.locnbr = t3.locnbr) AND (t1.businessdt = t3.businessdt)
           LEFT JOIN WORK.DEFAULT_AMT_CNT_TP t4 ON (t1.locnbr = t4.LOCNBR) AND (t1.businessdt = t4.BUSINESSDT)
           LEFT JOIN WORK.DEFAULT_PMTS_TP t5 ON (t1.locnbr = t5.LOCNBR) AND (t1.businessdt = t5.BUSINESSDT)
           LEFT JOIN WORK.QF4_TLP_PNL t6 ON (t1.locnbr = t6.LOCNBR) AND (t1.businessdt = t6.BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.EPP_COUNTS_PRE AS 
   SELECT /* LOCNBR */
            (INPUT(T1.STORENUMBER,BEST32.)) AS LOCNBR, 
          /* BUSINESSDT */
            (DATEPART(t1.BUSINESSDATE)) FORMAT=MMDDYY10. AS BUSINESSDT, 
          /* REPMTPLANCNT */
            (SUM(t1.OPENPAYMENTPLANS)) AS REPMTPLANCNT, 
          /* OPENPRINRECEIVABLES */
            (SUM(t1.OPENPRINRECEIVABLES)) FORMAT=21.4 AS OPENPRINRECEIVABLES
      FROM ECA.TBL_EPP t1
      WHERE (CALCULATED BUSINESSDT) BETWEEN INTNX('MONTH',TODAY(),-24,'B') AND INTNX('DAY',TODAY(),-1,'B')
      GROUP BY (CALCULATED LOCNBR),
               (CALCULATED BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.EPP_COUNTS AS 
   SELECT t2.LOCATION_AA AS LOCNBR, 
          t1.BUSINESSDT, 
          t1.REPMTPLANCNT, 
          t1.OPENPRINRECEIVABLES
      FROM WORK.EPP_COUNTS_PRE t1
           INNER JOIN CADA.ECA_LOCATION_XREF t2 ON (t1.LOCNBR = t2.BRANCH_ECA);
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.ECAPAYDAYDAILYSUMMARY_TMP AS 
   SELECT DISTINCT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.bankmodel, 
          t1.BRANDCD, 
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
          t1.locnbr, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.businessdt, 
          t1.begindt, 
          t1.advcnt, 
          t1.agncnt, 
          t1.advamtsum, 
          t3.ADVFEEAMT, 
          t1.totadvrecv, 
          t1.totadvfeerecv, 
          t1.heldcnt, 
          t7.REPMTPLANCNT, 
          t7.OPENPRINRECEIVABLES AS RPPPRINRECEIVABLES, 
          t1.OPS_EARNEDFEES AS EARNEDFEES, 
          t1.defaultcnt, 
          t1.REFINANCE_CNT, 
          t5.DEFAULT_AMT, 
          t5.DEFAULT_CNT, 
          t6.DEFAULT_PMT, 
          t1.PASTDUECNT_1, 
          t1.totdefaultrecv, 
          t1.totdefaultfeerecv, 
          t4.NSFAMTSUM, 
          t4.NPAMTSUM, 
          t4.NPPAMTSUM, 
          t1.woamtsum, 
          t1.wobamtsum, 
          t1.woramtsum, 
          t1.TOTALBUYBACKS, 
          t1.TOTALCASHAGAINS, 
          t1.TOTALDEPOSITCHECKS, 
          t2.repaid_duration_count, 
          t2.repaid_duration_days, 
          t2.deposit_duration_count, 
          t2.deposit_duration_days, 
          t2.wob_duration_count, 
          t2.wob_duration_days, 
          t2.wo_duration_count, 
          t2.wo_duration_days, 
          t2.actual_duration_advamt, 
          t2.actual_duration_fees, 
          t8.GROSS_WRITE_OFF, 
          t8.WOR, 
          t8.GROSS_REVENUE, 
          t1.substituterow, 
          /* MONTH */
            (MONTH(T1.BUSINESSDT)) AS MONTH, 
          /* YEAR */
            (YEAR(T1.BUSINESSDT)) AS YEAR
      FROM WORK.ECAPAYDAYDAILYSUMMARY_TM1 t1
           LEFT JOIN WORK.ECA_AGGREGATE_DURATION t2 ON (t1.businessdt = t2.businessdt) AND (t1.locnbr = t2.locnbr)
           LEFT JOIN WORK.ADVFEEAMT t3 ON (t1.businessdt = t3.businessdt) AND (t1.locnbr = t3.locnbr)
           LEFT JOIN WORK.NPNPPSUMMARY t4 ON (t1.businessdt = t4.businessdt) AND (t1.locnbr = t4.locnbr)
           LEFT JOIN WORK.DEFAULT_AMT_CNT t5 ON (t1.locnbr = t5.LOCNBR) AND (t1.businessdt = t5.BUSINESSDT)
           LEFT JOIN WORK.DEFAULT_PMTS t6 ON (t1.locnbr = t6.LOCNBR) AND (t1.businessdt = t6.BUSINESSDT)
           LEFT JOIN WORK.EPP_COUNTS t7 ON (t1.locnbr = t7.LOCNBR) AND (t1.businessdt = t7.BUSINESSDT)
           LEFT JOIN WORK.QF4_PDL_PNL t8 ON (t1.locnbr = t8.LOCNBR) AND (t1.businessdt = t8.BUSINESSDT);
%RUNQUIT(&job,&sub9);

PROC SQL;
CREATE TABLE WORK.qfund4_dailysummary_pre AS 
SELECT * FROM WORK.ECATITLEDAILYSUMMARY_TMP
 OUTER UNION CORR 
SELECT * FROM WORK.ECAPAYDAYDAILYSUMMARY_TMP
;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.QF4_ADD_PWO AS 
   SELECT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.bankmodel, 
          t1.BRANDCD, 
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
          t1.businessdt, 
          t1.begindt, 
          t1.ADVCNT, 
          t1.ADVAMTSUM, 
          t1.ADVFEEAMT, 
          t1.AGNCNT, 
          t1.AGNAMTSUM, 
          t1.AGNFEEAMT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          t1.HELDCNT, 
          t1.DEFAULTCNT, 
          t1.TOTDEFAULTRECV, 
          t1.EARNEDFEES, 
          t1.WOAMTSUM, 
          t1.WOBAMTSUM, 
          t1.WORAMTSUM, 
          t1.DEFAULT_AMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_PMT, 
          t1.REFINANCE_CNT, 
          t1.substituterow, 
          t1.repaid_duration_count, 
          t1.repaid_duration_days, 
          t1.default_duration_count, 
          t1.default_duration_days, 
          t1.repo_duration_count, 
          t1.repo_duration_days, 
          t1.wob_duration_count, 
          t1.wob_duration_days, 
          t1.wo_duration_count, 
          t1.wo_duration_days, 
          t1.actual_duration_advamt, 
          t1.actual_duration_fees, 
          t1.BLACKBOOK_VALUE, 
          t1.POSSESSION_AMT, 
          t1.POSSESSION_CNT, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUECNT_1, 
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
          t2.CURRENT_PWO_AMT, 
          t2.NEXT_MONTH_PWO_AMT, 
          t2.NEXT_2_MONTH_PWO_AMT, 
          t1.SALETOTAL, 
          t1.GROSS_WRITE_OFF, 
          t1.WOR, 
          t1.GROSS_REVENUE, 
          t1.MONTH, 
          t1.YEAR, 
          t1.REPMTPLANCNT, 
          t1.RPPPRINRECEIVABLES, 
          t1.totdefaultfeerecv, 
          t1.NSFAMTSUM, 
          t1.NPAMTSUM, 
          t1.NPPAMTSUM, 
          t1.TOTALBUYBACKS, 
          t1.TOTALCASHAGAINS, 
          t1.TOTALDEPOSITCHECKS, 
          t1.deposit_duration_count, 
          t1.deposit_duration_days
      FROM WORK.QFUND4_DAILYSUMMARY_PRE t1
           LEFT JOIN WORK.PWO_QFUND4 t2 ON (t1.LOCNBR = t2.LOCNBR) AND (t1.businessdt = t2.BUSINESSDT) AND (t1.Product 
          = t2.PRODUCT);
%RUNQUIT(&job,&sub9);

PROC SQL;
	CREATE TABLE QFUND4_DAILYSUMMARY AS
		SELECT T1.*
			  ,CASE WHEN T2.BEGIN_PWO_AMT = . THEN 0 ELSE T2.BEGIN_PWO_AMT END AS BEGIN_PWO_AMT
		FROM WORK.QF4_ADD_PWO T1
		LEFT JOIN
		WORK.BEGIN_PWO_AMT T2
		ON(T1.LOCNBR=T2.LOCNBR AND
		   T1.PRODUCT=T2.PRODUCT AND
		   T1.YEAR=T2.YEAR AND
		   T1.MONTH=T2.MONTH)
;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE QFUND4_NEW_ORIGINATIONS AS 
   SELECT t1.Product, 
          t1.pos, 
          t1.INSTANCE, 
          t1.BRANDCD, 
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
          t1.locnbr, 
          t1.Location_Name, 
          t1.LOC_OPEN_DT, 
          t1.LOC_CLOSE_DT, 
          t1.businessdt, 
          /* NEW_ORIGINATIONS */
            (sum(t1.advcnt,t1.agncnt)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (sum(t1.advamtsum,t1.agnamtsum)) AS NEW_ADV_AMT, 
          t1.ADVFEEAMT AS NEW_ADVFEE_AMT, 
          /* totadvrecv */
            (sum(t1.totadvrecv,t1.RPPPRINRECEIVABLES)) AS totadvrecv, 
          t1.REPMTPLANCNT, 
          t1.totadvfeerecv, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (sum(t1.heldcnt,t1.REPMTPLANCNT)) AS COMPLIANT_LOANS_OUTSTANDING, 
          t1.DEFAULTCNT AS DEFAULT_LOANS_OUTSTANDING, 
          t1.totdefaultfeerecv, 
          t1.totdefaultrecv, 
          t1.NSFAMTSUM AS NSF_AMOUNT, 
          t1.NPAMTSUM AS NSF_PAYMENT_AMOUNT, 
          t1.NPPAMTSUM AS NSF_PREPAYMENT_AMOUNT, 
          t1.woamtsum, 
          t1.wobamtsum, 
          /* WORAMTSUM */
            (CASE WHEN t1.businessdt < '01APR2017'D THEN t1.woramtsum ELSE 0 END) AS WORAMTSUM, 
          t1.DEFAULT_AMT, 
          t1.DEFAULT_CNT, 
          t1.DEFAULT_PMT, 
          t1.TOTALBUYBACKS AS BUYBACK_COUNT, 
          t1.TOTALCASHAGAINS AS CASHAGAIN_COUNT, 
          t1.TOTALDEPOSITCHECKS AS DEPOSIT_COUNT, 
          t1.POSSESSION_AMT, 
          t1.POSSESSION_CNT, 
          t1.PASTDUEAMT_1, 
          t1.PASTDUECNT_1, 
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
		  t1.BEGIN_PWO_AMT,
		  t1.CURRENT_PWO_AMT,
		  t1.NEXT_MONTH_PWO_AMT,
		  t1.NEXT_2_MONTH_PWO_AMT,
          t1.REFINANCE_CNT, 
          /* GROSS_REVENUE */
            (CASE WHEN t1.businessdt < '01APR2017'd THEN (t1.EARNEDFEES) ELSE 0 END) AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (CASE WHEN t1.businessdt < '01APR2017'D THEN (SUM(t1.woamtsum,t1.wobamtsum)) ELSE 0 END) AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (CASE WHEN t1.businessdt < '01APR2017'D then ((SUM(t1.woamtsum,t1.wobamtsum)) - t1.woramtsum) else 0 END) 
            AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (CASE WHEN t1.businessdt < '01APR2017'D THEN ((t1.EARNEDFEES) - ((SUM(t1.woamtsum,t1.wobamtsum)) - 
            t1.woramtsum)) ELSE 0 END) AS NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            
            (sum(t1.repaid_duration_count,t1.default_duration_count,t1.repo_duration_count,t1.wob_duration_count,t1.wo_duration_count,t1.deposit_duration_count)) 
            AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            
            (sum(t1.repaid_duration_days,t1.default_duration_days,t1.repo_duration_days,t1.wob_duration_days,t1.wo_duration_days,t1.deposit_duration_days)) 
            AS ACTUAL_DURATION_DAYS, 
          t1.BLACKBOOK_VALUE AS BLACK_BOOK_VALUE, 
          t1.SALETOTAL AS SOLD_AMOUNT, 
          t1.advamtsum, 
          t1.agnamtsum, 
          t1.advcnt, 
          t1.agncnt, 
          t1.actual_duration_fees, 
          t1.actual_duration_advamt, 
          t1.heldcnt, 
          /* PRODUCT_DESC */
            (case
              when product = "TITLE" then "ECA TITLE"
              when product = "PAYDAY" then "ECA PAYDAY"
            end) AS PRODUCT_DESC
      FROM QFUND4_DAILYSUMMARY t1;
%RUNQUIT(&job,&sub9);

LIBNAME EDW ORACLE
	USER=&USER
	PW=&PASSWORD
	PATH=EDWPRD
	SCHEMA=EDW;

DATA PNL_INTIAL_PULL;
	SET EDW.QF_BADDEBT_PNLAMT;
	BUSINESSDT = DATEPART(BUSINESS_DATE);
	WHERE BUSINESS_DATE >= '01APR2017:00:00:00'DT;
	FORMAT BUSINESSDT MMDDYY10.;
%RUNQUIT(&job,&sub9);

PROC SQL;
	CREATE TABLE WORK.QFUND_PNL AS 
		SELECT 
			STORE_NUMBER AS LOCNBR
		   ,CASE WHEN SOURCE_SYSTEM = 'QFUND5' THEN 'QFUND5-6' ELSE SOURCE_SYSTEM END AS INSTANCE
		   ,PRODUCT_TYPE
		   ,BUSINESSDT
		   ,BAD_DEBT AS GROSS_WRITE_OFF
		   ,-BADDEBT_PMT AS WORAMTSUM
		   ,-PNL_AMT AS GROSS_REVENUE
		   ,SUM(BAD_DEBT,BADDEBT_PMT) AS NET_WRITE_OFF
		   ,SUM(-PNL_AMT,(-SUM(BAD_DEBT,BADDEBT_PMT))) AS NET_REVENUE
		FROM WORK.PNL_INTIAL_PULL
	WHERE COMPRESS(PRODUCT_TYPE) ^= 'MISC'
;
%RUNQUIT(&job,&sub9);

/*-------------*/
/* QFUND 4 TLP / PDL */
/*-------------*/
PROC SQL;
	CREATE TABLE QFUND4_TLP_PDL AS
		SELECT
		    CASE WHEN COMPRESS(PRODUCT_TYPE) IN('TLP','VATLP') THEN 'TITLE'
				 WHEN COMPRESS(PRODUCT_TYPE) = 'PDL' THEN 'PAYDAY'
			     ELSE PRODUCT_TYPE 
            END AS PRODUCT
		   ,(CASE WHEN COMPRESS(PRODUCT_TYPE) IN('TLP','VATLP') THEN 'ECA TITLE'
				  WHEN COMPRESS(PRODUCT_TYPE) = 'PDL' THEN 'ECA PAYDAY'
				  ELSE '' END) AS PRODUCT_DESC
		   ,'QFUND' AS POS
		   ,INSTANCE
		   ,'STANDARD' AS BANKMODEL
		   ,LOC.BRND_CD AS BRANDCD
		   ,LOC.CTRY_CD AS COUNTRYCD
		   ,LOC.ST_PVC_CD AS STATE
		   ,LOC.ADR_CITY_NM AS CITY
		   ,LOC.MAIL_CD AS ZIP
		   ,LOC.BUSN_UNIT_ID AS BUSINESS_UNIT
		   ,LOC.HIER_ZONE_NBR AS ZONENBR
		   ,LOC.HIER_ZONE_NM AS ZONENAME
		   ,LOC.HIER_RGN_NBR AS REGIONNBR
		   ,LOC.HIER_RDO_NM AS REGIONRDO
		   ,LOC.HIER_DIV_NBR AS DIVISIONNBR
		   ,LOC.HIER_DDO_NM AS DIVISIONDDO
		   ,LOCNBR
		   ,LOC.LOC_NM AS LOCATION_NAME
		   ,LOC.OPEN_DT AS LOC_OPEN_DT
		   ,LOC.CLS_DT AS LOC_CLOSE_DT
		   ,BUSINESSDT
		   ,SUM(GROSS_WRITE_OFF) AS GROSS_WRITE_OFF
		   ,SUM(NET_WRITE_OFF) AS NET_WRITE_OFF
		   ,SUM(WORAMTSUM) AS WORAMTSUM
		   ,SUM(NET_REVENUE) AS NET_REVENUE
		   ,SUM(GROSS_REVENUE) AS GROSS_REVENUE
		FROM WORK.QFUND_PNL PNL
		LEFT JOIN 
		EDW.D_LOCATION LOC
		ON(PNL.LOCNBR=LOC.LOC_NBR)
	WHERE COMPRESS(PRODUCT_TYPE) IN('TLP','VATLP','PDL') AND INSTANCE = 'QFUND4' AND ZONENBR IS NOT MISSING
	GROUP BY CALCULATED PRODUCT, CALCULATED PRODUCT_DESC, CALCULATED POS, INSTANCE, CALCULATED BANKMODEL, LOC.BRND_CD, LOC.CTRY_CD, LOC.ST_PVC_CD, LOC.ADR_CITY_NM, LOC.MAIL_CD, 
			 LOC.BUSN_UNIT_ID,  LOC.HIER_ZONE_NBR, LOC.HIER_ZONE_NM, LOC.HIER_RGN_NBR, LOC.HIER_RDO_NM, LOC.HIER_DIV_NBR, LOC.HIER_DDO_NM, LOCNBR,
			 LOC.LOC_NM, LOC.OPEN_DT, LOC.CLS_DT, BUSINESSDT
;
%RUNQUIT(&job,&sub9);

PROC SQL;
CREATE TABLE WORK.RU1_LENDINGPRODUCTS_ROLLUP_PRE AS 
	SELECT * FROM QFUND4_NEW_ORIGINATIONS
		OUTER UNION CORR 
	SELECT * FROM QFUND4_TLP_PDL
;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE RU1_LENDINGPRODUCTS_ROLLUP AS 
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
          t1.BusinessDt, 
          /* NEW_ORIGINATIONS */
            (SUM(t1.NEW_ORIGINATIONS)) AS NEW_ORIGINATIONS, 
          /* NEW_ADV_AMT */
            (SUM(t1.NEW_ADV_AMT)) AS NEW_ADV_AMT, 
          /* NEW_ADVFEE_AMT */
            (SUM(t1.NEW_ADVFEE_AMT)) AS NEW_ADVFEE_AMT, 
          /* TOTADVRECV */
            (SUM(t1.TOTADVRECV)) FORMAT=12.2 AS TOTADVRECV, 
          /* TOTADVFEERECV */
            (SUM(t1.TOTADVFEERECV)) FORMAT=10.2 AS TOTADVFEERECV, 
          /* COMPLIANT_LOANS_OUTSTANDING */
            (SUM(t1.COMPLIANT_LOANS_OUTSTANDING)) AS COMPLIANT_LOANS_OUTSTANDING, 
          /* DEFAULT_LOANS_OUTSTANDING */
            (SUM(t1.DEFAULT_LOANS_OUTSTANDING)) AS DEFAULT_LOANS_OUTSTANDING, 
          /* TOTDEFAULTRECV */
            (SUM(t1.TOTDEFAULTRECV)) FORMAT=12.2 AS TOTDEFAULTRECV, 
          /* TOTDEFAULTFEERECV */
            (SUM(t1.TOTDEFAULTFEERECV)) FORMAT=10.2 AS TOTDEFAULTFEERECV, 
          /* NSF_AMOUNT */
            (SUM(t1.NSF_AMOUNT)) FORMAT=10.2 AS NSF_AMOUNT, 
          /* NSF_PAYMENT_AMOUNT */
            (SUM(t1.NSF_PAYMENT_AMOUNT)) FORMAT=10.2 AS NSF_PAYMENT_AMOUNT, 
          /* NSF_PREPAYMENT_AMOUNT */
            (SUM(t1.NSF_PREPAYMENT_AMOUNT)) FORMAT=10.2 AS NSF_PREPAYMENT_AMOUNT, 
          /* WOCNT */
            (SUM(0)) AS WOCNT, 
          /* WOAMTSUM */
            (SUM(t1.WOAMTSUM)) FORMAT=14.2 AS WOAMTSUM, 
          /* WOBAMTSUM */
            (SUM(t1.WOBAMTSUM)) FORMAT=10.2 AS WOBAMTSUM, 
          /* WOBCNT */
            (SUM(0)) AS WOBCNT, 
          /* WORCNT */
            (SUM(0)) AS WORCNT, 
          /* WORAMTSUM */
            (SUM(t1.WORAMTSUM)) FORMAT=10.2 AS WORAMTSUM, 
          /* CASHAGAIN_COUNT */
            (SUM(t1.CASHAGAIN_COUNT)) AS CASHAGAIN_COUNT, 
          /* BUYBACK_COUNT */
            (SUM(t1.BUYBACK_COUNT)) AS BUYBACK_COUNT, 
          /* DEPOSIT_COUNT */
            (SUM(t1.DEPOSIT_COUNT)) AS DEPOSIT_COUNT, 
          /* BEGIN_PWO_AMT */
            (SUM(t1.BEGIN_PWO_AMT)) AS BEGIN_PWO_AMT, 
          /* CURRENT_PWO_AMT */
            (SUM(t1.CURRENT_PWO_AMT)) AS CURRENT_PWO_AMT, 
          /* NEXT_MONTH_PWO_AMT */
            (SUM(t1.NEXT_MONTH_PWO_AMT)) AS NEXT_MONTH_PWO_AMT, 
          /* NEXT_2_MONTH_PWO_AMT */
            (SUM(t1.NEXT_2_MONTH_PWO_AMT)) AS NEXT_2_MONTH_PWO_AMT, 
          /* DEFAULT_PMT */
            (SUM(t1.DEFAULT_PMT)) FORMAT=10.2 AS DEFAULT_PMT, 
          /* DEFAULT_CNT */
            (SUM(t1.DEFAULT_CNT)) AS DEFAULT_CNT, 
          /* DEFAULT_AMT */
            (SUM(t1.DEFAULT_AMT)) FORMAT=10.2 AS DEFAULT_AMT, 
          /* GROSS_REVENUE */
            (SUM(t1.GROSS_REVENUE)) FORMAT=10.2 AS GROSS_REVENUE, 
          /* GROSS_WRITE_OFF */
            (SUM(t1.GROSS_WRITE_OFF)) FORMAT=10.2 AS GROSS_WRITE_OFF, 
          /* NET_WRITE_OFF */
            (SUM(t1.NET_WRITE_OFF)) FORMAT=10.2 AS NET_WRITE_OFF, 
          /* NET_REVENUE */
            (SUM(t1.NET_REVENUE)) FORMAT=10.2 AS NET_REVENUE, 
          /* ACTUAL_DURATION_COUNT */
            (SUM(t1.ACTUAL_DURATION_COUNT)) AS ACTUAL_DURATION_COUNT, 
          /* ACTUAL_DURATION_DAYS */
            (SUM(t1.ACTUAL_DURATION_DAYS)) AS ACTUAL_DURATION_DAYS, 
          /* ACTUAL_DURATION_ADVAMT */
            (SUM(t1.ACTUAL_DURATION_ADVAMT)) AS ACTUAL_DURATION_ADVAMT, 
          /* ACTUAL_DURATION_FEES */
            (SUM(t1.ACTUAL_DURATION_FEES)) AS ACTUAL_DURATION_FEES, 
          /* AVGDURATIONDAYS */
            (SUM(0)) AS AVGDURATIONDAYS, 
          /* AVGDURATIONCNT */
            (SUM(0)) AS AVGDURATIONCNT, 
          /* HELDCNT */
            (SUM(t1.HELDCNT)) AS HELDCNT, 
          /* PASTDUECNT_1 */
            (SUM(t1.PASTDUECNT_1)) AS PASTDUECNT_1, 
          /* PASTDUEAMT_1 */
            (SUM(t1.PASTDUEAMT_1)) FORMAT=12.2 AS PASTDUEAMT_1, 
          /* OVERSHORTAMT */
            (SUM(0)) AS OVERSHORTAMT, 
          /* HOLDOVERAMT */
            (SUM(0)) AS HOLDOVERAMT, 
          /* ADVAMTSUM */
            (SUM(t1.ADVAMTSUM)) FORMAT=14.2 AS ADVAMTSUM, 
          /* AGNADVSUM */
            (SUM(0)) FORMAT=14.2 AS AGNADVSUM, 
          /* REPMTPLANCNT */
            (SUM(t1.REPMTPLANCNT)) AS REPMTPLANCNT, 
          /* ADVCNT */
            (SUM(t1.ADVCNT)) AS ADVCNT, 
          /* AVGADVAMT */
            (SUM(0)) FORMAT=10.2 AS AVGADVAMT, 
          /* AVGDURATION */
            (SUM(0)) FORMAT=10.2 AS AVGDURATION, 
          /* AVGFEEAMT */
            (SUM(0)) FORMAT=10.2 AS AVGFEEAMT, 
          /* PASTDUEAMT_2 */
            (SUM(t1.PASTDUEAMT_2)) FORMAT=12.2 AS PASTDUEAMT_2, 
          /* PASTDUECNT_2 */
            (SUM(t1.PASTDUECNT_2)) FORMAT=11. AS PASTDUECNT_2, 
          /* REFINANCE_CNT */
            (SUM(t1.REFINANCE_CNT)) AS REFINANCE_CNT, 
          /* AGNCNT */
            (SUM(t1.AGNCNT)) AS AGNCNT, 
          /* POSSESSION_AMT */
            (SUM(t1.POSSESSION_AMT)) FORMAT=21.4 AS POSSESSION_AMT, 
          /* POSSESSION_CNT */
            (SUM(t1.POSSESSION_CNT)) AS POSSESSION_CNT, 
          /* PASTDUEAMT_3 */
            (SUM(t1.PASTDUEAMT_3)) FORMAT=21.4 AS PASTDUEAMT_3, 
          /* PASTDUECNT_3 */
            (SUM(t1.PASTDUECNT_3)) AS PASTDUECNT_3, 
          /* PASTDUEAMT_4 */
            (SUM(t1.PASTDUEAMT_4)) FORMAT=21.4 AS PASTDUEAMT_4, 
          /* PASTDUECNT_4 */
            (SUM(t1.PASTDUECNT_4)) AS PASTDUECNT_4, 
          /* PASTDUEAMT_5 */
            (SUM(t1.PASTDUEAMT_5)) FORMAT=21.4 AS PASTDUEAMT_5, 
          /* PASTDUECNT_5 */
            (SUM(t1.PASTDUECNT_5)) AS PASTDUECNT_5, 
          /* PASTDUEAMT_6 */
            (SUM(t1.PASTDUEAMT_6)) FORMAT=21.4 AS PASTDUEAMT_6, 
          /* PASTDUECNT_6 */
            (SUM(t1.PASTDUECNT_6)) AS PASTDUECNT_6, 
          /* BLACK_BOOK_VALUE */
            (SUM(t1.BLACK_BOOK_VALUE)) AS BLACK_BOOK_VALUE, 
          /* SOLD_AMOUNT */
            (SUM(t1.SOLD_AMOUNT)) FORMAT=21.4 AS SOLD_AMOUNT, 
          /* agnamtsum */
            (SUM(t1.agnamtsum)) AS agnamtsum, 
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
      FROM WORK.RU1_LENDINGPRODUCTS_ROLLUP_PRE t1
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
%RUNQUIT(&job,&sub9);


%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\STDM\STDM_LIBRARY_SCRIPT.SAS";
LIBNAME SKYNET "E:\SHARED\CADA\SAS DATA\DATAMART\STDM";
LIBNAME NORECV "E:\SHARED\CADA\SAS DATA\DATAMART\SCOCHRAN";

%LET ENDINGDT = INTNX('DAY',TODAY(),-1,'BEGINNING');

DATA _NULL_;
	CALL SYMPUTX('END_DT',PUT(&ENDINGDT,YYMMDDN8.),G);
%RUNQUIT(&job,&sub9);

DATA WORK.A&END_DT;
	X = &ENDINGDT;
	FORMAT X MMDDYY10.;
%RUNQUIT(&job,&sub9);

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
%RUNQUIT(&job,&sub9);

proc sql;
	create table thursdaydates_tmp2 as
		select t1.*, t2.holidayname
		  from thursdaydates_tmp1 t1
			LEFT JOIN bior.i_holidays t2 on (t1.businessdt = datepart(t2.holiday_dt))
		 order by t1.businessdt desc;
%RUNQUIT(&job,&sub9);

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
%RUNQUIT(&job,&sub9);

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
%RUNQUIT(&job,&sub9);

proc sql;
	create table work.daily_summary_all_tmp3 as
		select t1.*, t2.thursdayweek
          from work.daily_summary_all_tmp2 t1, work.thursdaydates_tmp3 t2
		 where t1.businessdt = t2.businessdt;
%RUNQUIT(&job,&sub9);

proc sort data=daily_summary_all_tmp3;
	by locnbr businessdt;
%RUNQUIT(&job,&sub9);

data last_report_date;
	set daily_summary_all_tmp3;
	by locnbr businessdt;
	loc_last_reported_dt = businessdt;
	if last.locnbr then output;
	keep locnbr loc_last_reported_dt;
	format loc_last_reported_dt mmddyy10.;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.holidays(label="HOLIDAYS") AS 
   SELECT /* holidaydt */
            (datepart(t1.HOLIDAY_DT)) FORMAT=mmddyy10. LABEL="holidaydt" AS holidaydt, 
          t1.HOLIDAYNAME
      FROM BIOR.I_HOLIDAYS t1;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE WORK.DAILY_SUMMARY_ALL_TMP4 AS 
   SELECT t1.Product, 
          t1.PRODUCT_DESC, 
          t1.pos, 
          t1.INSTANCE, 
          t1.brandcd, 
          t1.bankmodel, 
          t1.COUNTRYCD, 
          t1.STATE, 
          t1.ZIP, 
          t1.CITY, 
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
          t1.lastthursday, 
          t2.HOLIDAYNAME, 
          t1.ThursdayWeek, 
          t1.LAST_REPORT_DT, 
          t1.NEW_ORIGINATIONS, 
          t1.NEW_ADV_AMT, 
          t1.NEW_ADVFEE_AMT, 
          t1.TOTADVRECV, 
          t1.TOTADVFEERECV, 
          t1.COMPLIANT_LOANS_OUTSTANDING, 
          t1.DEFAULT_LOANS_OUTSTANDING, 
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
          t1.ACTUAL_DURATION_COUNT, 
          t1.ACTUAL_DURATION_DAYS, 
          t1.ACTUAL_DURATION_ADVAMT, 
          t1.ACTUAL_DURATION_FEES, 
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
          t1.REPMTPLANCNT AS REPMTPLANCNT1, 
          t1.ADVCNT, 
          t1.AVGADVAMT, 
          t1.AVGDURATION, 
          t1.AVGFEEAMT, 
          t1.ADVAMTSUM, 
          t1.AVGDURATIONDAYS, 
          t1.AVGDURATIONCNT, 
          t1.HELDCNT, 
          t1.REPMTPLANCNT, 
          t1.AGNCNT
      FROM WORK.DAILY_SUMMARY_ALL_TMP3 t1
           LEFT JOIN WORK.HOLIDAYS t2 ON (t1.BusinessDt = t2.holidaydt);
%RUNQUIT(&job,&sub9);

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
%RUNQUIT(&job,&sub9);

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
%RUNQUIT(&job,&sub9);


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
			SET QFUND4_PAYDAY_STATUS = 'WAITING_CL'
			   ,QFUND4_TITLE_STATUS = 'WAITING_CL'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub9);

%MACRO WAITFORCUSTLIFE();


	%DO %UNTIL (%EVAL(&COUNT_R. >= 1));	
		PROC SQL;
			CREATE TABLE CUST_LIFE_CHECK_TODAY AS
			SELECT INSTANCE
				  ,MAX(BUSINESS_DATE)	AS BUSINESSDT
			FROM BIOR.CUST_CATEGORY_DAILY_COUNT
			WHERE INSTANCE = 'QFUND4'
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
						WHERE INSTANCE = 'QFUND4' AND BUSINESSDT >= DHMS(TODAY()-1,00,00,00)
					;
					QUIT;

				%END;
		%ELSE %IF &DAYOFWEEK. = MONDAY %THEN
				%DO;

					/*EADV*/
					PROC SQL;
						SELECT COUNT(*) INTO: COUNT_R
						FROM CUST_LIFE_CHECK_TODAY
						WHERE INSTANCE = 'QFUND4' AND BUSINESSDT >= DHMS(TODAY()-2,00,00,00)
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
			SET QFUND4_PAYDAY_STATUS = 'RUNNING'
			   ,QFUND4_TITLE_STATUS = 'RUNNING'
			WHERE SOURCE = 'BIOR.O_DAILY_SUMMARY_ALL'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub9);

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
	  WHERE T1.INSTANCE = 'QFUND4' AND T1.BUSINESS_DATE >= DHMS(TODAY()-5,00,00,00)
      GROUP BY (CALCULATED BUSINESS_DATE),
               t1.LOCATION_NBR,
               t1.INSTANCE,
               t1.PRODUCT,
               (CALCULATED PRODUCT_DESC)
;
%RUNQUIT(&job,&sub9);

PROC SQL;
   CREATE TABLE DAILY_SUMMARY_ALL_QF4_1 AS 
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
		  "STOREFRONT"				AS CHANNELCD
      FROM DAILY_SUMMARY_ALL_PRE t1
           LEFT JOIN WORK.PROD_DESC_CHANGE t2 ON (t1.INSTANCE = t2.INSTANCE) AND (t1.PRODUCT_DESC = t2.PRODUCT_DESC) 
          AND (T1.PRODUCT = t2.PRODUCT) AND (T1.BUSINESSDT = T2.BUSINESS_DATE) AND (t1.LOCNBR = t2.LOCATION_NBR);
%RUNQUIT(&job,&sub9);

PROC FORMAT;
    PICTURE CHECKTHEDAY OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
    PICTURE CHECKTHETIME OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub9);

%LET DATE=%SYSFUNC(INTNX(DAY,%SYSFUNC(TODAY()),0,END),DATE7.);
%PUT &DATE;


/* CREATE MACROS FOR BULKLOAD PATH AND TIMESTAMP */
DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
    CALL SYMPUTX('PATH',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND4",'G');
    CALL SYMPUTX('PATHTWO',"E:\SHARED\CADA\SAS DATA\DATAMART\SKYNET REDESIGN BULKLOAD LOGS\DAILY\QFUND4\Dir2",'G');
%RUNQUIT(&job,&sub9);

PROC SQL;
    INSERT INTO SKY.DAILYSUM_DATAMART_QF4 (BULKLOAD=YES BL_LOG="&PATH.\BL_&DATE..LOG" BL_DELETE_DATAFILE=YES 
                                                   BL_DEFAULT_DIR="&PATHTWO.")
    SELECT 
        *
    FROM DAILY_SUMMARY_ALL_QF4_1
	WHERE BUSINESSDT >= DHMS(TODAY()-5,00,00,00)
	AND STATE NOT IN ('TN');
%RUNQUIT(&job,&sub9);

/* CREATE TIMESTAMP */
PROC FORMAT;
	PICTURE WHATDAYISIT OTHER=%0Y.%0M.%0D (DATATYPE=DATE);
	PICTURE WHATTIMEISIT OTHER=%0H.%0M.%0S (DATATYPE=TIME);
%RUNQUIT(&job,&sub9);

DATA _NULL_;
	CALL SYMPUTX('TIMESTAMP',TRANWRD(PUT(DATETIME(),DATETIME20.),':','.'),'G');
RUN;

%PUT &TIMESTAMP;


/*KICK OFF QF4_DAILY UPLOAD*/
SYSTASK COMMAND "'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SAS.EXE'
				 '&DAILY_FILE_PATH.\TRANSPOSE QFUND4.SAS'
				 -LOG '&DAILY_LOGPATH.\TRANSPOSE_QFUND4_&TIMESTAMP..LOG'
				 -CONFIG 'C:\PROGRAM FILES\SASHOME\SASFOUNDATION\9.4\SASV9.CFG'"
TASKNAME=TRANSPOSE_QF4
STATUS=TRANSPOSE_QF4;


/*UPLOAD ECA*/
%INCLUDE "&DAILY_FILE_PATH.\DAILYSUM_UPLOAD_QFUND4.SAS";



PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.O_DAILY_SUMMARY_ALL
		    SET LAST_REPORT_DT = TO_DATE(TO_CHAR(CURRENT_DATE-1, 'MM/DD/YYYY'), 'MM/DD/YYYY')
			WHERE INSTANCE = 'QFUND4'
			)
	BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;

WAITFOR _ALL_ TRANSPOSE_QF4;


/*ABORT PROGRAM*/
%MACRO STOPPROGRAM();

	%IF %EVAL(1=1) %THEN %DO;
		%ABORT CANCEL;
	%END;

%MEND;

%STOPPROGRAM
