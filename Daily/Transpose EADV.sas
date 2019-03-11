%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN\DATAMART REDESIGN\DAILY\DAILY_ERROR_INPUTS.SAS";

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET EADV_STATUS = 'UPLOADING'
			   ,EADV_RUN_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.TRANSPOSE_DAILY_SUMMARY'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub30);

/*INSERTS INTO STAGING TABLE*/
PROC SQL;
	CONNECT TO ORACLE(USER=&USER. PASSWORD=&PASSWORD. PATH='BIOR');
	EXEC(INSERT INTO SKYNET.TRANSPOSE_DAILY_SUMMARY_EADV
		            SELECT
               PRODUCT ,PRODUCT_DESC ,POS ,INSTANCE ,BRANDCD ,BANKMODEL 
                ,COUNTRYCD ,STATE ,CITY ,ZIP ,ZONENBR ,ZONENAME ,REGIONNBR 
                ,REGIONRDO ,DIVISIONNBR ,DIVISIONDDO ,LOCNBR ,LOCATION_NAME 
                ,LOC_OPEN_DT ,LOC_CLOSE_DT ,BUSINESSDT ,LAST_REPORT_DT 
                ,LOC_LAST_REPORTED_DT ,LATITUDE ,LONGITUDE ,HOLIDAYNAME ,LASTTHURSDAY 
                ,THURSDAYWEEK
              ,KPINAME
              ,VALUE1
            FROM
            (
            SELECT 
               PRODUCT ,PRODUCT_DESC ,POS ,INSTANCE ,BRANDCD ,BANKMODEL 
                ,COUNTRYCD ,STATE ,CITY ,ZIP ,ZONENBR ,ZONENAME ,REGIONNBR 
                ,REGIONRDO ,DIVISIONNBR ,DIVISIONDDO ,LOCNBR ,LOCATION_NAME 
                ,LOC_OPEN_DT ,LOC_CLOSE_DT ,BUSINESSDT ,LAST_REPORT_DT 
                ,LOC_LAST_REPORTED_DT ,LATITUDE ,LONGITUDE ,HOLIDAYNAME ,LASTTHURSDAY 
                ,THURSDAYWEEK
              ,TOTADVRECV ,TOTADVFEERECV ,COMPLIANT_LOANS_OUTSTANDING 
                ,NEWCUSTCNTCOMPANY ,NET_REVENUE ,NEW_ORIGINATIONS 
                ,GROSS_REVENUE ,NET_WRITE_OFF ,WORAMTSUM ,TOTDEFAULTRECV
				,REACTIVE_CUSTOMER_CNT 
            FROM SKYNET.DAILYSUM_DATAMART_EADV
            GROUP BY
                PRODUCT ,PRODUCT_DESC ,POS ,INSTANCE ,BRANDCD ,BANKMODEL 
                ,COUNTRYCD ,STATE ,CITY ,ZIP ,ZONENBR ,ZONENAME ,REGIONNBR 
                ,REGIONRDO ,DIVISIONNBR ,DIVISIONDDO ,LOCNBR ,LOCATION_NAME 
                ,LOC_OPEN_DT ,LOC_CLOSE_DT ,BUSINESSDT ,LAST_REPORT_DT 
                ,LOC_LAST_REPORTED_DT ,LATITUDE ,LONGITUDE ,HOLIDAYNAME ,LASTTHURSDAY 
                ,THURSDAYWEEK                 
              ,TOTADVRECV ,TOTADVFEERECV ,COMPLIANT_LOANS_OUTSTANDING 
                ,NEWCUSTCNTCOMPANY ,NET_REVENUE ,NEW_ORIGINATIONS 
                ,GROSS_REVENUE ,NET_WRITE_OFF ,WORAMTSUM ,TOTDEFAULTRECV
				,REACTIVE_CUSTOMER_CNT  
                )
            UNPIVOT(
                VALUE1
                FOR KPINAME
                IN (   
                TOTADVRECV ,TOTADVFEERECV ,COMPLIANT_LOANS_OUTSTANDING 
                ,NEWCUSTCNTCOMPANY ,NET_REVENUE ,NEW_ORIGINATIONS 
                ,GROSS_REVENUE ,NET_WRITE_OFF ,WORAMTSUM ,TOTDEFAULTRECV
				,REACTIVE_CUSTOMER_CNT                       ))) BY ORACLE;
DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub30);

/*MERGE INTO PRODUCTION TABLE*/
PROC SQL;
	CONNECT TO ORACLE (USER=&USER. PASSWORD=&PASSWORD. PATH="BIOR");
	EXECUTE (MERGE INTO BIOR.TRANSPOSE_DAILY_SUMMARY BIOR
    USING SKYNET.TRANSPOSE_DAILY_SUMMARY_EADV UPSERT
        ON (BIOR.INSTANCE=UPSERT.INSTANCE 
		    AND BIOR.PRODUCT_DESC=UPSERT.PRODUCT_DESC 
			AND BIOR.LOCNBR=UPSERT.LOCNBR
			AND BIOR.BUSINESSDT = UPSERT.BUSINESSDT
			AND BIOR.PRODUCT = UPSERT.PRODUCT
			AND BIOR.KPINAME = UPSERT.KPINAME)
        WHEN MATCHED THEN UPDATE
           SET 
				/*BIOR.PRODUCT=UPSERT.PRODUCT,*/
				/*BIOR.PRODUCT_DESC=UPSERT.PRODUCT_DESC,*/
                BIOR.POS=UPSERT.POS,
				/*BIOR.INSTANCE=UPSERT.INSTANCE,*/
                BIOR.BRANDCD=UPSERT.BRANDCD,
                BIOR.BANKMODEL=UPSERT.BANKMODEL,
                BIOR.COUNTRYCD=UPSERT.COUNTRYCD,
                BIOR.STATE=UPSERT.STATE,
                BIOR.CITY=UPSERT.CITY,
                BIOR.ZIP=UPSERT.ZIP,
                BIOR.ZONENBR=UPSERT.ZONENBR,
                BIOR.ZONENAME=UPSERT.ZONENAME,
                BIOR.REGIONNBR=UPSERT.REGIONNBR,
                BIOR.REGIONRDO=UPSERT.REGIONRDO,
                BIOR.DIVISIONNBR=UPSERT.DIVISIONNBR,
				BIOR.DIVISIONDDO=UPSERT.DIVISIONDDO,
				/*BIOR.LOCNBR=UPSERT.LOCNBR,*/
                BIOR.LOCATION_NAME=UPSERT.LOCATION_NAME,
                BIOR.LOC_OPEN_DT=UPSERT.LOC_OPEN_DT,
                BIOR.LOC_CLOSE_DT=UPSERT.LOC_CLOSE_DT,
				/*BIOR.BUSINESSDT=UPSERT.BUSINESSDT,*/
                BIOR.LAST_REPORT_DT=UPSERT.LAST_REPORT_DT,
                BIOR.LOC_LAST_REPORTED_DT=UPSERT.LOC_LAST_REPORTED_DT,
                BIOR.LATITUDE=UPSERT.LATITUDE,
				BIOR.LONGITUDE=UPSERT.LONGITUDE,
                BIOR.HOLIDAYNAME=UPSERT.HOLIDAYNAME,
                BIOR.LASTTHURSDAY=UPSERT.LASTTHURSDAY,
                BIOR.THURSDAYWEEK=UPSERT.THURSDAYWEEK,
				/*BIOR.KPINAME = UPSERT.KPINAME,*/
				BIOR.VALUE1 = UPSERT.VALUE1
         WHEN NOT MATCHED
            THEN INSERT
                 (PRODUCT, 
		          PRODUCT_DESC, 
		          POS, 
		          INSTANCE, 
		          BRANDCD, 
		          BANKMODEL, 
		          COUNTRYCD, 
		          STATE, 
		          CITY, 
		          ZIP, 
		          ZONENBR, 
		          ZONENAME, 
		          REGIONNBR, 
		          REGIONRDO, 
		          DIVISIONNBR, 
		          DIVISIONDDO, 
		          LOCNBR, 
		          LOCATION_NAME, 
		          LOC_OPEN_DT, 
		          LOC_CLOSE_DT, 
		          BUSINESSDT, 
		          LAST_REPORT_DT, 
		          LOC_LAST_REPORTED_DT, 
		          LATITUDE, 
		          LONGITUDE, 
		          HOLIDAYNAME, 
		          LASTTHURSDAY, 
		          THURSDAYWEEK, 
				  KPINAME,
				  VALUE1)
            VALUES 
                 (UPSERT.PRODUCT, 
		          UPSERT.PRODUCT_DESC, 
		          UPSERT.POS, 
		          UPSERT.INSTANCE, 
		          UPSERT.BRANDCD, 
		          UPSERT.BANKMODEL, 
		          UPSERT.COUNTRYCD, 
		          UPSERT.STATE, 
		          UPSERT.CITY, 
		          UPSERT.ZIP, 
		          UPSERT.ZONENBR, 
		          UPSERT.ZONENAME, 
		          UPSERT.REGIONNBR, 
		          UPSERT.REGIONRDO, 
		          UPSERT.DIVISIONNBR, 
		          UPSERT.DIVISIONDDO, 
		          UPSERT.LOCNBR, 
		          UPSERT.LOCATION_NAME, 
		          UPSERT.LOC_OPEN_DT, 
		          UPSERT.LOC_CLOSE_DT, 
		          UPSERT.BUSINESSDT, 
		          UPSERT.LAST_REPORT_DT, 
		          UPSERT.LOC_LAST_REPORTED_DT, 
		          UPSERT.LATITUDE, 
		          UPSERT.LONGITUDE, 
		          UPSERT.HOLIDAYNAME, 
		          UPSERT.LASTTHURSDAY, 
		          UPSERT.THURSDAYWEEK, 
		          UPSERT.KPINAME,
				  UPSERT.VALUE1)
	) BY ORACLE;
	DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub30);

/*UPDATE STATUS TABLE*/
PROC SQL;
CONNECT TO ORACLE (USER=&USER. PW=&PASSWORD. PATH='BIOR');
	EXECUTE(UPDATE BIOR.DATAMART_STATUS
			SET EADV_STATUS = 'FINISHED'
			   ,EADV_COMPLETION_DATE = CURRENT_DATE
			WHERE SOURCE = 'BIOR.TRANSPOSE_DAILY_SUMMARY'
			)
	 BY ORACLE;
	 DISCONNECT FROM ORACLE;
%RUNQUIT(&job,&sub30);
