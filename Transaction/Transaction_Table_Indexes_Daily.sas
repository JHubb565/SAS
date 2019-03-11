/*OPTIONS FULLSTIMER SOURCE SOURCE2 MSGLEVEL=I MPRINT NOTES;*/
/*PROC OPTIONS GROUP=MEMORY;*/
/*PROC OPTIONS GROUP=PERFORMANCE;*/
/*RUN;*/
/*LIBNAME _ALL_ LIST;*/
/*PROC OPTIONS OPTION=WORK;*/
/*PROC OPTIONS OPTION=UTILLOC;*/
/*RUN;*/
/*DATA _NULL_;*/
/*%PUT This job started on &sysdate at &systime;*/
/*RUN;*/

OPTIONS MPRINT SYMBOLGEN;

%macro _eg_conditional_dropds /parmbuff;
   	%let num=1;
	/* flags to determine whether a PROC SQL step is needed */
	/* or even started yet                                  */
	%let stepneeded=0;
	%let stepstarted=0;
   	%let dsname=%scan(&syspbuff,&num,',()');
	%do %while(&dsname ne);	
		%if %sysfunc(exist(&dsname)) %then %do;
			%let stepneeded=1;
			%if (&stepstarted eq 0) %then %do;
				proc sql;
				%let stepstarted=1;
			%end;
				drop table &dsname;
		%end;
		%if %sysfunc(exist(&dsname,view)) %then %do;
			%let stepneeded=1;
			%if (&stepstarted eq 0) %then %do;
				proc sql;
				%let stepstarted=1;
			%end;
				drop view &dsname;
		%end;
		%let num=%eval(&num+1);
      	%let dsname=%scan(&syspbuff,&num,',()');
	%end;
	%if &stepstarted %then %do;
		quit;
	%end;
%mend _eg_conditional_dropds;

DATA TRANDM.TRANSACTION_TABLE_QF2_UPDATE;
INPUT X;
CARDS; 
1
;
RUN;

%_EG_CONDITIONAL_DROPDS(TRANDM.TRANSACTION_TABLE_QF2_UPDATE);


PROC SQL;
CREATE TABLE TEMPTABL.TRANSACTION_TABLE_AANET AS 
SELECT *
FROM TRANDMC.TRANSACTION_TABLE_AANET_SC;
QUIT;

PROC SQL;
CREATE TABLE O_DT_UP AS 
SELECT 
	PRODUCT
   ,PRODUCTDESC
   ,POS
   ,INSTANCE
   ,LOCNBR
   ,SSN
   ,CUSTNBR
   ,DEALNBR
   ,TITLE_DEALNBR
   ,DEALTRANNBR
   ,ORIGTRANNBR
   ,VOIDFLG
   ,VOIDDT
   ,DEALSTATUSCD
   ,POSTRANCD
   ,STNDTRANCD
   ,POSAPPLIEDCD
   ,STNDAPPLIEDCD
   ,CI_FLG
   ,MONETARYCD
   ,TRANAMT
   ,TRANDT
   ,TRANCREATEDT
   ,BUSINESSDT
   ,DHMS(DATEPART(TRANDT),0,0,0) 	  AS TRANDATE   FORMAT=DATETIME9.
   ,UPDATEDT
   ,NCP_IND
FROM BIOR.O_DEALTRANSACTION_ALL;
QUIT;