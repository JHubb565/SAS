/* DROP DATASET MACRO */

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