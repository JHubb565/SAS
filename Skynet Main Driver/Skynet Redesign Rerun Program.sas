OPTIONS SYMBOLGEN MPRINT MLOGIC NOXWAIT;
%INCLUDE "\\CSSSASAPP\CADA\SAS SOURCE CODE\PRODUCTION\SERVICE ACCOUNTS\SVC_SASUSER.SAS";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";




/*PROGRAM LOCATIONS*/
%LET WHEREPRGRMSAT_REDESIGN = E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\SKYNET REDESIGN;

%INCLUDE "&WHEREPRGRMSAT_REDESIGN.\SKYNET REDESIGN RERUN CREDENTIALS.SAS";


/*TAKE INPUTS FROM USER ON WHAT TO RERUN*/

%GLOBAL EADV;
%GLOBAL QFUND1;
%GLOBAL QFUND2;
%GLOBAL QFUND3;
%GLOBAL QFUND4;
%GLOBAL QFUND5;
%GLOBAL NG;
%GLOBAL ONLINE;

%LET EADV_USE = %UPCASE(&EADV.);
%LET QFUND1_USE = %UPCASE(&QFUND1.);
%LET QFUND2_USE = %UPCASE(&QFUND21.);
%LET QFUND3_USE = %UPCASE(&QFUND31.);
%LET QFUND4_USE = %UPCASE(&QFUND41.);
%LET QFUND5_USE = %UPCASE(&QFUND51.);
%LET NG_USE = %UPCASE(&NG1.);
%LET ONLINE_USE = %UPCASE(&ONLINE1.);








/*USE INPUTS FROM USER TO RESET STAGING TABLES AND TRUNCATE TABLES ASSOCIATED*/



