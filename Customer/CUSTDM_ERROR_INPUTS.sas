/***************************************************************************************
Sub Program	: CUSTDM_ERROR_INPUTS
Main		: ERROR_CHECK
Purpose		: Inputs needed to run MASTER_ERROR_CHECK in each job & sub-program
Programmer  : Spencer Hopkins
****************************************************************************************/

* INCLUDE MASTER_ERROR_CHECK PROGRAM;
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\PRODUCTION\SAS MACRO\MASTER_ERROR_CHECK.SAS";

/*
============================================================================= 
     INPUTS & SETUP
=============================================================================
*/

* NAME OF PROGRAM/JOB;
%LET JOB = CUSTOMER DATAMART;

* NAME(S) OF SUB PROGRAMS INCLUDED IN JOB;
%LET SUB1 = EADV INPUT;
%LET SUB2 = QF1_QF2 INPUT;
%LET SUB3 = QF3 INPUT;
%LET SUB4 = QF4 INPUT;
%LET SUB5 = QF5 INPUT;
%LET SUB6 = NG INPUT;
%LET SUB7 = ONLINE INPUT;
/*UPLOAD STEPS*/
%LET SUB8 = UPLOAD_EADV;
%LET SUB9 = UPLOAD_QF1_QF2;
%LET SUB10 = UPLOAD_QF3;
%LET SUB11 = UPLOAD_QF4;
%LET SUB12 = UPLOAD_QF5;
%LET SUB13 = UPLOAD_NG;
%LET SUB14 = UPLOAD_ONLINE;
%LET SUB15 = UPLOAD_FUSE;



* NAME OF TEMP ERROR TABLE;
%LET TEMP_TBL = CUSTDM_TEMP_ERROR;

* PEOPLE WHO NEED TO RECEIVE ERROR EMAIL;
%LET PPL_TO_EMAIL = 'SHOPKINS@ADVANCEAMERICA.NET','JHUBBARD@ADVANCEAMERICA.NET','JROSE@ADVANCEAMERICA.NET','RMUGABE@ADVANCEAMERICA.NET';

