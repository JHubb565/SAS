/***************************************************************************************
Sub Program	: TranDM_Error_Check
Main		: ERROR_CHECK
Purpose		: Inputs needed to run MASTER_ERROR_CHECK in each job & sub-program
Programmer  : Nathan Rochester
****************************************************************************************/

* INCLUDE MASTER_ERROR_CHECK PROGRAM;
%include "E:\Shared\CADA\SAS Source Code\Production\SAS Macro\MASTER_ERROR_CHECK.sas";
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\TOP SECRET PROGRAM.SAS";

/*
============================================================================= 
     INPUTS & SETUP
=============================================================================
*/

* NAME OF PROGRAM/JOB;
%LET job = Transaction Datamart;

* NAME(S) OF SUB PROGRAMS INCLUDED IN JOB;
%LET sub1 = Delete From;
%LET sub2 = EADV Tran;
%LET sub3 = QF1I Tran;
%LET sub4 = QF1T Tran;
%LET sub5 = QF2 Tran;
%LET sub6 = QF3 Tran;
%LET sub7 = QF4P Tran;
%LET sub8 = QF4T Tran;
%LET sub9 = QF5 Tran;
%LET sub10 = ROLLUP Tran;
%LET sub11 = NG Tran;
%LET sub12 = AANET Tran;
%LET sub13 = KSL Tran;
%LET sub14 = CNU Tran;
%LET sub15 = UPLOAD Tran;
%LET sub16 = INDEX Tran;
%LET sub17 = DROP Tran;
%LET sub18 = QF3TT Tran;
%LET sub19 = DRIVER Tran;

* NAME OF TEMP ERROR TABLE;
%LET TEMP_TBL = Transaction_Temp_Error;

* PEOPLE WHO NEED TO RECEIVE ERROR EMAIL;
%LET PPL_TO_EMAIL = 'JHUBBARD@advanceamerica.net';



