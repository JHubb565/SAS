/*  UPDATE BIOR PRODUCTION TABLE WITH NEW DATA  */
%INCLUDE "E:\SHARED\CADA\SAS SOURCE CODE\DEVELOPMENT\JHUBBARD\skynet-v2\Skynet Redesign\Datamart Redesign\Deal\DEALSUM_MASTER_UPLOAD.SAS";
%DEALMERGEINTO(EADV)
%DEALMERGEINTO(QF1QF2)
%DEALMERGEINTO(QF1T)
%DEALMERGEINTO(QF3TXTITLE)
%DEALMERGEINTO(QF3TETL)
%DEALMERGEINTO(QF3TTOC)
%DEALMERGEINTO(QF3FAI)
%DEALMERGEINTO(QF4TLP)
%DEALMERGEINTO(QF4PDL)
%DEALMERGEINTO(QF5ILP)
%DEALMERGEINTO(QF5PDL)
%DEALMERGEINTO(QF5TLP)
%DEALMERGEINTO(NG)
%DEALMERGEINTO(OL)
%DEALMERGEINTO(LOC)
