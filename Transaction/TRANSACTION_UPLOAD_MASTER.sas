%MACRO TRANMERGEINTO(INSTANCE);
PROC SQL;
	CONNECT TO ORACLE (USER=&USER. PASSWORD=&PASSWORD. PATH="BIOR") ;
	EXECUTE (MERGE INTO BIOR.O_DEALTRANSACTION_90 BIOR
    USING SKYNET.TRAN_DATAMART_&INSTANCE. UPSERT
        ON (BIOR.INSTANCE=UPSERT.INSTANCE AND 
			BIOR.DEALNBR=UPSERT.DEALNBR AND 
            BIOR.TITLE_DEALNBR=UPSERT.TITLE_DEALNBR AND
            BIOR.DEALTRANNBR=UPSERT.DEALTRANNBR AND 
            BIOR.POSAPPLIEDCD=UPSERT.POSAPPLIEDCD AND
            BIOR.POSTRANCD=UPSERT.POSTRANCD AND
            BIOR.TRANDATE=UPSERT.TRANDATE)
		WHEN MATCHED THEN UPDATE
           SET 
                BIOR.PRODUCT=UPSERT.PRODUCT,
                BIOR.PRODUCTDESC=UPSERT.PRODUCTDESC,
                BIOR.POS=UPSERT.POS,
				BIOR.CHANNELCD=UPSERT.CHANNELCD,
/*                BIOR.INSTANCE=UPSERT.INSTANCE,*/
                BIOR.STATE=UPSERT.STATE,
                BIOR.LOCNBR=UPSERT.LOCNBR,
                BIOR.SSN=UPSERT.SSN,
                BIOR.CUSTNBR=UPSERT.CUSTNBR,
				BIOR.OMNINBR=UPSERT.OMNINBR,
                BIOR.DEAL_DT=UPSERT.DEAL_DT,
                BIOR.DEAL_DTTM=UPSERT.DEAL_DTTM,
/*                BIOR.DEALNBR=UPSERT.DEALNBR,*/
/*                BIOR.TITLE_DEALNBR=UPSERT.TITLE_DEALNBR,*/
/*                BIOR.DEALTRANNBR=UPSERT.DEALTRANNBR,*/
                BIOR.ORIGTRANNBR=UPSERT.ORIGTRANNBR,
                BIOR.VOIDFLG=UPSERT.VOIDFLG,
                BIOR.VOIDDT=UPSERT.VOIDDT,
				BIOR.DEALSTATUSCD=UPSERT.DEALSTATUSCD,
/*                BIOR.POSTRANCD=UPSERT.POSTRANCD,*/
                BIOR.STNDTRANCD=UPSERT.STNDTRANCD,
/*                BIOR.POSAPPLIEDCD=UPSERT.POSAPPLIEDCD,*/
                BIOR.STNDAPPLIEDCD=UPSERT.STNDAPPLIEDCD,
                BIOR.CI_FLG=UPSERT.CI_FLG,
                BIOR.MONETARYCD=UPSERT.MONETARYCD,
                BIOR.TRANAMT=UPSERT.TRANAMT,
                BIOR.TRANDT=UPSERT.TRANDT,
				BIOR.TRANCREATEDT=UPSERT.TRANCREATEDT,
                BIOR.BUSINESSDT=UPSERT.BUSINESSDT,
/*                BIOR.TRANDATE=UPSERT.TRANDATE,*/
                BIOR.UPDATEDT=UPSERT.UPDATEDT,
				BIOR.NCP_IND=UPSERT.NCP_IND,
                BIOR.CREATEUSR=UPSERT.CREATEUSR
         WHEN NOT MATCHED
            THEN INSERT
                 (PRODUCT, 
		          PRODUCTDESC, 
		          POS,
		          INSTANCE,
				  CHANNELCD, 
		          STATE, 
		          LOCNBR, 
		          SSN,
		          CUSTNBR,
				  OMNINBR, 
		          DEAL_DT, 
		          DEAL_DTTM, 
		          DEALNBR, 
		          TITLE_DEALNBR, 
		          DEALTRANNBR, 
		          ORIGTRANNBR, 
		          VOIDFLG, 
		          VOIDDT, 
		          DEALSTATUSCD, 
		          POSTRANCD, 
		          STNDTRANCD, 
		          POSAPPLIEDCD, 
		          STNDAPPLIEDCD, 
		          CI_FLG, 
		          MONETARYCD, 
		          TRANAMT, 
		          TRANDT, 
		          TRANCREATEDT, 
		          BUSINESSDT, 
		          TRANDATE, 
		          UPDATEDT, 
		          NCP_IND, 
		          CREATEUSR)
            VALUES 
                 (UPSERT.PRODUCT, 
		          UPSERT.PRODUCTDESC, 
		          UPSERT.POS,
		          UPSERT.INSTANCE,
				  UPSERT.CHANNELCD, 
		          UPSERT.STATE, 
		          UPSERT.LOCNBR, 
		          UPSERT.SSN,
		          UPSERT.CUSTNBR,
				  UPSERT.OMNINBR,  
		          UPSERT.DEAL_DT, 
		          UPSERT.DEAL_DTTM, 
		          UPSERT.DEALNBR, 
		          UPSERT.TITLE_DEALNBR, 
		          UPSERT.DEALTRANNBR, 
		          UPSERT.ORIGTRANNBR, 
		          UPSERT.VOIDFLG, 
		          UPSERT.VOIDDT, 
		          UPSERT.DEALSTATUSCD, 
		          UPSERT.POSTRANCD, 
		          UPSERT.STNDTRANCD, 
		          UPSERT.POSAPPLIEDCD, 
		          UPSERT.STNDAPPLIEDCD, 
		          UPSERT.CI_FLG, 
		          UPSERT.MONETARYCD, 
		          UPSERT.TRANAMT, 
		          UPSERT.TRANDT, 
		          UPSERT.TRANCREATEDT, 
		          UPSERT.BUSINESSDT, 
		          UPSERT.TRANDATE, 
		          UPSERT.UPDATEDT, 
		          UPSERT.NCP_IND, 
		          UPSERT.CREATEUSR)
	) BY ORACLE;
	DISCONNECT FROM ORACLE;
QUIT;
%MEND;
