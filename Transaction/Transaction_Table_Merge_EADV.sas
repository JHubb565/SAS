PROC SQL;    
CONNECT TO ORACLE (USER=SVC_SASUSER PASSWORD="{SAS002}8E8C78044906924E47EBAD620CFCE3294AE9C1533DF26A16" PATH="BIOR") ;
EXECUTE (MERGE INTO BIOR.O_DEALTRANSACTION_ALL BIOR
    USING  TEMPTABLES.TRANSACTION_TABLE_UPDATE1 UPSERT
        ON (BIOR.INSTANCE=UPSERT.INSTANCE AND BIOR.DEALNBR=UPSERT.DEALNBR AND 
            BIOR.TITLE_DEALNBR=UPSERT.TITLE_DEALNBR AND
            BIOR.DEALTRANNBR=UPSERT.DEALTRANNBR AND 
            BIOR.POSAPPLIEDCD=UPSERT.POSAPPLIEDCD AND
            BIOR.POSTRANCD=UPSERT.POSTRANCD AND
            BIOR.TRANAMT=UPSERT.TRANAMT)
        WHEN MATCHED THEN UPDATE
           SET 
                BIOR.PRODUCT=UPSERT.PRODUCT,
                BIOR.PRODUCTDESC=UPSERT.PRODUCTDESC,
                BIOR.POS=UPSERT.POS,
				BIOR.STATE=UPSERT.STATE,
                BIOR.LOCNBR=UPSERT.LOCNBR,
                BIOR.CUSTNBR=UPSERT.CUSTNBR,
				BIOR.DEAL_DT=UPSERT.DEAL_DT,
				BIOR.DEAL_DTTM=UPSERT.DEAL_DTTM,
                BIOR.ORIGTRANNBR=UPSERT.ORIGTRANNBR,
                BIOR.VOIDFLG=UPSERT.VOIDFLG,
                BIOR.VOIDDT=UPSERT.VOIDDT,
                BIOR.STNDTRANCD=UPSERT.STNDTRANCD,
                BIOR.STNDAPPLIEDCD=UPSERT.STNDAPPLIEDCD,
                BIOR.CI_FLG=UPSERT.CI_FLG,
                BIOR.MONETARYCD=UPSERT.MONETARYCD,
                BIOR.TRANDT=UPSERT.TRANDT,
                BIOR.TRANCREATEDT=UPSERT.TRANCREATEDT,
                BIOR.BUSINESSDT=UPSERT.BUSINESSDT,
                BIOR.UPDATEDT=UPSERT.UPDATEDT,
                BIOR.NCP_IND=UPSERT.NCP_IND,
				BIOR.CREATEUSR=UPSERT.CREATEUSR
         WHEN NOT MATCHED 
            THEN INSERT
                 (PRODUCT
                 ,PRODUCTDESC
                 ,POS
                 ,INSTANCE
				 ,STATE
                 ,LOCNBR
                 ,SSN
                 ,CUSTNBR
				 ,DEAL_DT
				 ,DEAL_DTTM
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
				 ,TRANDATE
                 ,UPDATEDT
                 ,NCP_IND
				 ,CREATEUSR)
            VALUES 
                 (UPSERT.PRODUCT
                 ,UPSERT.PRODUCTDESC
                 ,UPSERT.POS
				 ,UPSERT.STATE
                 ,UPSERT.INSTANCE
                 ,UPSERT.LOCNBR
                 ,UPSERT.SSN
                 ,UPSERT.CUSTNBR
				 ,UPSERT.DEAL_DT
				 ,UPSERT.DEAL_DTTM
                 ,UPSERT.DEALNBR
                 ,UPSERT.TITLE_DEALNBR
                 ,UPSERT.DEALTRANNBR
                 ,UPSERT.ORIGTRANNBR
                 ,UPSERT.VOIDFLG
                 ,UPSERT.VOIDDT
                 ,UPSERT.DEALSTATUSCD
                 ,UPSERT.POSTRANCD
                 ,UPSERT.STNDTRANCD
                 ,UPSERT.POSAPPLIEDCD
                 ,UPSERT.STNDAPPLIEDCD
                 ,UPSERT.CI_FLG
                 ,UPSERT.MONETARYCD
                 ,UPSERT.TRANAMT
                 ,UPSERT.TRANDT
                 ,UPSERT.TRANCREATEDT
                 ,UPSERT.BUSINESSDT
				 ,UPSERT.TRANDATE
                 ,UPSERT.UPDATEDT
                 ,UPSERT.NCP_IND
				 ,UPSERT.CREATEUSR)) BY ORACLE;
DISCONNECT FROM ORACLE;
QUIT;
                 
                 