CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
    LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub)
    SELECT
        i_OnDate,
        credit_account_rk,
        SUM(credit_amount),
        SUM(credit_amount * COALESCE(er.reduced_cource, 1))
    FROM
        DS.FT_POSTING_F pf
            LEFT JOIN
        DS.MD_EXCHANGE_RATE_D er
        ON
            pf.oper_date = er.data_actual_date
    WHERE
        pf.oper_date = i_OnDate
    GROUP BY
        credit_account_rk;

    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, debet_amount, debet_amount_rub)
    SELECT
        i_OnDate,
        debet_account_rk,
        SUM(debet_amount),
        SUM(debet_amount * COALESCE(er.reduced_cource, 1))
    FROM
        DS.FT_POSTING_F pf
            LEFT JOIN
        DS.MD_EXCHANGE_RATE_D er
        ON
            pf.oper_date = er.data_actual_date
    WHERE
        pf.oper_date = i_OnDate
    GROUP BY
        debet_account_rk;
    INSERT INTO LOGS.LOAD_LOG (PROCESS_NAME, START_TIME, END_TIME, STATUS, COMMENT)
    VALUES ('fill_account_turnover_f', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'SUCCESS', 'Turnover calculated successfully for ' || i_OnDate);

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO LOGS.LOAD_LOG (PROCESS_NAME, START_TIME, END_TIME, STATUS, COMMENT)
        VALUES ('fill_account_turnover_f', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'FAILURE', SQLERRM);
        RAISE;
END;
$$;
