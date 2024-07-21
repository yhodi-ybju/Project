CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
    LANGUAGE plpgsql
AS $$
DECLARE
    prev_date DATE;
BEGIN
    prev_date := i_OnDate - INTERVAL '1 day';
    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate,
        acc.account_rk,
        CASE
            WHEN acc.char_type = 'А' THEN COALESCE(prev_bal.balance_out, 0) + COALESCE(turn.debet_amount, 0) - COALESCE(turn.credit_amount, 0)
            WHEN acc.char_type = 'П' THEN COALESCE(prev_bal.balance_out, 0) - COALESCE(turn.debet_amount, 0) + COALESCE(turn.credit_amount, 0)
            END AS balance_out,
        CASE
            WHEN acc.char_type = 'А' THEN COALESCE(prev_bal.balance_out_rub, 0) + COALESCE(turn.debet_amount_rub, 0) - COALESCE(turn.credit_amount_rub, 0)
            WHEN acc.char_type = 'П' THEN COALESCE(prev_bal.balance_out_rub, 0) - COALESCE(turn.debet_amount_rub, 0) + COALESCE(turn.credit_amount_rub, 0)
            END AS balance_out_rub
    FROM
        DS.MD_ACCOUNT_D acc
            LEFT JOIN
        DM.DM_ACCOUNT_BALANCE_F prev_bal
        ON
            acc.account_rk = prev_bal.account_rk AND prev_bal.on_date = prev_date
            LEFT JOIN
        DM.DM_ACCOUNT_TURNOVER_F turn
        ON
            acc.account_rk = turn.account_rk AND turn.on_date = i_OnDate
    WHERE
        i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;

    INSERT INTO LOGS.LOAD_LOG (PROCESS_NAME, START_TIME, END_TIME, STATUS, COMMENT)
    VALUES ('fill_account_balance_f', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'SUCCESS', 'Balance calculated successfully for ' || i_OnDate);

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO LOGS.LOAD_LOG (PROCESS_NAME, START_TIME, END_TIME, STATUS, COMMENT)
        VALUES ('fill_account_balance_f', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'FAILURE', SQLERRM);
        RAISE;
END;
$$;
