DO $$
    DECLARE
        start_date DATE := '2018-01-01';
        end_date DATE := '2018-01-31';
    BEGIN
        INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
        SELECT
            '2017-12-31' AS on_date,
            account_rk,
            balance_out,
            balance_out * COALESCE((SELECT reduced_cource FROM DS.MD_EXCHANGE_RATE_D WHERE data_actual_date = '2017-12-31'), 1) AS balance_out_rub
        FROM
            DS.FT_BALANCE_F;

        WHILE start_date <= end_date LOOP
                CALL ds.fill_account_turnover_f(start_date);
                CALL ds.fill_account_balance_f(start_date);
                start_date := start_date + INTERVAL '1 day';
            END LOOP;
    END $$;
