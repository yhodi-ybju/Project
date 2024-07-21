CREATE SCHEMA DM;

CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F (
    ON_DATE DATE NOT NULL,
    ACCOUNT_RK NUMERIC NOT NULL,
    CREDIT_AMOUNT NUMERIC(23,8),
    CREDIT_AMOUNT_RUB NUMERIC(23,8),
    DEBET_AMOUNT NUMERIC(23,8),
    DEBET_AMOUNT_RUB NUMERIC(23,8)
);

CREATE TABLE DM.DM_ACCOUNT_BALANCE_F (
    ON_DATE DATE NOT NULL,
    ACCOUNT_RK NUMERIC NOT NULL,
    BALANCE_OUT NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8)
);
