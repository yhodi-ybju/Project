package ru.victoria.project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@org.springframework.stereotype.Service
public class DataLoadService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final SimpleDateFormat[] DATE_FORMATS = {
            new SimpleDateFormat("dd-MM-yyyy"),
            new SimpleDateFormat("dd.MM.yyyy"),
            new SimpleDateFormat("yyyy-MM-dd")
    };

    public void loadData() {
        try {
            logStart("loadCsvData");

            loadBalances("src/main/resources/data/ft_balance_f.csv");
            loadPostings("src/main/resources/data/ft_posting_f.csv");
            loadAccounts("src/main/resources/data/md_account_d.csv");
            loadCurrencies("src/main/resources/data/md_currency_d.csv");
            loadExchangeRates("src/main/resources/data/md_exchange_rate_d.csv");
            loadLedgerAccounts("src/main/resources/data/md_ledger_account_s.csv");

            Thread.sleep(5000);

            logEnd("loadCsvData", "SUCCESS", null);
        } catch (Exception e) {
            logEnd("loadCsvData", "FAILURE", e.getMessage());
        }
    }

    private Date parseDate(String dateStr) throws ParseException {
        ParseException lastException = null;
        for (SimpleDateFormat dateFormat : DATE_FORMATS) {
            try {
                dateFormat.setLenient(false);
                return new Date(dateFormat.parse(dateStr).getTime());
            } catch (ParseException e) {
                lastException = e;
            }
        }
        throw new ParseException("Cannot parse date: \"" + dateStr + "\"", lastException != null ? lastException.getErrorOffset() : 0);
    }


    private void loadBalances(String filePath) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (values.length < 4) {
                    System.err.println("Invalid line " + line);
                    continue;
                }
                try {
                    Date onDate = parseDate(values[0]);
                    int accountRk = Integer.parseInt(values[1]);
                    int currencyRk = Integer.parseInt(values[2]);
                    float balanceOut = Float.parseFloat(values[3]);

                    jdbcTemplate.update("INSERT INTO DS.FT_BALANCE_F (ON_DATE, ACCOUNT_RK, CURRENCY_RK, BALANCE_OUT) VALUES (?, ?, ?, ?) " +
                                    "ON CONFLICT (ON_DATE, ACCOUNT_RK) DO UPDATE SET CURRENCY_RK = EXCLUDED.CURRENCY_RK, BALANCE_OUT = EXCLUDED.BALANCE_OUT",
                            onDate, accountRk, currencyRk, balanceOut);
                } catch (Exception e) {
                    System.err.println("Invalid line " + line);
                }
            }
        }
    }


    private void loadPostings(String filePath) throws Exception {
        jdbcTemplate.update("TRUNCATE TABLE DS.FT_POSTING_F");

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (values.length < 5) {
                    System.err.println("Invalid line " + line);
                    continue;
                }
                try {
                    Date operDate = parseDate(values[0]);
                    int creditAccountRk = Integer.parseInt(values[1]);
                    int debetAccountRk = Integer.parseInt(values[2]);
                    float creditAmount = Float.parseFloat(values[3]);
                    float debetAmount = Float.parseFloat(values[4]);

                    jdbcTemplate.update("INSERT INTO DS.FT_POSTING_F (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK, CREDIT_AMOUNT, DEBET_AMOUNT) VALUES (?, ?, ?, ?, ?)",
                            operDate, creditAccountRk, debetAccountRk, creditAmount, debetAmount);
                } catch (IllegalArgumentException | ParseException e) {
                    System.err.println("Invalid line " + line);
                }
            }
        }
    }

    private void loadAccounts(String filePath) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (values.length < 7) {
                    System.err.println("Invalid line " + line);
                    continue;
                }
                try {
                    Date dataActualDate = parseDate(values[0]);
                    Date dataActualEndDate = parseDate(values[1]);
                    int accountRk = Integer.parseInt(values[2]);
                    String accountNumber = values[3];
                    String charType = values[4];
                    int currencyRk = Integer.parseInt(values[5]);
                    String currencyCode = values[6];

                    jdbcTemplate.update("INSERT INTO DS.MD_ACCOUNT_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, ACCOUNT_RK, ACCOUNT_NUMBER, CHAR_TYPE, CURRENCY_RK, CURRENCY_CODE) VALUES (?, ?, ?, ?, ?, ?, ?) " +
                                    "ON CONFLICT (DATA_ACTUAL_DATE, ACCOUNT_RK) DO UPDATE SET DATA_ACTUAL_END_DATE = EXCLUDED.DATA_ACTUAL_END_DATE, ACCOUNT_NUMBER = EXCLUDED.ACCOUNT_NUMBER, CHAR_TYPE = EXCLUDED.CHAR_TYPE, CURRENCY_RK = EXCLUDED.CURRENCY_RK, CURRENCY_CODE = EXCLUDED.CURRENCY_CODE",
                            dataActualDate, dataActualEndDate, accountRk, accountNumber, charType, currencyRk, currencyCode);
                } catch (IllegalArgumentException | ParseException e) {
                    System.err.println("Invalid line " + line);
                }
            }
        }
    }

    private void loadCurrencies(String filePath) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                try {
                    int currencyRk = Integer.parseInt(values[0]);
                    Date dataActualDate = parseDate(values[1]);
                    Date dataActualEndDate = values.length > 2 && !values[2].isEmpty() ? parseDate(values[2]) : null;
                    String currencyCode = values.length > 3 ? values[3] : "";
                    String codeIsoChar = values.length > 4 ? values[4] : "";

                    jdbcTemplate.update("INSERT INTO DS.MD_CURRENCY_D (CURRENCY_RK, DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_CODE, CODE_ISO_CHAR) VALUES (?, ?, ?, ?, ?) " +
                                    "ON CONFLICT (CURRENCY_RK, DATA_ACTUAL_DATE) DO UPDATE SET DATA_ACTUAL_END_DATE = EXCLUDED.DATA_ACTUAL_END_DATE, CURRENCY_CODE = EXCLUDED.CURRENCY_CODE, CODE_ISO_CHAR = EXCLUDED.CODE_ISO_CHAR",
                            currencyRk, dataActualDate, dataActualEndDate, currencyCode, codeIsoChar);
                } catch (IllegalArgumentException | ParseException e) {
                    System.err.println("Invalid line " + line);
                }
            }
        }
    }

    private void loadExchangeRates(String filePath) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (values.length < 5) {
                    System.err.println("Invalid line " + line);
                    continue;
                }
                try {
                    Date dataActualDate = parseDate(values[0]);
                    Date dataActualEndDate = parseDate(values[1]);
                    int currencyRk = Integer.parseInt(values[2]);
                    float reducedCource = Float.parseFloat(values[3]);
                    String codeIsoNum = values[4];

                    jdbcTemplate.update("INSERT INTO DS.MD_EXCHANGE_RATE_D (DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_RK, REDUCED_COURCE, CODE_ISO_NUM) VALUES (?, ?, ?, ?, ?) " +
                                    "ON CONFLICT (DATA_ACTUAL_DATE, CURRENCY_RK) DO UPDATE SET DATA_ACTUAL_END_DATE = EXCLUDED.DATA_ACTUAL_END_DATE, REDUCED_COURCE = EXCLUDED.REDUCED_COURCE, CODE_ISO_NUM = EXCLUDED.CODE_ISO_NUM",
                            dataActualDate, dataActualEndDate, currencyRk, reducedCource, codeIsoNum);
                } catch (Exception e) {
                    System.err.println("Invalid line " + line);
                    e.printStackTrace();
                }
            }
        }
    }

    private void loadLedgerAccounts(String filePath) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (values.length < 12) {
                    System.err.println("Invalid line: " + line);
                    continue;
                }
                try {
                    String chapter = values[0];
                    String chapterName = values[1];
                    int sectionNumber = parseInteger(values[2], 0);
                    String sectionName = values[3];
                    String subSectionName = values[4];
                    int ledger1Account = parseInteger(values[5], 0);
                    String ledger1AccountName = values[6];
                    int ledgerAccount = parseInteger(values[7], 0);
                    String ledgerAccountName = values[8];
                    String characteristic = values[9];
                    Date startDate = parseDate(values[10]);
                    Date endDate = parseDate(values[11]);

                    jdbcTemplate.update("INSERT INTO DS.MD_LEDGER_ACCOUNT_S (CHAPTER, CHAPTER_NAME, SECTION_NUMBER, SECTION_NAME, SUBSECTION_NAME, LEDGER1_ACCOUNT, LEDGER1_ACCOUNT_NAME, LEDGER_ACCOUNT, LEDGER_ACCOUNT_NAME, CHARACTERISTIC, START_DATE, END_DATE) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            chapter, chapterName, sectionNumber, sectionName, subSectionName, ledger1Account, ledger1AccountName, ledgerAccount, ledgerAccountName, characteristic, startDate, endDate);
                } catch (IllegalArgumentException e) {
                    System.err.println("Error processing line: " + line + " Error: " + e.getMessage());
                }
            }
        }
    }

    private int parseInteger(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }


    private void logStart(String processName) {
        jdbcTemplate.update("INSERT INTO LOGS.LOAD_LOG (PROCESS_NAME, START_TIME) VALUES (?, ?)",
                processName, new Timestamp(System.currentTimeMillis()));
    }

    private void logEnd(String processName, String status, String comment) {
        jdbcTemplate.update("UPDATE LOGS.LOAD_LOG SET END_TIME = ?, STATUS = ?, COMMENT = ? WHERE PROCESS_NAME = ? AND END_TIME IS NULL",
                new Timestamp(System.currentTimeMillis()), status, comment, processName);
    }
}
