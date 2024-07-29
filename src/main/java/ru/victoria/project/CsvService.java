package ru.victoria.project;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

@Service
public class CsvService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String path = "src/main/resources/data/";
    private static final String script = "scripts/1.4/create_dm_f101_round_f_v2.sql";

    public void exportDataToCsv(String fileName) {
        String outputPath = Paths.get(path, fileName).toString();
        String query = "SELECT * FROM dm.dm_f101_round_f";

        try (CSVWriter writer = new CSVWriter(new FileWriter(outputPath))) {
            jdbcTemplate.query(query, (ResultSet rs) -> {
                try {
                    String[] header = {"FROM_DATE", "TO_DATE", "CHAPTER", "LEDGER_ACCOUNT", "CHARACTERISTIC",
                            "BALANCE_IN_RUB", "R_BALANCE_IN_RUB", "BALANCE_IN_VAL", "R_BALANCE_IN_VAL",
                            "BALANCE_IN_TOTAL", "R_BALANCE_IN_TOTAL", "TURN_DEB_RUB", "R_TURN_DEB_RUB",
                            "TURN_DEB_VAL", "R_TURN_DEB_VAL", "TURN_DEB_TOTAL", "R_TURN_DEB_TOTAL",
                            "TURN_CRE_RUB", "R_TURN_CRE_RUB", "TURN_CRE_VAL", "R_TURN_CRE_VAL",
                            "TURN_CRE_TOTAL", "R_TURN_CRE_TOTAL", "BALANCE_OUT_RUB", "R_BALANCE_OUT_RUB",
                            "BALANCE_OUT_VAL", "R_BALANCE_OUT_VAL", "BALANCE_OUT_TOTAL", "R_BALANCE_OUT_TOTAL"};
                    writer.writeNext(header);
                    while (rs.next()) {
                        String[] row = {
                                rs.getString("FROM_DATE"),
                                rs.getString("TO_DATE"),
                                rs.getString("CHAPTER"),
                                rs.getString("LEDGER_ACCOUNT"),
                                rs.getString("CHARACTERISTIC"),
                                rs.getString("BALANCE_IN_RUB"),
                                rs.getString("R_BALANCE_IN_RUB"),
                                rs.getString("BALANCE_IN_VAL"),
                                rs.getString("R_BALANCE_IN_VAL"),
                                rs.getString("BALANCE_IN_TOTAL"),
                                rs.getString("R_BALANCE_IN_TOTAL"),
                                rs.getString("TURN_DEB_RUB"),
                                rs.getString("R_TURN_DEB_RUB"),
                                rs.getString("TURN_DEB_VAL"),
                                rs.getString("R_TURN_DEB_VAL"),
                                rs.getString("TURN_DEB_TOTAL"),
                                rs.getString("R_TURN_DEB_TOTAL"),
                                rs.getString("TURN_CRE_RUB"),
                                rs.getString("R_TURN_CRE_RUB"),
                                rs.getString("TURN_CRE_VAL"),
                                rs.getString("R_TURN_CRE_VAL"),
                                rs.getString("TURN_CRE_TOTAL"),
                                rs.getString("R_TURN_CRE_TOTAL"),
                                rs.getString("BALANCE_OUT_RUB"),
                                rs.getString("R_BALANCE_OUT_RUB"),
                                rs.getString("BALANCE_OUT_VAL"),
                                rs.getString("R_BALANCE_OUT_VAL"),
                                rs.getString("BALANCE_OUT_TOTAL"),
                                rs.getString("R_BALANCE_OUT_TOTAL")
                        };
                        writer.writeNext(row);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Error processing row", e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Error writing CSV file", e);
        }
    }

    public void importDataFromCsv(String fileName) {
        String inputPath = Paths.get(path, fileName).toString();
        try {
            String createTableQuery = new String(Files.readAllBytes(Paths.get(script)));
            jdbcTemplate.execute(createTableQuery);
        } catch (IOException e) {
            throw new RuntimeException("Error reading SQL file", e);
        }

        try (CSVReader reader = new CSVReader(new FileReader(inputPath))) {
            List<String[]> records = reader.readAll();
            String insertQuery = """
                    INSERT INTO dm.dm_f101_round_f_v2 (FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC, 
                                                       BALANCE_IN_RUB, R_BALANCE_IN_RUB, BALANCE_IN_VAL, R_BALANCE_IN_VAL, 
                                                       BALANCE_IN_TOTAL, R_BALANCE_IN_TOTAL, TURN_DEB_RUB, R_TURN_DEB_RUB, 
                                                       TURN_DEB_VAL, R_TURN_DEB_VAL, TURN_DEB_TOTAL, R_TURN_DEB_TOTAL, 
                                                       TURN_CRE_RUB, R_TURN_CRE_RUB, TURN_CRE_VAL, R_TURN_CRE_VAL, 
                                                       TURN_CRE_TOTAL, R_TURN_CRE_TOTAL, BALANCE_OUT_RUB, R_BALANCE_OUT_RUB, 
                                                       BALANCE_OUT_VAL, R_BALANCE_OUT_VAL, BALANCE_OUT_TOTAL, R_BALANCE_OUT_TOTAL)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """;
            for (int i = 1; i < records.size(); i++) {
                String[] row = records.get(i);
                if (row.length < 29) {
                    throw new RuntimeException("Invalid row length at line: " + (i + 1));
                }

                Date fromDate = parseDate(row[0]);
                Date toDate = parseDate(row[1]);
                String chapter = row[2];
                String ledgerAccount = row[3];
                String characteristic = row[4];
                java.math.BigDecimal balanceInRub = parseBigDecimal(row[5]);
                java.math.BigDecimal rBalanceInRub = parseBigDecimal(row[6]);
                java.math.BigDecimal balanceInVal = parseBigDecimal(row[7]);
                java.math.BigDecimal rBalanceInVal = parseBigDecimal(row[8]);
                java.math.BigDecimal balanceInTotal = parseBigDecimal(row[9]);
                java.math.BigDecimal rBalanceInTotal = parseBigDecimal(row[10]);
                java.math.BigDecimal turnDebRub = parseBigDecimal(row[11]);
                java.math.BigDecimal rTurnDebRub = parseBigDecimal(row[12]);
                java.math.BigDecimal turnDebVal = parseBigDecimal(row[13]);
                java.math.BigDecimal rTurnDebVal = parseBigDecimal(row[14]);
                java.math.BigDecimal turnDebTotal = parseBigDecimal(row[15]);
                java.math.BigDecimal rTurnDebTotal = parseBigDecimal(row[16]);
                java.math.BigDecimal turnCreRub = parseBigDecimal(row[17]);
                java.math.BigDecimal rTurnCreRub = parseBigDecimal(row[18]);
                java.math.BigDecimal turnCreVal = parseBigDecimal(row[19]);
                java.math.BigDecimal rTurnCreVal = parseBigDecimal(row[20]);
                java.math.BigDecimal turnCreTotal = parseBigDecimal(row[21]);
                java.math.BigDecimal rTurnCreTotal = parseBigDecimal(row[22]);
                java.math.BigDecimal balanceOutRub = parseBigDecimal(row[23]);
                java.math.BigDecimal rBalanceOutRub = parseBigDecimal(row[24]);
                java.math.BigDecimal balanceOutVal = parseBigDecimal(row[25]);
                java.math.BigDecimal rBalanceOutVal = parseBigDecimal(row[26]);
                java.math.BigDecimal balanceOutTotal = parseBigDecimal(row[27]);
                java.math.BigDecimal rBalanceOutTotal = parseBigDecimal(row[28]);

                jdbcTemplate.update(insertQuery,
                        fromDate, toDate, chapter, ledgerAccount, characteristic,
                        balanceInRub, rBalanceInRub, balanceInVal, rBalanceInVal,
                        balanceInTotal, rBalanceInTotal, turnDebRub, rTurnDebRub,
                        turnDebVal, rTurnDebVal, turnDebTotal, rTurnDebTotal,
                        turnCreRub, rTurnCreRub, turnCreVal, rTurnCreVal,
                        turnCreTotal, rTurnCreTotal, balanceOutRub, rBalanceOutRub,
                        balanceOutVal, rBalanceOutVal, balanceOutTotal, rBalanceOutTotal
                );
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV file", e);
        } catch (CsvException e) {
            throw new RuntimeException(e);
        }
    }

    private Date parseDate(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return null;
        }
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            return new Date(sdf.parse(dateStr).getTime());
        } catch (ParseException e) {
            throw new RuntimeException("Error parsing date: " + dateStr, e);
        }
    }

    private java.math.BigDecimal parseBigDecimal(String numberStr) {
        if (numberStr == null || numberStr.isEmpty()) {
            return java.math.BigDecimal.ZERO;
        }
        try {
            return new java.math.BigDecimal(numberStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Error parsing number: " + numberStr, e);
        }
    }
}