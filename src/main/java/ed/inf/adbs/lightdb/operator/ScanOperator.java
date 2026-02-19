package ed.inf.adbs.lightdb.operator;


import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ScanOperator extends Operator {
    private static final String DELIMITER_REGEX = ",";
    
    private final TableMeta tableMeta;
    private Path dataFilePath;

    private BufferedReader reader;

    public ScanOperator(String tableName){
        if (tableName == null) throw new NullPointerException("tableName cannot be null");

        Catalog catalog = Catalog.getInstance();
        Optional<TableMeta> tableMetaOpt = catalog.getTable(tableName);
        if (!tableMetaOpt.isPresent()) {
            throw new IllegalArgumentException("Table not found in catalog: " + tableName);
        }

        this.tableMeta = tableMetaOpt.get();
        this.dataFilePath = Paths.get(this.tableMeta.getDataFile());

        openReader(); 
    }

    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return null; 
            }

            String[] values = line.split(DELIMITER_REGEX, -1); 
            List<String> valueList = Arrays.asList(values);
            return new Tuple(valueList);
        } catch (IOException e) {
            throw new RuntimeException("Error reading data file: " + this.dataFilePath, e);
        }
    }

    @Override
    public void reset() {
        closeReader();
        openReader();
    }

    private void openReader() {
        try {
            this.reader = Files.newBufferedReader(this.dataFilePath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open data file: " + this.dataFilePath, e);
        }
    }

    private void closeReader() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close data file reader: " + this.dataFilePath, e);
        }
    }

    
}
