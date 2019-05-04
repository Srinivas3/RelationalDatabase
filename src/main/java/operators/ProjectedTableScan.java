package operators;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Constants;
import utils.Utils;

import java.io.*;
import java.util.*;

public class ProjectedTableScan implements Operator {
    List<ColumnDefinition> colDefs;
    Map<String, PrimitiveValue> tuple;
    boolean isFirstCall;
    Map<String, Integer> schema;

    public String getTableName() {
        return tableName;
    }

    String tableName;
    DataInputStream[] dataInputStreams;
    List<Iterator<PrimitiveValue>> cachedTuplesIterators;
    boolean[] isCached;
    boolean[] isCachable;
    int numCols;
    int totalNumTuples;
    int scannedTuples;
    Set<String> schemaCols;

    public boolean isView() {
        return isView;
    }

    boolean isView;

    public ProjectedTableScan(Set<String> projectedCols, String tableName, boolean isView) {
        this.tableName = tableName;
        colDefs = Utils.nameToColDefs.get(tableName);
        this.isView = isView;
        setSchema(projectedCols);
        schemaCols = schema.keySet();
        tuple = new LinkedHashMap<String, PrimitiveValue>();
        isFirstCall = true;
        totalNumTuples = Utils.tableToLines.get(tableName);
        scannedTuples = 0;

    }

    @Override
    public Map<String, PrimitiveValue> next() {
        if (isFirstCall) {
            handleFirstCall();
            isFirstCall = false;
        }
        if (scannedTuples == totalNumTuples) {
            return null;
        }
        scannedTuples++;
        int colCounter = 0;
        int cachedCounter = 0;
        int onDiskCounter = 0;
        for (String tableColName : schemaCols) {
            PrimitiveValue primitiveValue;
            if (isCached[colCounter]) {
                primitiveValue = cachedTuplesIterators.get(cachedCounter).next();
                cachedCounter++;
            } else {
                ColumnDefinition columnDefinition = Utils.colToColDef.get(tableColName);
                String dataType = columnDefinition.getColDataType().getDataType();
                primitiveValue = readPrimitiveValueAndCache(dataType, dataInputStreams[onDiskCounter]);
                onDiskCounter++;
                if (isCachable[colCounter]) {
                    Utils.cachedCols.get(tableColName).add(primitiveValue);
                }
            }
            tuple.put(tableColName, primitiveValue);
            colCounter++;
        }

        return tuple;
    }

    private void handleFirstCall() {

        numCols = schemaCols.size();
        isCachable = new boolean[numCols];
        isCached = new boolean[numCols];
        int colCounter = 0;
        int cachedColCounter = 0;
        int diskColCounter = 0;
        for (String tableColName : schemaCols) {
            if (!isView && Utils.cachedCols.containsKey(tableColName)) {
                isCached[colCounter] = true;
                isCachable[colCounter] = false;
                cachedColCounter++;
            } else {
                isCached[colCounter] = false;
                if (!isView && Utils.isCachable(tableColName)) {
                    isCachable[colCounter] = true;
                    Utils.cachedCols.put(tableColName, new ArrayList<PrimitiveValue>());
                } else {
                    isCachable[colCounter] = false;
                }
                diskColCounter++;
            }
            colCounter++;
        }
        if (diskColCounter != 0) {
            dataInputStreams = new DataInputStream[diskColCounter];
            openDataInputStreams();
        }
        if (cachedColCounter != 0) {
            cachedTuplesIterators = new ArrayList<>();
            initCachedTupleIterators();
        }

    }

    private void initCachedTupleIterators() {
        int colCounter = 0;
        for (String tableColName : schemaCols) {
            if (isCached[colCounter]) {
                cachedTuplesIterators.add(Utils.cachedCols.get(tableColName).iterator());
            }
        }
    }




    private PrimitiveValue readPrimitiveValueAndCache(String dataType, DataInputStream dataInputStream) {
        try {
            if (dataType.equalsIgnoreCase("int")) {
                int val = getIntValueAndCacheBytes(dataInputStream);
                return new LongValue(val);

            } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
                double val = getDoubleValueAndCacheBytes(dataInputStream);
                return new DoubleValue(val);

            } else {
                String stringVal = getStringValue(dataInputStream);
                if (dataType.equalsIgnoreCase("date")) {
                    return new DateValue(stringVal);
                } else {
                    return new StringValue(stringVal);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getStringValue(DataInputStream dataInputStream) throws IOException {

        int length;
        byte[] byteArr = null;
        length = (int) dataInputStream.readShort();
        byteArr = new byte[length];
        dataInputStream.readFully(byteArr);
        return new String(byteArr);
    }

    private double getDoubleValueAndCacheBytes(DataInputStream dataInputStream) throws IOException {
        double val = dataInputStream.readDouble();
        return val;
    }


    private int getIntValueAndCacheBytes(DataInputStream dataInputStream) throws IOException {
        int val = dataInputStream.readInt();
        return val;
    }


    private void openDataInputStreams() {
        int colCounter = 0;
        int onDiskCounter = 0;
        for (String tableColName : schemaCols) {
            try {
                if (!isCached[colCounter]) {
                    File colFile = new File(Constants.COLUMN_STORE_DIR + "/" + tableName, tableColName);
                    FileInputStream fileInputStream = new FileInputStream(colFile);
                    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                    DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);
                    dataInputStreams[onDiskCounter] = dataInputStream;
                    onDiskCounter++;
                }
                colCounter++;

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }


        }
    }


    @Override
    public void init() {

    }

    @Override
    public Map<String, Integer> getSchema() {
        return schema;
    }

    private void setSchema(Set<String> projectedCols) {
        schema = new LinkedHashMap<String, Integer>();
        int colCounter = 0;
        for (ColumnDefinition colDef : colDefs) {
            String colName = colDef.getColumnName();
            String tableColName;
            if (isView) {
                tableColName = colName;
            } else {
                tableColName = this.tableName + "." + colName;
            }
            if (projectedCols == null) {
                schema.put(tableColName, colCounter);
            } else if ((projectedCols.contains(tableColName) || projectedCols.contains(colName))) {
                schema.put(tableColName, colCounter);
                colCounter++;
            }
        }
    }
}
