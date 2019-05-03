package operators;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Constants;
import utils.Utils;

import java.io.*;
import java.nio.ByteBuffer;
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
    Map<String, DataInputStream> colTodataInputStream;
    Map<String, ByteBuffer> cachedColToByteBuffer;
    Map<String, ByteBuffer> localCachedCols;
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
            openDataInputStreams();
            isFirstCall = false;
        }
        if (scannedTuples == totalNumTuples) {
            return null;
        }
        scannedTuples++;

        for (String tableColName : schemaCols) {
            ColumnDefinition columnDefinition = Utils.colToColDef.get(tableColName);
            String dataType = columnDefinition.getColDataType().getDataType();
            PrimitiveValue primitiveValue = readPrimitiveValueAndCache(dataType, tableColName);
            tuple.put(tableColName, primitiveValue);
        }

        return tuple;
    }

    private PrimitiveValue readPrimitiveValueAndCache(String dataType, String tableColName) {
        try {
            if (dataType.equalsIgnoreCase("int")) {
                int val = getIntValueAndCacheBytes(tableColName);
                return new LongValue(val);

            } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
                double val = getDoubleValueAndCacheBytes(tableColName);
                return new DoubleValue(val);

            } else {
                String stringVal = getStringValue(tableColName);
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

    private String getStringValue(String tableColName) throws IOException {

        int length;
        byte[] byteArr = null;
        DataInputStream dataInputStream = colTodataInputStream.get(tableColName);
        length = (int) dataInputStream.readShort();
        byteArr = new byte[length];
        dataInputStream.readFully(byteArr);
        return new String(byteArr);
    }

    private double getDoubleValueAndCacheBytes(String tableColName) throws IOException {

        double val = colTodataInputStream.get(tableColName).readDouble();
        return val;
    }


    private int getIntValueAndCacheBytes(String tableColName) throws IOException {

        int val = colTodataInputStream.get(tableColName).readInt();;
        return val;
    }


    private void openDataInputStreams() {
        colTodataInputStream = new HashMap<String, DataInputStream>();
        localCachedCols = new HashMap<String, ByteBuffer>();
        cachedColToByteBuffer = new HashMap<String, ByteBuffer>();
        Set<String> colsInSchema = schema.keySet();
        for (String tableColName : colsInSchema) {
            try {
                DataInputStream dataInputStream = null;
                if (!Utils.cachedCols.containsKey(tableColName)) {
                    File colFile = new File(Constants.COLUMN_STORE_DIR + "/" + tableName, tableColName);
                    FileInputStream fileInputStream = new FileInputStream(colFile);
                    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                    dataInputStream = new DataInputStream(bufferedInputStream);
                    colTodataInputStream.put(tableColName, dataInputStream);
                    if (Utils.isCachable(tableColName)) {
                        int colByteCnt = Utils.colToByteCnt.get(tableColName).intValue();
                        byte[] colCache = new byte[colByteCnt];
                        ByteBuffer byteBuffer = ByteBuffer.wrap(colCache);
                        localCachedCols.put(tableColName, byteBuffer);
                    }

                } else {
                    byte[] cachedBytes = Utils.cachedCols.get(tableColName);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(cachedBytes);
                    cachedColToByteBuffer.put(tableColName, byteBuffer);
                }
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
