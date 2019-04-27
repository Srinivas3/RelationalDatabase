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
    List<DataInputStream> dataInputStreams;
//    Map<String, ByteArrayOutputStream> localCachedCols;
    //Map<String, DataOutputStream> localCachedColsOutputStr;
    int totalNumTuples;
    int scannedTuples;
    Set<String> schemaCols;


    public ProjectedTableScan(Set<String> projectedCols, String tableName) {
        this.tableName = tableName;
        colDefs = Utils.nameToColDefs.get(tableName);
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
            //mergeLocalCachedCols();
            return null;
        }
        scannedTuples++;
        Iterator<DataInputStream> dataInputStreamIterator = dataInputStreams.iterator();
        for (String tableColName : schemaCols) {
            DataInputStream dataInputStream = dataInputStreamIterator.next();
            ColumnDefinition columnDefinition = Utils.colToColDef.get(tableColName);
            PrimitiveValue primitiveValue = readPrimitiveValueAndCache(columnDefinition.getColDataType().getDataType(), dataInputStream, tableColName);
            tuple.put(tableColName, primitiveValue);
        }


        return tuple;
    }

    private PrimitiveValue readPrimitiveValueAndCache(String dataType, DataInputStream dataInputStream, String tableColName) {
       // DataOutputStream dataOutputStream = localCachedColsOutputStr.get(tableColName);
        try {
            if (dataType.equalsIgnoreCase("int")) {
                int val = dataInputStream.readInt();
         /*
                if (dataOutputStream != null) {
                    dataOutputStream.writeInt(val);
                }
                */
                return new LongValue(val);

            } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
                double val = dataInputStream.readDouble();
//                if (dataOutputStream != null) {
//                    dataOutputStream.writeDouble(val);
//                }
                return new DoubleValue(val);
            } else {
                int length = dataInputStream.readInt();
                byte[] byteBuffer = new byte[length];
                dataInputStream.readFully(byteBuffer);
//                if (dataOutputStream != null) {
//                    dataOutputStream.writeInt(length);
//                    dataOutputStream.write(byteBuffer);
//                }
                if (dataType.equalsIgnoreCase("date")) {
                    return new DateValue(new String(byteBuffer));
                } else {
                    return new StringValue(new String(byteBuffer));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }




//    private void mergeLocalCachedCols() {
//        Set<String> colNames = localCachedCols.keySet();
//        for (String colName : colNames) {
//            try {
//               DataOutputStream dataOutputStream = localCachedColsOutputStr.get(colName);
//                dataOutputStream.flush();
//                Utils.cachedCols.put(colName, localCachedCols.get(colName).toByteArray());
//                dataOutputStream.close();
//            }
//            catch (Exception e){
//                e.printStackTrace();
//            }
//
//        }
//        localCachedCols = null;
//    }

    private void openDataInputStreams() {
        dataInputStreams = new ArrayList<DataInputStream>();
//        localCachedCols = new HashMap<String, ByteArrayOutputStream>();
//        localCachedColsOutputStr = new HashMap<String,DataOutputStream>();
        Set<String> colsInSchema = schema.keySet();
        for (String tableColName : colsInSchema) {
            try {
                DataInputStream dataInputStream = null;
                if (!Utils.cachedCols.containsKey(tableColName)) {
                    File colFile = new File(Constants.COLUMN_STORE_DIR + "/" + tableName,tableColName);

                    FileInputStream fileInputStream = new FileInputStream(colFile);
                    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                    dataInputStream = new DataInputStream(bufferedInputStream);
//                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(Utils.colToByteCnt.get(tableColName).intValue());
                    //localCachedCols.put(tableColName, byteArrayOutputStream);
//                    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
                    //localCachedColsOutputStr.put(tableColName,dataOutputStream);

                } else {
                    byte[] cachedBytes = Utils.cachedCols.get(tableColName);
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(cachedBytes);
                    dataInputStream = new DataInputStream(byteArrayInputStream);
                }
                dataInputStreams.add(dataInputStream);
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
            String tableColName = this.tableName + "." + colName;
            if (projectedCols.contains(tableColName) || projectedCols.contains(colName)) {
                schema.put(tableColName, colCounter);
                colCounter++;
            }
        }
    }
}
