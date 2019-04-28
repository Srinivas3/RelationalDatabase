package preCompute;

import Indexes.PrimaryIndex;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import utils.Constants;
import utils.PrimValComp;
import utils.Utils;
import Indexes.IndexFactory;


import java.io.*;
import java.util.*;

public class PreProcessor {
    private IndexFactory indexFactory = new IndexFactory();

    CreateTable createTableStatement;

    public PreProcessor(CreateTable createTableStatement) {
        this.createTableStatement = createTableStatement;
    }

    public void preCompute() {
        populateIndexTypes();
        populateTableSchema();
        saveColDefsToDisk();
        scanTableOperations();
        //saveSecondaryIndex();
        populateTupleCount();
        //buildPrimaryIndexAndSave();

    }


    private void populateTupleCount() {
        String tableName = createTableStatement.getTable().getName();
        File file = new File(Constants.LINES_DIR, tableName);
        try {
            DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file));
            dataOutputStream.writeInt(Utils.tableToLines.get(tableName));
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveColDefsToDisk() {
        List<ColumnDefinition> columnDefinitions = createTableStatement.getColumnDefinitions();
        String tableName = createTableStatement.getTable().getName();
        File colDefsFile = new File(Constants.COL_DEFS_DIR, tableName);
        try {
            FileWriter fileWriter = new FileWriter(colDefsFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (ColumnDefinition colDef : columnDefinitions) {
                String colName = colDef.getColumnName();
                String colDataType = colDef.getColDataType().getDataType();
                bufferedWriter.write(colName + "," + colDataType);
                bufferedWriter.newLine();
                String tableColName = tableName + "." + colName;
                Utils.colToColDef.put(tableColName,colDef);
            }
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void populateTableSchema() {
        String tableName = createTableStatement.getTable().getName();
        List<ColumnDefinition> colDefs = createTableStatement.getColumnDefinitions();
        Utils.nameToColDefs.put(tableName, colDefs);
    }

    private void populateIndexTypes() {
        List<Index> indexes = createTableStatement.getIndexes();
        if (indexes == null) {
            return;
        }
        for (Index index : indexes) {
            String tableColName = createTableStatement.getTable().getName() + "." + index.getColumnsNames().get(0);
            if (createTableStatement.getTable().getName().equalsIgnoreCase("lineitem")) {
                if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                    Utils.colToIndexType.put(tableColName, index.getType());
                }
            } else {
                Utils.colToIndexType.put(tableColName, index.getType());
            }

        }
    }

    private void buildPrimaryIndexAndSave() {
        Table table = createTableStatement.getTable();
        List<Index> indexes = createTableStatement.getIndexes();
        if (indexes == null || indexes.isEmpty()) {
            return;
        }
        for (Index index : indexes) {
            if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                String colName = index.getColumnsNames().get(0);
                PrimaryIndex primaryIndex = indexFactory.getIndex(table, colName);
                String keyFileName = table.getName() + "." + colName;
                Utils.colToPrimIndex.put(keyFileName, primaryIndex);
                writeIndexToFile(keyFileName, primaryIndex);
            }
        }
    }

    private void writeIndexToFile(String keyFileName, PrimaryIndex primaryIndex) {
        File indexFile = new File(Constants.PRIMARY_INDICES_DIR, keyFileName);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(indexFile);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
            primaryIndex.serializeToStream(dataOutputStream);
            dataOutputStream.flush();
            dataOutputStream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void scanTableOperations() {
        Table table = createTableStatement.getTable();
        String tableName = table.getName();
        File tableFile = new File("data", tableName + Constants.FORMAT);
        File compressedTableFile = new File(Constants.COMPRESSED_TABLES_DIR, tableName);
        List<ColumnDefinition> colDefs = Utils.nameToColDefs.get(tableName);
        DataOutputStream[] dataOutputStreams = getDataOutputStreams(colDefs, tableName);
        Long[] colBytesCnts = initializeColBytesCnts(colDefs.size());
        int numOfLines = 0;

        try {
            int bytesWrittenSoFar = 0;
            int tupleBytesWrittenSoFar = 0;
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(compressedTableFile));
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(tableFile));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String tupleArr[] = line.split("\\|");
                for (int i = 0; i < tupleArr.length; i++) {
                    ColumnDefinition colDef = colDefs.get(i);
                    String tableColName = tableName + "." + colDef.getColumnName();
                    String indexType = Utils.colToIndexType.get(tableColName);
                    if (!tableName.equalsIgnoreCase("lineitem") && indexType != null && indexType.equalsIgnoreCase("index")) {
                      //  insertFilePosition(colDef, tableColName, tupleArr[i], bytesWrittenSoFar);
                    }
                    tupleBytesWrittenSoFar += writeBytes(dataOutputStream, tupleArr[i], colDef);
                    colBytesCnts[i] += writeBytes(dataOutputStreams[i], tupleArr[i], colDef);
                }
                bytesWrittenSoFar += tupleBytesWrittenSoFar;
                tupleBytesWrittenSoFar = 0;
                numOfLines++;

            }
            saveColBytesCnts(colBytesCnts, colDefs,tableName);
            Utils.tableToLines.put(tableName, numOfLines);
            bufferedReader.close();
            flushAndClose(dataOutputStream);
            flushAndClose(dataOutputStreams);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveColBytesCnts(Long[] colBytesCnts, List<ColumnDefinition> colDefs,String tableName) {
        for (int i = 0; i < colBytesCnts.length; i++) {
            String colName = colDefs.get(i).getColumnName();
            String tableColName = tableName+"." + colName;
            Utils.colToByteCnt.put(tableColName, colBytesCnts[i]);
            try {

                DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(new File(Constants.COLUMN_BYTES_DIR, tableColName)));
                dataOutputStream.writeLong(colBytesCnts[i]);
                flushAndClose(dataOutputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    private void flushAndClose(DataOutputStream dataOutputStream) {
        try {
            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void flushAndClose(DataOutputStream[] dataOutputStreams) {
        for (DataOutputStream dataOutputStream : dataOutputStreams) {
            flushAndClose(dataOutputStream);
        }
    }

    private Long[] initializeColBytesCnts(int numOfCols) {
        Long[] colBytesCnts = new Long[numOfCols];
        for (int i = 0; i < colBytesCnts.length; i++) {
            colBytesCnts[i] = new Long(0);
        }
        return colBytesCnts;
    }

    private DataOutputStream[] getDataOutputStreams(List<ColumnDefinition> colDefs, String tableName) {
        String tableColDir = Constants.COLUMN_STORE_DIR + "/" + tableName;
        new File(tableColDir).mkdir();
        DataOutputStream[] dataOutputStreams = new DataOutputStream[colDefs.size()];
        int i = 0;
        for (ColumnDefinition columnDefinition : colDefs) {
            String colName = columnDefinition.getColumnName();
            File columnFile = new File(tableColDir, tableName + "." +colName);
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(columnFile);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                dataOutputStreams[i] = new DataOutputStream(bufferedOutputStream);
                i++;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
        return dataOutputStreams;
    }

    private void insertFilePosition(ColumnDefinition colDef, String tableColName, String primValInString, int filePosition) {
        PrimitiveValue primitiveValue = Utils.getPrimitiveValue(colDef.getColDataType().getDataType(), primValInString);
        TreeMap<PrimitiveValue, List<Integer>> secIndex = Utils.colToSecIndex.get(tableColName);
        if (secIndex == null) {
            secIndex = new TreeMap<PrimitiveValue, List<Integer>>(new PrimValComp());
            Utils.colToSecIndex.put(tableColName, secIndex);
        }
        List<Integer> filePositions = secIndex.get(primitiveValue);
        if (filePositions == null) {
            filePositions = new ArrayList<Integer>();
            secIndex.put(primitiveValue, filePositions);
        }
        filePositions.add(filePosition);
    }


    private int writeBytes(DataOutputStream dataOutputStream, String colInString, ColumnDefinition columnDefinition) {
        String dataType = columnDefinition.getColDataType().getDataType().toLowerCase();
        try {
            if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("date")) {
                dataOutputStream.writeShort((short)colInString.length());
                dataOutputStream.write(colInString.getBytes());
                return 2 + colInString.length();
            } else if (dataType.equalsIgnoreCase("int")) {
                dataOutputStream.writeInt(Integer.valueOf(colInString));
                return 4;
            } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
                dataOutputStream.writeDouble(Double.valueOf(colInString));
                return 8;
            } else {
                throw new Exception("invalid data type " + dataType);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    private void saveSecondaryIndex() {
        Set<String> tableColNames = Utils.colToIndexType.keySet();
        for (String tableColName : tableColNames) {
            if (Utils.isSameTable(createTableStatement.getTable().getName(), tableColName)) {
                if (Utils.colToIndexType.get(tableColName).equalsIgnoreCase("index")) {
                    writeSecondaryIndexToFile(tableColName);
                }
            }

        }

    }

    private void writeSecondaryIndexToFile(String tableColName) {
        File secIndexFile = new File(Constants.SECONDARY_INDICES_DIR, tableColName);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(secIndexFile);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
            TreeMap<PrimitiveValue, List<Integer>> secIdx = Utils.colToSecIndex.get(tableColName);
            Iterator<PrimitiveValue> primValIterator = secIdx.keySet().iterator();
            while (primValIterator.hasNext()) {
                PrimitiveValue primitiveValue = primValIterator.next();
                writeBytes(dataOutputStream, primitiveValue);
                List<Integer> positions = secIdx.get(primitiveValue);
                dataOutputStream.writeInt(positions.size());
                for (int position : positions) {
                    dataOutputStream.writeInt(position);
                }
            }
            dataOutputStream.flush();
            dataOutputStream.close();
            secIdx.clear();
            Utils.colToSecIndex.remove(tableColName);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void writeBytes(DataOutputStream dataOutputStream, PrimitiveValue primitiveValue) {
        try {
            if (primitiveValue instanceof DoubleValue) {
                dataOutputStream.writeDouble(primitiveValue.toDouble());
            } else if (primitiveValue instanceof LongValue) {
                dataOutputStream.writeInt((int) primitiveValue.toLong());
            } else {
                String primValInString = primitiveValue.toRawString();
                dataOutputStream.writeShort((short)primValInString.length());
                dataOutputStream.writeBytes(primValInString);
            }
        } catch (Exception e) {

        }
    }
}
