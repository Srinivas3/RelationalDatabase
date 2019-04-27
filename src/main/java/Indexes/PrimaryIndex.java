package Indexes;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import operators.TableScan;
import utils.Utils;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class PrimaryIndex {
    public abstract int getPosition(PrimitiveValue primitiveValue);


    protected int positions[];
    protected int numOfLines;
    protected Table table;
    protected String colName;

    public int[] getPositions() {
        return positions;
    }

    public PrimaryIndex(Table table, String colName) {
        this.table = table;
        this.colName = colName;
        numOfLines = Utils.tableToLines.get(table.getName());
        positions = new int[numOfLines];
    }

    void setPrimaryKeyPositions() {
        TableScan tableScan = new TableScan(table);
        for (int i = 0; i < numOfLines; i++) {
            positions[i] = tableScan.getBytesReadSoFar();
            Map<String, PrimitiveValue> tuple = tableScan.next();
            PrimitiveValue primaryKeyPrimVal = Utils.getColValue(table.getName(), colName, tuple);
            insertInPrimaryKeys(i, primaryKeyPrimVal);
        }
    }

    public String getColName() {
        return colName;
    }

    public Table getTable() {
        return table;
    }

    protected abstract void insertInPrimaryKeys(int position, PrimitiveValue primaryKeyPrimVal);

    public void serializeToStream(DataOutputStream dataOutputStream) {
        for (int i = 0; i < numOfLines; i++) {
            try {
                dataOutputStream.writeInt(positions[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        writePrimaryKeysToStream(dataOutputStream);
    }

    protected abstract void writePrimaryKeysToStream(DataOutputStream dataOutputStream);

    public void deserializeFromFile(File indexFile) {
        try {

            FileInputStream fileInputStream = new FileInputStream(indexFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);
            for (int i = 0; i < numOfLines; i++) {
                positions[i] = dataInputStream.readInt();
            }
            deserializePrimaryKeys(dataInputStream);
            dataInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void deserializePrimaryKeys(DataInputStream dataInputStream);

    protected int getPosition(PrimitiveValue primitiveValue, Object keysObj) {
        int position;
        Object key = null;
        if (keysObj instanceof int[]) {
            try {
                key = new Integer((int) primitiveValue.toLong());
            } catch (PrimitiveValue.InvalidPrimitive throwables) {
                throwables.printStackTrace();
            }
            position = Arrays.binarySearch((int[]) keysObj, (Integer) key);
        } else if (keysObj instanceof double[]) {
            try {
                key = primitiveValue.toDouble();
            } catch (PrimitiveValue.InvalidPrimitive throwables) {
                throwables.printStackTrace();
            }
            position = Arrays.binarySearch((double[]) keysObj, (Double) key);
        } else {
            key = primitiveValue.toRawString();
            position = Arrays.binarySearch((String[]) keysObj, key);
        }
        if (position < 0) {
            position = (position * -1) - 1;
        } else {
            position = getPosition(keysObj, 0, position, key, position);
        }
        return position;
    }

    protected int getPosition(Object keysObj, int fromPos, int toPos, Object searchKey, int currPos) {
        if (fromPos > toPos){
            return currPos;
        }
        int position;
        if (keysObj instanceof int[]) {
            position = Arrays.binarySearch((int[]) keysObj, fromPos, toPos, (Integer) searchKey);
        } else if (keysObj instanceof double[]) {
            position = Arrays.binarySearch((double[]) keysObj, fromPos, toPos, (Double) searchKey);
        } else {
            position = Arrays.binarySearch((String[]) keysObj, fromPos, toPos, (String) searchKey);
        }

        if (position < 0) {
            return currPos;
        } else {
            return getPosition(keysObj, fromPos, position, searchKey, position);
        }
    }

}
