package Indexes;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DoubleIndex extends PrimaryIndex {
    private double primaryKeys[];


    public DoubleIndex(Table table, String colName) {
        super(table,colName);
        primaryKeys = new double[numOfLines];
    }

    protected void insertInPrimaryKeys(int position, PrimitiveValue primitiveValue) {
        try {
            primaryKeys[position] = primitiveValue.toDouble();
        } catch (PrimitiveValue.InvalidPrimitive throwables) {
            throwables.printStackTrace();
        }
    }

    protected void writePrimaryKeysToStream(DataOutputStream dataOutputStream) {
        for(int i = 0;i<numOfLines;i++){
            try {
                dataOutputStream.writeDouble(primaryKeys[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    protected void deserializePrimaryKeys(DataInputStream dataInputStream) {
        for (int i = 0; i < numOfLines; i++) {
            try {
                primaryKeys[i] = dataInputStream.readDouble();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
