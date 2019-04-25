package Indexes;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import operators.TableScan;
import utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class IntegerIndex extends PrimaryIndex {

    private int primaryKeys[];

    public int getPosition(PrimitiveValue primitiveValue) {
        try {
            int key = (int) primitiveValue.toLong();
            int position = Arrays.binarySearch(primaryKeys,key);
            if(position<0) {
                position = (position * -1) - 1;
            }
            return position;
        } catch (PrimitiveValue.InvalidPrimitive throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }


    public IntegerIndex(Table table, String colName) {
        super(table,colName);
        primaryKeys = new int[numOfLines];
    }

    protected void insertInPrimaryKeys(int position, PrimitiveValue primitiveValue) {
        try {
            primaryKeys[position] = (int) primitiveValue.toLong();
        } catch (PrimitiveValue.InvalidPrimitive throwables) {
            throwables.printStackTrace();
        }
    }

    protected void writePrimaryKeysToStream(DataOutputStream dataOutputStream) {
        for(int i = 0;i<numOfLines;i++){
            try {
                dataOutputStream.writeInt(primaryKeys[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected void deserializePrimaryKeys(DataInputStream dataInputStream) {
        for (int i = 0; i < numOfLines; i++) {
            try {
                primaryKeys[i] = dataInputStream.readInt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
