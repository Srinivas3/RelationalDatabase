package Indexes;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class StringIndex extends PrimaryIndex {
    private String primaryKeys[];

    public int getPosition(PrimitiveValue primitiveValue) {

            String key = primitiveValue.toString();
            int position = Arrays.binarySearch(primaryKeys,key);
            if(position<0) {
                position = (position * -1) - 1;
            }
            return position;

    }

    public StringIndex(Table table, String colName) {
        super(table, colName);
        primaryKeys = new String[numOfLines];
    }

    protected void insertInPrimaryKeys(int position, PrimitiveValue primitiveValue) {
        primaryKeys[position] = primitiveValue.toRawString();
    }

    protected void writePrimaryKeysToStream(DataOutputStream dataOutputStream) {
        for (int i = 0; i < numOfLines; i++) {
            try {
                dataOutputStream.writeInt(primaryKeys[i].length());
                dataOutputStream.write(primaryKeys[i].getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected void deserializePrimaryKeys(DataInputStream dataInputStream) {
        for (int i = 0; i < numOfLines; i++) {
            try {
                int strLen = dataInputStream.readInt();
                byte[] stringBytes = new byte[strLen];
                dataInputStream.readFully(stringBytes);
                primaryKeys[i] = new String(stringBytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
