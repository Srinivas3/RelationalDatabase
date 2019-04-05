package utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;


public class TupleSerializer {

    boolean isFirstCall = true;
    PrimitiveType[] primitiveTypes;
    String delimiter = ":;:";

    public String serialize(List<PrimitiveValue> tuple) {
        if (isFirstCall) {
            isFirstCall = false;
            primitiveTypes = new PrimitiveType[tuple.size()];
            int counter = 0;
            for (PrimitiveValue tupleObject : tuple) {
                primitiveTypes[counter] = tupleObject.getType();
                counter++;
            }
        }
        StringJoiner sj = new StringJoiner(delimiter);
        for (PrimitiveValue tupleObject : tuple) {
            sj.add(tupleObject.toRawString());
        }
        return sj.toString();

    }

    public List<PrimitiveValue> deserialize(String obj) {
        List<PrimitiveValue> primitiveValues = new ArrayList<PrimitiveValue>();
        int counter = 0;
        String[] splittedBoject = obj.split(delimiter);
        for (String ob : splittedBoject) {
            try {
                PrimitiveType primitiveType = primitiveTypes[counter];
                if (primitiveType.equals(PrimitiveType.DOUBLE)) {
                    primitiveValues.add(new DoubleValue(ob));
                } else if (primitiveType.equals(PrimitiveType.LONG)) {
                    primitiveValues.add(new LongValue(ob));
                } else if (primitiveType.equals(PrimitiveType.DATE)) {
                    primitiveValues.add(new DateValue(ob));
                } else {
                    primitiveValues.add(new StringValue(ob));
                }
                counter++;
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
                System.out.println(e);
            }
        }
        return primitiveValues;
    }

}
