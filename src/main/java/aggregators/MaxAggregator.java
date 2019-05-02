package aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class MaxAggregator implements Aggregator {

    PrimitiveValue accum;

    void MaxAggregator(){
        init();
    }

    public void init() {
        accum = null;
    }

    public void fold(PrimitiveValue next) {
        if (accum == null){
            accum = next;

        } else {
            PrimitiveType type = accum.getType();
            if (type.equals(PrimitiveType.DOUBLE)){
                try {
                    double a = accum.toDouble() > next.toDouble() ? accum.toDouble() : next.toDouble();
                    accum = new DoubleValue(a);

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (type.equals(PrimitiveType.LONG)){
                try {
                    long a = accum.toLong() > next.toLong() ? accum.toLong() : next.toLong();
                    accum = new LongValue(a);

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }else {
                String a = accum.toRawString().compareTo(next.toRawString()) > 0 ? accum.toRawString() : next.toRawString();
                accum = new StringValue(a);
            }

        }
    }
    public PrimitiveValue getAggregate() {
        return accum;
    }
}