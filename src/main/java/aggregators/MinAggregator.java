package aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class MinAggregator implements AggregatePattern{

    PrimitiveValue accum;

    void MinAggregator(){
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
                    double a = accum.toDouble() < next.toDouble() ? accum.toDouble() : next.toDouble();
                    accum = new DoubleValue(a);

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (type.equals(PrimitiveType.LONG)){
                try {
                    long a = accum.toLong() < next.toLong() ? accum.toLong() : next.toLong();
                    accum = new LongValue(a);

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }

        }
    }
    public PrimitiveValue getAggregate() {
        return accum;
    }
}