package aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class AverageAggregator implements AggregatePattern {
    PrimitiveValue sum_accum, count_accum;

    void SumAggregator(){
        init();
    }

    public void init() {
        sum_accum = null;
        count_accum = null;
    }


    public void fold(PrimitiveValue next) {
        if (sum_accum == null){
            sum_accum = next;
            count_accum = new LongValue(1);
        } else {
            PrimitiveType type = sum_accum.getType();
            if (type.equals(PrimitiveType.DOUBLE)){
                try {
                    double a = sum_accum.toDouble() + next.toDouble();
                    sum_accum = new DoubleValue(a);
                    Long b = count_accum.toLong()+1;
                    count_accum = new LongValue(b);

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (type.equals(PrimitiveType.LONG)){
                try {
                    long a = sum_accum.toLong() + next.toLong();
                    sum_accum = new LongValue(a);
                    Long b = count_accum.toLong()+1;
                    count_accum = new LongValue(b);

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }

        }

    }

    public PrimitiveValue getAggregate() {
        double a=0;
        try {
            a = sum_accum.toDouble()/count_accum.toDouble();

        } catch (PrimitiveValue.InvalidPrimitive throwables) {
            throwables.printStackTrace();
        }
        return new DoubleValue(a);
    }
}
