package aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

public class AverageAggregator implements Aggregator {
//    PrimitiveValue sum_accum, count_accum;
    long lsum_accum = 0;
    double dsum_accum = 0;
    long count = 0;
    String type = null;

    void SumAggregator(){
        init();
    }

    public void init() {
        long lsum_accum = 0;
        double dsum_accum = 0;
        long count = 0;
        String type = null;
    }


    public void fold(PrimitiveValue next) {
         {
             PrimitiveType type = next.getType();
            if (type.equals(PrimitiveType.DOUBLE)){
                this.type = "double";
                try {
                    dsum_accum += + next.toDouble();
                    count++;

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (type.equals(PrimitiveType.LONG)){
                this.type = "long";
                try {
                    lsum_accum += next.toLong();
                    count++;
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }

        }

    }

    public PrimitiveValue getAggregate() {

            if (this.type.equalsIgnoreCase("double")){
                return new DoubleValue(dsum_accum/count);
            } else {
                return new DoubleValue(lsum_accum* 1.0/count);
            }

    }
}
