package aggregators;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;


public class SumAggregator implements Aggregator {
    PrimitiveValue accum;
    double db_accum = 0;
    long lo_accum = 0;
    String type = null;


    void SumAggregator(){
        init();
    }

    public void init() {
        accum = null;
        db_accum = 0;
        lo_accum = 0;
        type = null;
    }


    public void fold(PrimitiveValue next) {
             {
            PrimitiveType type = next.getType();
            if (type.equals(PrimitiveType.DOUBLE)){
                this.type = "double";
                try {
                    db_accum += next.toDouble();
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (type.equals(PrimitiveType.LONG)){
                this.type = "long";
                try {
                    lo_accum += next.toLong();

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    public PrimitiveValue getAggregate() {
        if (type.equalsIgnoreCase("double")){
            return  new DoubleValue(db_accum);
        } else {
            return  new LongValue(lo_accum);
        }
    }
}
