package aggregators;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class CountAggregator implements Aggregator {
    PrimitiveValue accum;

    void CountAggregator(){
        init();
    }

    public void init() {
        accum = null;
    }


    public void fold(PrimitiveValue next) {
        if(accum == null && next!=null){
            accum = new LongValue(1);
        }
        else if(next != null){
            try {
                Long a = accum.toLong()+1;
                accum = new LongValue(a);
            } catch (PrimitiveValue.InvalidPrimitive throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public PrimitiveValue getAggregate() {
        return accum;
    }
}
