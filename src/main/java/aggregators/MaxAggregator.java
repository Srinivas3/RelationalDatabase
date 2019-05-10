package aggregators;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.PrimitiveType;

public class MaxAggregator implements Aggregator {

    double db_accum = Long.MIN_VALUE;
    long lo_accum = Long.MIN_VALUE;
    String st_accum = null;
    String type = null;

    void MaxAggregator(){
        init();
    }

    public void init() {
        db_accum = Long.MIN_VALUE;
        lo_accum = Long.MIN_VALUE;
        st_accum = null;
        type = null;
    }

    public void fold(PrimitiveValue next) {
         {
            PrimitiveType type = next.getType();
            if (type.equals(PrimitiveType.DOUBLE)){
                this.type = "double";
                try {
                    double next_double = next.toDouble();
                    this.db_accum = this.db_accum > next_double ? this.db_accum : next_double;

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            } else if (type.equals(PrimitiveType.LONG)){
                this.type = "long";
                try {
                    long next_long = next.toLong();
                    this.lo_accum = this.lo_accum > next_long ? this.lo_accum : next_long;

                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }else {
                if (type.equals(PrimitiveType.DATE)){
                    this.type = "date";
                }
                else{
                    this.type = "string";
                }

                if (st_accum == null){
                    st_accum = next.toRawString();
                } else {
                    String next_string = next.toRawString();
                    st_accum = st_accum.compareTo(next_string) > 0 ? st_accum : next_string;
                }

            }

        }
    }
    public PrimitiveValue getAggregate() {
        if (type.equalsIgnoreCase("double")){
            return  new DoubleValue(db_accum);
        } else if (type.equalsIgnoreCase("long")){
            return  new LongValue(lo_accum);
        } else {
            if (type.equalsIgnoreCase("date")){
                return new DateValue(st_accum);
            }
            return  new StringValue(st_accum);
        }
    }
}