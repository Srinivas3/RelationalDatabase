package CustomPrimitiveTypes;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.sql.Date;

public class EfficientDateValue extends DateValue{
    short year;
    byte month;
    byte day;
    public EfficientDateValue(String value) {
        super(value);
        year = (short)super.getYear();
        month = (byte)super.getMonth();
        day  = (byte)super.getDate();
    }

    public int getDate() {
        return day;
    }
    public int getMonth(){
        return month;
    }
    public int getYear(){
        return year;
    }

}
