package CustomPrimitiveTypes;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.sql.Date;

public class EfficientDateValue implements PrimitiveValue{
    short year;
    byte month;
    byte day;
    public EfficientDateValue(short year, byte month,byte day) {
        this.year = year;
        this.month = month;
        this.day = day;
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

    @Override
    public long toLong() throws InvalidPrimitive {
        return 0;
    }

    @Override
    public double toDouble() throws InvalidPrimitive {
        return 0;
    }

    @Override
    public boolean toBool() throws InvalidPrimitive {
        return false;
    }

    @Override
    public String toRawString() {
        return null;
    }

    @Override
    public PrimitiveType getType() {
        return PrimitiveType.DATE;
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {

    }
}
