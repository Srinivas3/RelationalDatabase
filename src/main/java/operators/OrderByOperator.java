package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import schema.TableUtils;

import java.sql.SQLException;
import java.util.*;

public class OrderByOperator implements Operator {
    Operator child;
    List<OrderByElement> orderByElements;
    List<Map<String,PrimitiveValue>> filteredTuples;
    int counter = -1;
    Map<String,PrimitiveValue> childTuple;

    public OrderByOperator(List<OrderByElement> orderByElements, Operator operator) {

        this.orderByElements = orderByElements;
        this.child = operator;
    }



    public Map<String, PrimitiveValue> next() {

        if (filteredTuples == null){
            filteredTuples = new ArrayList<Map<String, PrimitiveValue>>();
            childTuple = child.next();
            while (childTuple!=null) {
                Map<String,PrimitiveValue> obj = new LinkedHashMap<String, PrimitiveValue>();
                for(String key : childTuple.keySet()){
                    obj.put(key,childTuple.get(key));
                }
                filteredTuples.add(obj);
                childTuple= child.next();

            }
            Collections.sort(filteredTuples, new CompareTuples(orderByElements));

        }

        if(++counter < filteredTuples.size()){
            return filteredTuples.get(counter);

        }
        else return null;
    }

    public void init() {

    }

}

class CompareTuples extends Eval implements Comparator<Map<String,PrimitiveValue>>{

    List<OrderByElement> orderByElements;
    Map<String,PrimitiveValue> currTuple;
    public CompareTuples(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }


    public int compare(Map<String, PrimitiveValue> o1, Map<String, PrimitiveValue> o2) {

        for(OrderByElement element : orderByElements){
            Expression expression = element.getExpression();
            boolean isAsc = element.isAsc();
            PrimitiveValue value1 = evaluate(expression,o1);
            PrimitiveValue value2 = evaluate(expression,o2);
            if(value1.getType().equals(PrimitiveType.DOUBLE)){
                try {
                    double val1 = value1.toDouble();
                    double val2 = value2.toDouble();
                    if(val1<val2){ return isAsc ? -1 : 1;}
                    else if(val1 > val2){ return isAsc ? 1 : -1;}
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }
            else if(value1.getType().equals(PrimitiveType.LONG)){
                try {
                    long val1 = value1.toLong();
                    long val2 = value2.toLong();
                    if(val1<val2){ return isAsc ? -1 : 1;}
                    else if(val1 > val2){ return isAsc ? 1 : -1;}
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }
            else if(value1.getType().equals(PrimitiveType.STRING)){
                    String val1 = value1.toRawString();
                    String val2 = value2.toRawString();
                    int compare = val1.compareTo(val2);
                    if(compare==0)continue;
                    if(isAsc) return compare;
                    else return (-1 * compare);

            }

        }
        return 0;
    }

    public PrimitiveValue evaluate(Expression expression, Map<String,PrimitiveValue> tuple){
        currTuple = tuple;
        PrimitiveValue value=null;
        try {
            value = eval(expression);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return value;
    }

    public PrimitiveValue eval(Column x){

        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return TableUtils.getColValue(tableName, colName, currTuple);
    }
}
