package Operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import schema.TableUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class JoinOperator extends Eval implements Operator{
    Operator leftChild;
    Operator rightChild;
    Join join;
    Map<String,PrimitiveValue> leftChildTuple;
    Map<String,PrimitiveValue> rightChildTuple;
    Map<String,PrimitiveValue> currTuple;
    public JoinOperator(Operator leftChild, Operator rightChild, Join join){
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.join = join;
        leftChildTuple = leftChild.next();
    }
    public PrimitiveValue eval(Column x){
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return TableUtils.getColValue(tableName,colName,this.currTuple);
    }
    public Map<String,PrimitiveValue> next(){
            if (leftChildTuple == null)
                return null;
            rightChildTuple = rightChild.next();
            if (rightChildTuple == null){
                leftChildTuple = leftChild.next();
                rightChild.init();
                return next();
            }
            Map<String,PrimitiveValue> joinTuple = merge(leftChildTuple,rightChildTuple);
            this.currTuple = joinTuple;
            if (isValid(joinTuple))
                return joinTuple;
            else
                return next();

    }
    public void init(){

    }
    private Map<String,PrimitiveValue> merge(Map<String,PrimitiveValue> leftTuple,Map<String,PrimitiveValue> rightTuple){
      Map<String,PrimitiveValue> mergedTuple = new LinkedHashMap<String,PrimitiveValue>();
        for (String key: leftTuple.keySet())
            mergedTuple.put(key,leftTuple.get(key));
        for (String key: rightTuple.keySet())
            mergedTuple.put(key,rightTuple.get(key));
        return mergedTuple;
    }
    private boolean isValid(Map<String,PrimitiveValue> joinTuple){
        if (join.isSimple()){
            return true;
        }
        return false;
    }


}
