package operators;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.Map;

public class UnionOperator implements Operator {
   private Operator leftOperator;
   private Operator rightOperator;
   public UnionOperator(Operator leftOperator, Operator rightOperator){
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
    }
    public Map<String, PrimitiveValue> next(){
        return null;
    }
    public void init(){

    }
}
