package operators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.insert.Insert;
import preCompute.PreProcessor;
import utils.Utils;

import java.io.DataOutputStream;
import java.util.*;

public class InsertOperator implements Operator {

    private Table table;
    private List<Map<String, PrimitiveValue>> insertedTuples;
    private Iterator<Map<String, PrimitiveValue>> tupleIterator=null;
    Map<String,PrimitiveValue> tuple;


    public InsertOperator(Table table){
        this.table = table;
        this.insertedTuples = Utils.tableToInserts.get(table.getName());
        if(insertedTuples!=null){
            this.tupleIterator = insertedTuples.iterator();
        }

    }

    @Override
    public Map<String, PrimitiveValue> next() {
        if(tupleIterator == null) return null;
        if(tupleIterator.hasNext()){
            return tupleIterator.next();
        }
        else {
            return null;
        }
    }

    @Override
    public void init() {

    }

    @Override
    public Map<String, Integer> getSchema() {
        return null;
    }
}
