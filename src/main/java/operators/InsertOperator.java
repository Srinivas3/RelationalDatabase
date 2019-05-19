package operators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.insert.Insert;
import preCompute.PreProcessor;
import utils.Utils;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertOperator implements Operator {

    Map<String,PrimitiveValue> tuple;
    boolean isFirstCall;
    Insert insertStatement;

    public InsertOperator(Insert insertStatement){
        this.insertStatement = insertStatement;
        this.tuple = new HashMap<String,PrimitiveValue>();
        handleInsert(insertStatement);
        isFirstCall = true;
    }


    private void handleInsert(Insert insertStatement) {
        String tableName = insertStatement.getTable().getName();
        PreProcessor preProcessor = new PreProcessor();
        List<Column> columns = insertStatement.getColumns();
        List<String> tableColNames = new ArrayList<String>();
        for(Column column : columns){
            String tableColName = tableName +"."+ column.getColumnName();
            tableColNames.add(tableColName);
        }
        ItemsList itemsList = insertStatement.getItemsList();
        List<Expression> expressions = null;
        if(itemsList instanceof ExpressionList){
            ExpressionList expressionList = (ExpressionList) itemsList;
            expressions = expressionList.getExpressions();
        }
        int i=0;
        for(String tableColName : tableColNames){
            PrimitiveValue primitiveValue = (PrimitiveValue)expressions.get(i);
            tuple.put(tableColName,primitiveValue);
            i++;
        }

    }

    public Insert getInsertStatement() {
        return insertStatement;
    }

    @Override
    public Map<String, PrimitiveValue> next() {
        if(isFirstCall){
            isFirstCall = false;
            return tuple;
        }
        else return null;
    }

    @Override
    public void init() {

    }

    @Override
    public Map<String, Integer> getSchema() {
        return null;
    }
}
