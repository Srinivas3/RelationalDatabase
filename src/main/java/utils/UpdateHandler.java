package utils;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;

import java.sql.SQLException;
import java.util.*;

public class UpdateHandler extends Eval {
    Map<String,PrimitiveValue> currTuple;

    public void handleInsert(Insert insertStatement){
        String tableName = insertStatement.getTable().getName();
        Map<String,PrimitiveValue> insertTuple  = gettupleFromInsertStatement(insertStatement);
        List<Map<String,PrimitiveValue>> insertedTuples = Utils.tableToInserts.get(tableName);
        if(insertedTuples == null){
            insertedTuples = new LinkedList<Map<String,PrimitiveValue>>();
            Utils.tableToInserts.put(tableName,insertedTuples);
        }
        insertedTuples.add(insertTuple);
    }

    public  Map<String,PrimitiveValue> gettupleFromInsertStatement(Insert insertStatement) {
        Map<String,PrimitiveValue> tuple = new HashMap<String,PrimitiveValue>();
        String tableName = insertStatement.getTable().getName();
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
        return tuple;

    }

    public void handleDelete(Delete statement) {
        Delete deleteStatement = statement;
        Expression whereExpression = deleteStatement.getWhere();
        String tableName = deleteStatement.getTable().getName();
        InverseExpression inverseExpression = new InverseExpression(whereExpression);
        List<Expression> deleteExpressions = Utils.tableDeleteExpressions.get(tableName);
        if(deleteExpressions == null){
            deleteExpressions = new ArrayList<Expression>();
            Utils.tableDeleteExpressions.put(tableName,deleteExpressions);
        }
        deleteExpressions.add(inverseExpression);
        deleteFromInsertedTuples(tableName,whereExpression);
    }

    public void deleteFromInsertedTuples(String tableName,Expression whereExp) {
        List<Map<String,PrimitiveValue>> insertedTuples = Utils.tableToInserts.get(tableName);
        List<Map<String,PrimitiveValue>> toRemoveTuples = new LinkedList<Map<String, PrimitiveValue>>();
        if(insertedTuples!=null){
            for(int i = 0; i<insertedTuples.size();i++){
                currTuple = insertedTuples.get(i);
                BooleanValue whereCond = null;
                try {
                    whereCond = (BooleanValue) eval(whereExp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (whereCond.toBool()) {
                    toRemoveTuples.add(currTuple);
                }
            }

            insertedTuples.removeAll(toRemoveTuples);
        }

    }

    @Override
    public PrimitiveValue eval(Column x) throws SQLException {
            String colName = x.getColumnName();
            String tableName = x.getTable().getName();
            return Utils.getColValue(tableName, colName, currTuple);
    }
}
