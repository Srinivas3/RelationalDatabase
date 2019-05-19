package operators;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;
import utils.Utils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class UpdateOperator extends Eval implements Operator, SingleChildOperator {

    Update updateStatement;
    Operator child;
    Expression whereExp;
    List<Expression> colExpressions;
    List<Column> colsList;
    Map<String,PrimitiveValue> childTuple;
    String tableName;

    public UpdateOperator(Update updateStatement,Operator child){
        this.updateStatement = updateStatement;
        handleUpdate();
        this.child = child;
    }

    public Update getUpdateStatement() {
        return updateStatement;
    }

    private void handleUpdate() {
        this.whereExp = updateStatement.getWhere();
        this.colExpressions = updateStatement.getExpressions();
        this.colsList = updateStatement.getColumns();
        this.tableName = updateStatement.getTable().getName();
    }

    @Override
    public Map<String, PrimitiveValue> next() {
        while ((childTuple = child.next()) != null) {
            BooleanValue isUpdateTuple = null;
            try {
                isUpdateTuple = (BooleanValue)eval(whereExp);
                if(isUpdateTuple.toBool()){
                    int i=0;
                    for(Column column : colsList){
                        String tableColName = tableName + "." + column.getColumnName();
                        PrimitiveValue primitiveValue = eval(colExpressions.get(i));
                        childTuple.put(tableColName,primitiveValue);
                    }
                }
            return childTuple;
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return null;
    }

    public PrimitiveValue eval (Column x){
        String colName = x.getColumnName();
        String tableName = x.getTable().getName();
        return Utils.getColValue(tableName, colName, childTuple);
    }

    @Override
    public void init() {

    }

    @Override
    public Map<String, Integer> getSchema() {
        return child.getSchema();
    }

    @Override
    public Operator getChild() {
        return this.child;
    }

    @Override
    public void setChild(Operator child) {

    }


}
