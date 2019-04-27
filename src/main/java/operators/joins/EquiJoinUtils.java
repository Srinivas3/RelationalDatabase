package operators.joins;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import operators.Operator;
import utils.Utils;

import java.util.*;

public class EquiJoinUtils {

    public List<List<String>> getJoinColPairs() {
        return joinColPairs;
    }

    private List<List<String>> joinColPairs;
    private Map<String, Integer> leftSchema;
    private Map<String, Integer> rightSchema;

    public EquiJoinUtils(JoinOperator joinOperator) {
        leftSchema = joinOperator.getLeftChild().getSchema();
        rightSchema = joinOperator.getRightChild().getSchema();
        joinColPairs = new ArrayList<List<String>>();
        if (joinOperator.getJoin().isNatural()) {
            updateCommonCols();
        } else {
            parseExpressionAndUpdateColPairs(joinOperator.getJoin().getOnExpression());
        }
    }

    private void parseExpressionAndUpdateColPairs(Expression onExpression) {
        if (onExpression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) onExpression;
            parseExpressionAndUpdateColPairs(andExpression.getLeftExpression());
            parseExpressionAndUpdateColPairs(andExpression.getRightExpression());
        } else {
            EqualsTo equalsToExpression = (EqualsTo) onExpression;
            Column firstCol = (Column) equalsToExpression.getLeftExpression();
            Column secondCol = (Column) equalsToExpression.getRightExpression();
            updateColPairs(firstCol.toString(), secondCol.toString());
        }
    }

    private void updateColPairs(String firstColName, String secondColName) {
        List<String> colPair = new ArrayList<String>();
        if (leftSchema.containsKey(firstColName) && rightSchema.containsKey(secondColName)) {
            colPair.add(firstColName);
            colPair.add(secondColName);
        } else {
            colPair.add(secondColName);
            colPair.add(firstColName);
        }
        joinColPairs.add(colPair);
    }

    private void updateCommonCols() {

        Set<String> leftCols = leftSchema.keySet();
        Set<String> rightCols = rightSchema.keySet();
        for (String leftCol : leftCols) {
            for (String rightCol : rightCols) {
                if (Utils.areColsEqual(leftCol, rightCol)) {
                    updateColPairs(leftCol, rightCol);
                }
            }
        }

    }


}
