package optimizer;

import Indexes.PrimaryIndex;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import operators.*;
import operators.joins.IndexScanOperator;
import operators.joins.JoinOperator;
import utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

public class QueryOptimizer extends Eval {
    List<String> columnsInExp;
    public Operator getOptimizedOperator(Operator operator){

        if (operator instanceof SelectionOperator){
            return pushDown((SelectionOperator)operator);
        }
        else if (operator instanceof ProjectionOperator){
            return pushDown((ProjectionOperator)operator);
        }
        else{
            return operator;
        }

    }
    private Operator pushDown(SelectionOperator selectionOperator){
        Operator child = selectionOperator.getChild();
        if (child instanceof JoinOperator){
            return pushDown(selectionOperator,(JoinOperator)child);
        }
        else if (child instanceof TableScan ){
            return getIndexScan(selectionOperator,(TableScan) child);
        }
        else{
            return selectionOperator;
        }

    }

    private Operator getIndexScan(SelectionOperator selectionOperator, TableScan tableScanChild){
        List<Expression> selectAndExpressions = new ArrayList<Expression>();
        Expression indexScanExpression = null;
        populateAndExpressions(selectionOperator.getWhereExp(),selectAndExpressions);
        PrimaryIndex primaryIndex=null;
        PrimitiveValue primitiveValue=null;
        String conType="";
        Expression filterCondition=null;
        for (Expression expression : selectAndExpressions) {
            conType = getConType(expression);
            if (conType != null) {
                BinaryExpression binaryExpression = (BinaryExpression) expression;
                Expression leftExpression = binaryExpression.getLeftExpression();
                Expression rightExpression = binaryExpression.getRightExpression();
                if (leftExpression instanceof Column && rightExpression instanceof PrimitiveValue) {
                    String tableColName = getTableColName((Column)leftExpression,tableScanChild.getTableName());
                    if ((primaryIndex = Utils.colToPrimIndex.get(tableColName)) != null) {
                        indexScanExpression = expression;
                        primitiveValue = Utils.getPrimitiveValue(rightExpression);
                        break;
                    }
                }
            }
        }
        if(indexScanExpression!=null){
            filterCondition = getFilteredCondition(selectAndExpressions,indexScanExpression);
            return new IndexScanOperator(conType,primitiveValue,filterCondition,primaryIndex);
        }
        return selectionOperator;
    }

    private String getTableColName(Column column,String tableName) {
        String colName = Utils.getColName(column.getColumnName());
        return tableName+ "." + colName;
    }

    private String getConType(Expression expression) {
        if(expression instanceof GreaterThanEquals){
            return "GreaterEqualsTo";
        }else if(expression instanceof EqualsTo){
            return "EqualsTo";
        }else if(expression instanceof GreaterThan){
            return "GreaterThan";
        }
        else{
            return null;
        }
    }


    private Expression getFilteredCondition(List<Expression> selectAndExpressions, Expression removeExpression) {
        selectAndExpressions.remove(removeExpression);
        if(selectAndExpressions.size()>0){
            return constructByAdding(selectAndExpressions);
        }else {
            return null;
        }

    }

    private Operator pushDown(ProjectionOperator projectionOperator){

        SelectItem selectItem = projectionOperator.getSelectItems().get(0);
        if(selectItem instanceof AllColumns){
            return projectionOperator;
        }
        if (selectItem instanceof SelectExpressionItem){
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
            if (selectExpressionItem.getExpression() instanceof  Function){
                return projectionOperator;
            }
        }
        if (projectionOperator.getChild() instanceof JoinOperator){
            return pushDown(projectionOperator,(JoinOperator)projectionOperator.getChild());
        }
        return projectionOperator;
    }
    private Operator pushDown(ProjectionOperator projectionParent,JoinOperator joinChild){
        List<SelectItem> selectItems = projectionParent.getSelectItems();
        List<Expression> projectionExpressions = new ArrayList<Expression>();
        for (SelectItem selectItem: selectItems){
            if (selectItem instanceof  SelectExpressionItem){
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem)selectItem;
                projectionExpressions.add(selectExpressionItem.getExpression());
            }
            else{
                return projectionParent;
            }
        }
        Expression expression = constructByAdding(projectionExpressions);
        columnsInExp = new ArrayList<String>();
        populateColumnsInExp(expression);
        Map<String,Integer> leftChildSchema = joinChild.getLeftChild().getSchema();
        Map<String,Integer> rightChildSchema = joinChild.getRightChild().getSchema();
        Set<String>  leftColNames = new HashSet<String>();
        Set<String>  rightColNames = new HashSet<String>();
        for(String colName: columnsInExp){
            if (leftChildSchema.get(colName) != null){
                leftColNames.add(colName);
            }
            if (rightChildSchema.get(colName) != null){
                rightColNames.add(colName);
            }
        }
        ProjectionOperator leftProjectionOperator = constructProjectionOperator(leftColNames,joinChild.getLeftChild());
        ProjectionOperator rightProjectionOperator = constructProjectionOperator(rightColNames,joinChild.getRightChild());
        if (leftProjectionOperator != null){
            joinChild.setChild("left",leftProjectionOperator);
        }
        if (rightProjectionOperator != null){
            joinChild.setChild("right",rightProjectionOperator);
        }
        pushDown(leftProjectionOperator);
        pushDown(rightProjectionOperator);
        return projectionParent;
    }

    private ProjectionOperator constructProjectionOperator(Set<String> colNames, Operator child) {
        if (colNames.size() == 0){
            return null;
        }
        List<SelectItem> selectItems = new ArrayList<SelectItem>();
        for (String colName: colNames){
            Column column = Utils.getColumn(colName);
            SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
            selectExpressionItem.setExpression(column);
            selectItems.add(selectExpressionItem);
        }
        return new ProjectionOperator(selectItems,child,null);
    }

    private Operator pushDown(SelectionOperator selectParent, JoinOperator joinChild){
        List<Expression> selectAndExpressions = new ArrayList<Expression>();
        populateAndExpressions(selectParent.getWhereExp(),selectAndExpressions);
        List<Expression> leftChildExps =  getChildOnlyExps(joinChild.getLeftChild(),selectAndExpressions);
        composeAndAddSelectOperator(joinChild,joinChild.getLeftChild(),leftChildExps,"left");
        removeExpressions(selectAndExpressions,leftChildExps);
        List<Expression> rightChildExps =  getChildOnlyExps(joinChild.getRightChild(),selectAndExpressions);
        composeAndAddSelectOperator(joinChild,joinChild.getRightChild(),rightChildExps,"right");
        removeExpressions(selectAndExpressions,rightChildExps);
        List<Expression> equalExpressions = getEqualExpressions(selectAndExpressions);
        Operator newJoin = getNewJoin(equalExpressions,joinChild);
        removeExpressions(selectAndExpressions,equalExpressions);

        if (selectAndExpressions.size() == 0){
            return newJoin;
        }
        else{
            Expression andExpression = constructByAnding(selectAndExpressions);
            selectParent.setWhereExp(andExpression);
            selectParent.setChild(newJoin);
            return selectParent;
        }
    }
    private Operator getNewJoin(List<Expression> equalExpressions, JoinOperator joinChild){
        if (equalExpressions.size() == 0){
            return joinChild;
        }
        Expression additionalOnExpression = constructByAnding(equalExpressions);
        Join join = joinChild.getJoin();
        if (!join.isSimple()){
            Expression childJoinOnExpression = join.getOnExpression();
            Expression totalOnExpression = new AndExpression(additionalOnExpression,childJoinOnExpression);
            return new JoinOperator(joinChild.getLeftChild(),joinChild.getRightChild(),join);
        }
        else{
            if (join.isNatural()){
                return joinChild;
            }
            join.setSimple(false);
            join.setOnExpression(additionalOnExpression);
            return new JoinOperator(joinChild.getLeftChild(),joinChild.getRightChild(),join);
        }
    }
    private List<Expression> getEqualExpressions(List<Expression> selectAndExpressions){
        List<Expression> equalExpressions = new ArrayList<Expression>();
        for(Expression expression : selectAndExpressions){
            if (expression instanceof EqualsTo){
                equalExpressions.add((EqualsTo)expression);
            }
        }
        return equalExpressions;
    }
    private void removeExpressions(List<Expression> removeFrom,List<Expression> removable){
        for (Expression expression: removable){
            removeFrom.remove(expression);
        }
    }


    private void composeAndAddSelectOperator(JoinOperator joinOperator, Operator joinChild, List<Expression> childExps,String leftOrRight) {
        if (childExps.size() == 0){
            return;
        }
        Expression andExpression = constructByAnding(childExps);
        SelectionOperator selectionOperator = new SelectionOperator(andExpression,joinChild);
        joinOperator.setChild(leftOrRight,pushDown(selectionOperator));
    }
    private Expression constructByAnding(List<Expression> expressions){
        Iterator<Expression> expressionIterator = expressions.iterator();
        Expression andExpression = expressionIterator.next();
        while(expressionIterator.hasNext()){
            andExpression = new AndExpression(andExpression,expressionIterator.next());
        }
        return andExpression;
    }


    private List<Expression> getChildOnlyExps(Operator child, List<Expression> andExpressions) {
        Map<String,Integer> schema = child.getSchema();
        List<Expression> childOnlyExps = new ArrayList<Expression>();
        for(Expression expression : andExpressions){
            columnsInExp = new ArrayList<String>();
            populateColumnsInExp(expression);
            if (schema.keySet().containsAll(columnsInExp)){
                childOnlyExps.add(expression);
            }
            columnsInExp.clear();
        }
        return childOnlyExps;
    }
    public PrimitiveValue eval(Column x){
        columnsInExp.add(x.toString());
        return new LongValue(1);
    }
    private void populateColumnsInExp(Expression expression) {
        try{
            eval(expression);
        }
        catch (Exception e){
            //e.printStackTrace();
        }
    }
    private Expression constructByAdding(List<Expression> expressions){
        Iterator<Expression> expItr = expressions.iterator();
        Expression finalExp = expItr.next();
        while (expItr.hasNext()){
            finalExp = new Addition(finalExp,expItr.next());
        }
        return finalExp;

    }


    private void populateAndExpressions(Expression expression, List<Expression> andExpressions){
        if (expression instanceof AndExpression){
            AndExpression andExpression = (AndExpression)expression;
            populateAndExpressions(andExpression.getLeftExpression(),andExpressions);
            populateAndExpressions(andExpression.getRightExpression(),andExpressions);
        }
        else{
            andExpressions.add(expression);
        }
    }



}
