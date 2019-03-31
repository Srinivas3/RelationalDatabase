package optimizer;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import operators.*;

import java.util.ArrayList;
import java.util.List;
import java.util.*;

public class QueryOptimizer extends Eval {
    List<String> columnsInExp;
    public Operator getOptimizedOperator(Operator operator){

        if (operator instanceof SelectionOperator){
            return pushDown((SelectionOperator)operator);
        }
        else{
            return operator;
        }

    }
    private Operator pushDown(SelectionOperator selectionOperator){
        Operator child = selectionOperator.getChild();
        if (child instanceof ProjectionOperator){
            return pushDown(selectionOperator,(ProjectionOperator)child);
        }
        else if (child instanceof JoinOperator){
            return pushDown(selectionOperator,(JoinOperator)child);
        }
        else if (child instanceof InMemoryCacheOperator){
            return pushDown(selectionOperator,(InMemoryCacheOperator)child);
        }
        else if (child instanceof OnDiskCacheOperator){
            return pushDown(selectionOperator,(OnDiskCacheOperator)child);
        }
        else{
            return selectionOperator;
        }

    }

    private Operator pushDown(SelectionOperator parent, OnDiskCacheOperator cache) {
        Operator temp = cache.getChild();
        parent.setChild(temp);
        cache.setChild(pushDown(parent));
        return cache;
    }

    private Operator pushDown(SelectionOperator parent, InMemoryCacheOperator cache) {
        Operator temp = cache.getChild();
        parent.setChild(temp);
        cache.setChild(pushDown(parent));
        return cache;
    }

    private Operator pushDown(SelectionOperator parent,ProjectionOperator child){
        return parent;
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

        if (selectAndExpressions.size() == 0){
            return joinChild;
        }
        else{
            Expression andExpression = constructByAnding(selectAndExpressions);
            selectParent.setWhereExp(andExpression);
            return selectParent;
        }
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
            e.printStackTrace();
        }
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
