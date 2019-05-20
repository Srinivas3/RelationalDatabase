package optimizer;

import Indexes.PrimaryIndex;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import operators.*;
import operators.joins.IndexScanOperator;
import operators.joins.JoinOperator;
import preCompute.ViewBuilder;
import utils.PrimValComp;
import utils.Utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

import static utils.Utils.getRangeExpressions;
import static utils.Utils.populateAndExpressions;

public class QueryOptimizer extends Eval {
    List<String> columnsInExp = new ArrayList<String>();

    public Operator getOptimizedOperator(Operator operator) {

        if (operator instanceof SelectionOperator) {
            return pushDown((SelectionOperator) operator);
        } else if (operator instanceof ProjectionOperator) {
            return pushDown((ProjectionOperator) operator);
        } else {
            return operator;
        }

    }

    public void projectionPushdown(Operator operator) {
        Set<String> colsRefered = new HashSet<String>();
        replaceTableScans(colsRefered, operator);
    }

    public PrimitiveValue eval(Function function) throws SQLException {
        if (function.isAllColumns() != true) {
            List<Expression> expressions = function.getParameters().getExpressions();
            for (Expression expression : expressions) {
                eval(expression);
            }
        }
        return new LongValue(1);
    }

    public Operator replaceWithJoinViews(Operator operator) {
        if (operator instanceof JoinOperator) {
            JoinOperator joinOperator = (JoinOperator) operator;
            Map<String, Integer> joinSchema = joinOperator.getSchema();
            TableScan viewScan = getMatchingJoinView(joinSchema, joinOperator.getJoin());
            if (viewScan != null) {
                Expression combinedWhereExp = getCombinedWhereExp(joinOperator);
                if (combinedWhereExp != null) {
                    SelectionOperator selectionOperator = new SelectionOperator(combinedWhereExp, viewScan);
                    return selectionOperator;
                } else {
                    return viewScan;
                }
            }
            Operator newJoinLeftChild = replaceWithJoinViews(joinOperator.getLeftChild());
            Operator newJoinRightChild = replaceWithJoinViews(joinOperator.getRightChild());
            joinOperator.setChild("left", newJoinLeftChild);
            joinOperator.setChild("right", newJoinRightChild);
        } else if (operator instanceof SingleChildOperator) {
            SingleChildOperator singleChildOperator = (SingleChildOperator) operator;
            singleChildOperator.setChild(replaceWithJoinViews(singleChildOperator.getChild()));
        }
        return operator;
    }

    private Expression getCombinedWhereExp(Operator operator) {
        if (operator instanceof SingleChildOperator) {
            SingleChildOperator singleChildOperator = (SingleChildOperator) operator;
            if (singleChildOperator instanceof SelectionOperator) {
                SelectionOperator selectionOperator = (SelectionOperator) singleChildOperator;
                Expression whereExp = selectionOperator.getWhereExp();
                Expression remainingExp = getCombinedWhereExp(selectionOperator.getChild());
                if (remainingExp != null) {
                    return getAndExpression(whereExp, remainingExp);
                } else {
                    return whereExp;
                }
            } else {
                return getCombinedWhereExp(singleChildOperator.getChild());
            }
        } else if (operator instanceof JoinOperator) {
            JoinOperator joinOperator = (JoinOperator) operator;
            Expression leftExp = getCombinedWhereExp(joinOperator.getLeftChild());
            Expression rightExp = getCombinedWhereExp(joinOperator.getRightChild());
            if (leftExp == null) {
                return rightExp;
            }
            if (rightExp == null) {
                return leftExp;
            }
            return getAndExpression(leftExp, rightExp);
        } else {
            return null;
        }
    }

    private Expression getAndExpression(Expression leftExp, Expression rightExp) {
        AndExpression andExpression = new AndExpression();
        andExpression.setLeftExpression(leftExp);
        andExpression.setRightExpression(rightExp);
        return andExpression;
    }

    private TableScan getMatchingJoinView(Map<String, Integer> joinSchema, Join join) {
        Set<String> joinViewNames = Utils.viewToSchema.keySet();
        for (String joinViewName : joinViewNames) {
            Map<String, Integer> viewSchema = Utils.viewToSchema.get(joinViewName);
            if (checkIfSchemasMatch(viewSchema, joinSchema)) {
                return new TableScan(joinViewName);
            }
        }
        return null;
    }

    private boolean checkIfSchemasMatch(Map<String, Integer> viewSchema, Map<String, Integer> joinSchema) {
        Set<String> viewCols = viewSchema.keySet();
        Set<String> joinCols = joinSchema.keySet();
        if (joinCols.containsAll(viewCols) && viewCols.containsAll(joinCols)) {
            return true;
        }
        return false;
    }

    private void replaceTableScans(Set<String> colsRefered, Operator operator) {
        columnsInExp = new ArrayList<String>();
        if (operator instanceof ProjectionOperator || operator instanceof GroupByOperator) {
            List<SelectItem> selectItems = getSelectItems(operator);
            if (selectItems.get(0) instanceof AllColumns) {
                return;
            }
            List<Expression> expressions = getExpressionsFromSelectItems(selectItems);
            Expression addedExpression = Utils.constructByAdding(expressions);
            try {
                eval(addedExpression);
            } catch (SQLException e) {
            }
            colsRefered.addAll(columnsInExp);
        } else if (operator instanceof SelectionOperator) {
            SelectionOperator selectionOperator = (SelectionOperator) operator;
            try {
                eval(selectionOperator.getWhereExp());
            } catch (SQLException e) {
            }
            colsRefered.addAll(columnsInExp);

        } else if (operator instanceof DoubleChildOperator) {
            DoubleChildOperator doubleChildOperator = (DoubleChildOperator) operator;
            if (doubleChildOperator instanceof JoinOperator) {
                JoinOperator joinOperator = (JoinOperator) operator;
                try {
                    eval(joinOperator.getJoin().getOnExpression());
                } catch (Exception e) {
                }
                colsRefered.addAll(columnsInExp);
            }
            Operator leftChild = doubleChildOperator.getLeftChild();
            Operator rightChild = doubleChildOperator.getRightChild();
            replaceTableScans(colsRefered, leftChild);
            replaceTableScans(colsRefered, rightChild);
            if (leftChild instanceof TableScan) {
                TableScan tableScan = (TableScan) leftChild;
                doubleChildOperator.setLeftChild(new ProjectedTableScan(colsRefered, tableScan.getTableName(), tableScan.isView()));
            }
            if (rightChild instanceof TableScan) {
                TableScan tableScan = (TableScan) rightChild;
                doubleChildOperator.setRightChild(new ProjectedTableScan(colsRefered, tableScan.getTableName(), tableScan.isView()));
            }
        } else if (operator instanceof TableScan || operator instanceof InsertOperator) {
            return;
        }


        if (operator instanceof SingleChildOperator) {
            SingleChildOperator singleChildOperator = (SingleChildOperator) operator;
            Operator child = singleChildOperator.getChild();
            replaceTableScans(colsRefered, child);
            if (child instanceof TableScan) {
                TableScan tableScan = (TableScan) child;
                singleChildOperator.setChild(new ProjectedTableScan(colsRefered, tableScan.getTableName(), tableScan.isView()));
            }
        }

    }

    private List<SelectItem> getSelectItems(Operator operator) {
        if (operator instanceof ProjectionOperator) {
            return ((ProjectionOperator) operator).getSelectItems();
        }
        if (operator instanceof GroupByOperator) {
            return ((GroupByOperator) operator).getSelectItems();
        }
        return null;
    }

    private List<Expression> getExpressionsFromSelectItems(List<SelectItem> selectItems) {
        List<Expression> expressions = new ArrayList<Expression>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                expressions.add(selectExpressionItem.getExpression());
            }
        }
        return expressions;
    }


    private Operator pushDown(SelectionOperator selectionOperator) {
        Operator child = selectionOperator.getChild();
        if (child instanceof JoinOperator) {
            return pushDown(selectionOperator, (JoinOperator) child);
        } else if (child instanceof TableScan) {
            return getIndexScan(selectionOperator, (TableScan) child);
        } else {
            return selectionOperator;
        }

    }

    private Operator getIndexScan(SelectionOperator selectionOperator, TableScan tableScanChild) {
        List<Expression> selectAndExpressions = new ArrayList<Expression>();
        Expression indexScanExpression = null;
        populateAndExpressions(selectionOperator.getWhereExp(), selectAndExpressions);
        PrimaryIndex primaryIndex = null;
        PrimitiveValue primitiveValue = null;
        String conType = "";
        Expression filterCondition = null;
        for (Expression expression : selectAndExpressions) {
            conType = getConType(expression);
            if (conType != null) {
                BinaryExpression binaryExpression = (BinaryExpression) expression;
                Expression leftExpression = binaryExpression.getLeftExpression();
                Expression rightExpression = binaryExpression.getRightExpression();
                if (leftExpression instanceof Column && rightExpression instanceof PrimitiveValue) {
                    String tableColName = getTableColName((Column) leftExpression, tableScanChild.getTableName());
                    if ((primaryIndex = Utils.colToPrimIndex.get(tableColName)) != null) {
                        indexScanExpression = expression;
                        primitiveValue = Utils.getPrimitiveValue(rightExpression);
                        break;
                    }
                }
            }
        }
        if (indexScanExpression != null) {
            filterCondition = getFilteredCondition(selectAndExpressions, indexScanExpression);
            return new IndexScanOperator(conType, primitiveValue, filterCondition, primaryIndex);
        }
        return selectionOperator;
    }

    private String getTableColName(Column column, String tableName) {
        String colName = Utils.getColName(column.getColumnName());
        return tableName + "." + colName;
    }

    private String getConType(Expression expression) {
        if (expression instanceof GreaterThanEquals) {
            return "GreaterEqualsTo";
        } else if (expression instanceof EqualsTo) {
            return "EqualsTo";
        } else if (expression instanceof GreaterThan) {
            return "GreaterThan";
        } else {
            return null;
        }
    }


    private Expression getFilteredCondition(List<Expression> selectAndExpressions, Expression removeExpression) {
        selectAndExpressions.remove(removeExpression);
        if (selectAndExpressions.size() > 0) {
            return Utils.constructByAdding(selectAndExpressions);
        } else {
            return null;
        }

    }

    private Operator pushDown(ProjectionOperator projectionOperator) {

        SelectItem selectItem = projectionOperator.getSelectItems().get(0);
        if (selectItem instanceof AllColumns) {
            return projectionOperator;
        }
        if (selectItem instanceof SelectExpressionItem) {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
            if (selectExpressionItem.getExpression() instanceof Function) {
                return projectionOperator;
            }
        }
        if (projectionOperator.getChild() instanceof JoinOperator) {
            return pushDown(projectionOperator, (JoinOperator) projectionOperator.getChild());
        }
        return projectionOperator;
    }

    private Operator pushDown(ProjectionOperator projectionParent, JoinOperator joinChild) {
        List<SelectItem> selectItems = projectionParent.getSelectItems();
        List<Expression> projectionExpressions = new ArrayList<Expression>();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                projectionExpressions.add(selectExpressionItem.getExpression());
            } else {
                return projectionParent;
            }
        }
        Expression expression = Utils.constructByAdding(projectionExpressions);
        columnsInExp = new ArrayList<String>();
        populateColumnsInExp(expression);
        Map<String, Integer> leftChildSchema = joinChild.getLeftChild().getSchema();
        Map<String, Integer> rightChildSchema = joinChild.getRightChild().getSchema();
        Set<String> leftColNames = new HashSet<String>();
        Set<String> rightColNames = new HashSet<String>();
        for (String colName : columnsInExp) {
            if (leftChildSchema.get(colName) != null) {
                leftColNames.add(colName);
            }
            if (rightChildSchema.get(colName) != null) {
                rightColNames.add(colName);
            }
        }
        ProjectionOperator leftProjectionOperator = constructProjectionOperator(leftColNames, joinChild.getLeftChild());
        ProjectionOperator rightProjectionOperator = constructProjectionOperator(rightColNames, joinChild.getRightChild());
        if (leftProjectionOperator != null) {
            joinChild.setChild("left", leftProjectionOperator);
        }
        if (rightProjectionOperator != null) {
            joinChild.setChild("right", rightProjectionOperator);
        }
        pushDown(leftProjectionOperator);
        pushDown(rightProjectionOperator);
        return projectionParent;
    }

    private ProjectionOperator constructProjectionOperator(Set<String> colNames, Operator child) {
        if (colNames.size() == 0) {
            return null;
        }
        List<SelectItem> selectItems = new ArrayList<SelectItem>();
        for (String colName : colNames) {
            Column column = Utils.getColumn(colName);
            SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
            selectExpressionItem.setExpression(column);
            selectItems.add(selectExpressionItem);
        }
        return new ProjectionOperator(selectItems, child, null);
    }

    private Operator pushDown(SelectionOperator selectParent, JoinOperator joinChild) {
        List<Expression> selectAndExpressions = new ArrayList<Expression>();
        populateAndExpressions(selectParent.getWhereExp(), selectAndExpressions);
        List<Expression> leftChildExps = getChildOnlyExps(joinChild.getLeftChild(), selectAndExpressions);
        composeAndAddSelectOperator(joinChild, joinChild.getLeftChild(), leftChildExps, "left");
        removeExpressions(selectAndExpressions, leftChildExps);
        List<Expression> rightChildExps = getChildOnlyExps(joinChild.getRightChild(), selectAndExpressions);
        composeAndAddSelectOperator(joinChild, joinChild.getRightChild(), rightChildExps, "right");
        removeExpressions(selectAndExpressions, rightChildExps);
        List<Expression> equalExpressions = getEqualExpressions(selectAndExpressions);
        Operator newJoin = getNewJoin(equalExpressions, joinChild);
        removeExpressions(selectAndExpressions, equalExpressions);

        if (selectAndExpressions.size() == 0) {
            return newJoin;
        } else {
            Expression andExpression = Utils.constructByAnding(selectAndExpressions);
            selectParent.setWhereExp(andExpression);
            selectParent.setChild(newJoin);
            return selectParent;
        }
    }

    private Operator getNewJoin(List<Expression> equalExpressions, JoinOperator joinChild) {
        if (equalExpressions.size() == 0) {
            return joinChild;
        }
        Expression additionalOnExpression = Utils.constructByAnding(equalExpressions);
        Join join = joinChild.getJoin();
        if (!join.isSimple()) {
            Expression childJoinOnExpression = join.getOnExpression();
            Expression totalOnExpression = new AndExpression(additionalOnExpression, childJoinOnExpression);
            return new JoinOperator(joinChild.getLeftChild(), joinChild.getRightChild(), join);
        } else {
            if (join.isNatural()) {
                return joinChild;
            }
            join.setSimple(false);
            join.setOnExpression(additionalOnExpression);
            return new JoinOperator(joinChild.getLeftChild(), joinChild.getRightChild(), join);
        }
    }

    private List<Expression> getEqualExpressions(List<Expression> selectAndExpressions) {
        List<Expression> equalExpressions = new ArrayList<Expression>();
        for (Expression expression : selectAndExpressions) {
            if (expression instanceof EqualsTo) {
                equalExpressions.add((EqualsTo) expression);
            }
        }
        return equalExpressions;
    }

    private void removeExpressions(List<Expression> removeFrom, List<Expression> removable) {
        for (Expression expression : removable) {
            removeFrom.remove(expression);
        }
    }


    private void composeAndAddSelectOperator(JoinOperator joinOperator, Operator joinChild, List<Expression> childExps, String leftOrRight) {
        if (childExps.size() == 0) {
            return;
        }
        Expression andExpression = Utils.constructByAnding(childExps);
        SelectionOperator selectionOperator = new SelectionOperator(andExpression, joinChild);
        joinOperator.setChild(leftOrRight, pushDown(selectionOperator));
    }




    private List<Expression> getChildOnlyExps(Operator child, List<Expression> andExpressions) {
        Map<String, Integer> schema = child.getSchema();
        List<Expression> childOnlyExps = new ArrayList<Expression>();
        for (Expression expression : andExpressions) {
            columnsInExp = new ArrayList<String>();
            populateColumnsInExp(expression);
            if (schema.keySet().containsAll(columnsInExp)) {
                childOnlyExps.add(expression);
            }
            columnsInExp.clear();
        }
        return childOnlyExps;
    }

    public PrimitiveValue eval(Column x) {
        columnsInExp.add(x.toString());
        return new LongValue(1);
    }

    private void populateColumnsInExp(Expression expression) {
        try {
            eval(expression);
        } catch (Exception e) {
        }
    }






    public Operator replaceWithSelectionViews(Operator root) {
        if (root instanceof SelectionOperator) {
            SelectionOperator selectionOperator = (SelectionOperator) root;
            Operator selectionChild = selectionOperator.getChild();
            if (selectionChild instanceof TableScan) {
                TableScan view = getMatchingSelectionView(selectionOperator, (TableScan) selectionChild);
                if (view != null) {
                    selectionOperator.setChild(view);
                }
                return root;
            }

        }
        if (root instanceof SingleChildOperator) {
            SingleChildOperator singleChildOperator = (SingleChildOperator) root;
            singleChildOperator.setChild(replaceWithSelectionViews(singleChildOperator.getChild()));
        } else if (root instanceof DoubleChildOperator) {
            DoubleChildOperator doubleChildOperator = (DoubleChildOperator) root;
            doubleChildOperator.setLeftChild(replaceWithSelectionViews(doubleChildOperator.getLeftChild()));
            doubleChildOperator.setRightChild(replaceWithSelectionViews(doubleChildOperator.getRightChild()));
        }
        return root;
    }

    private SelectionOperator convertOrToUnion(SelectionOperator selectionOperator) {
        Expression whereExp = selectionOperator.getWhereExp();
        List<Expression> exps = new ArrayList<Expression>();
        populateAndExpressions(whereExp,exps);
        OrExpression orExpression = null;
        for(Expression expression: exps){
            if (expression instanceof  OrExpression){
                orExpression = (OrExpression)expression;
                break;
            }
        }
        if (orExpression == null){
            return selectionOperator;
        }
        Expression leftExp = orExpression.getLeftExpression();
        Expression rightExp = orExpression.getRightExpression();
        exps.remove(orExpression);
        Operator selectionChild = selectionOperator.getChild();
        SelectionOperator leftSelectionOp = new SelectionOperator(leftExp,selectionChild);
        SelectionOperator rightSelectionOp = new SelectionOperator(rightExp,selectionChild);
        selectionOperator.setChild(new UnionOperator(leftSelectionOp,rightSelectionOp));
        Expression remainingExp = null;
        if (exps.size() != 0){
            remainingExp = Utils.constructByAnding(exps);
            selectionOperator.setWhereExp(remainingExp);
            leftSelectionOp.setWhereExp(new AndExpression(leftExp,remainingExp));
            rightSelectionOp.setWhereExp(new AndExpression(rightExp,remainingExp));
        }
        return selectionOperator;
    }

    private SelectionOperator getUnionedSelectionOperator(SelectionOperator selectionOperator) {
        if (!(selectionOperator.getChild() instanceof TableScan)) {
            return selectionOperator;
        }
        List<Expression> andExps = new ArrayList<Expression>();
        populateAndExpressions(selectionOperator.getWhereExp(), andExps);
        List<Expression> rangeColExpressions = new ArrayList<Expression>();
        for (String colName : Utils.rangeScannedCols) {
            for (Expression expression : andExps) {
                    BinaryExpression binaryExpression = (BinaryExpression)expression;
                    if (isValidRangeScan(binaryExpression)){
                        if (colName.equalsIgnoreCase(binaryExpression.getLeftExpression().toString())){
                            rangeColExpressions.add(expression);
                        }
                    }
                    if (rangeColExpressions.size() == 2) {
                    break;
                }
            }

            if (rangeColExpressions.size() > 0) {
                break;
            }
        }
        andExps.removeAll(rangeColExpressions);
        BinaryExpression basicExpression;
        if (rangeColExpressions.size() == 2) {
            basicExpression = new AndExpression(rangeColExpressions.get(0), rangeColExpressions.get(1));
        } else if (rangeColExpressions.size() == 1) {
            basicExpression = (BinaryExpression) rangeColExpressions.get(0);
        } else {
            return selectionOperator;
        }
        if (basicExpression instanceof EqualsTo || basicExpression instanceof NotEqualsTo){
            return selectionOperator;
        }
        List<Expression> expressions = Utils.getRangeExpressions(basicExpression);
        if (expressions == null){
            return selectionOperator;
        }
        int numExps = expressions.size();
        if (numExps == 1) {
            return selectionOperator;
        }
        Expression remainingWhereExp =  null;
        if (andExps.size() != 0){
          remainingWhereExp = Utils.constructByAnding(andExps);
        }
        TableScan selectionChild = (TableScan) selectionOperator.getChild();
        List<SelectionOperator> selectionOperators = new ArrayList<SelectionOperator>();
        for (Expression expression : expressions) {
            Expression selectWhereExp =  expression;
            if (remainingWhereExp != null){
               selectWhereExp = new AndExpression(selectWhereExp,remainingWhereExp);
            }
            SelectionOperator newSelectOperator = new SelectionOperator(selectWhereExp, selectionChild);
            selectionOperators.add(newSelectOperator);
        }
        UnionOperator unionOperator = constructByUnioning(selectionOperators);
        selectionOperator.setChild(unionOperator);
        return selectionOperator;
    }

    private boolean isValidRangeScan(BinaryExpression binaryExpression) {

        Expression leftExpression = binaryExpression.getLeftExpression();
        Expression rightExpression = binaryExpression.getRightExpression();
        if (!(leftExpression instanceof  Column)){
            return false;
        }
        if (!(rightExpression instanceof Function)){
            return false;
        }
        return (binaryExpression instanceof MinorThan) || (binaryExpression instanceof MinorThanEquals) ||
                (binaryExpression instanceof GreaterThan) || (binaryExpression instanceof GreaterThanEquals);
    }

    private UnionOperator constructByUnioning(List<SelectionOperator> selectionOperators) {
        UnionOperator unionOperator = new UnionOperator(selectionOperators.get(0), selectionOperators.get(1));
        for (int i = 2; i < selectionOperators.size(); i++) {
            unionOperator = new UnionOperator(unionOperator, selectionOperators.get(i));
        }
        return unionOperator;
    }

    private TableScan getMatchingSelectionView(SelectionOperator selectionOperator, TableScan selectionChild) {
        List<String> validViews = getSchemaMatchedViews(selectionChild.getSchema());
        List<String> expressionCompatableViews = new ArrayList<String>();
        Expression selectExpression = selectionOperator.getWhereExp();
        for (String viewName : validViews) {
            Expression viewExpression = Utils.viewToExpression.get(viewName);
            if (isCompatable(viewExpression, selectExpression)) {
                expressionCompatableViews.add(viewName);
            }
        }
        if (expressionCompatableViews.size() == 0) {
            return null;
        }
        String viewName = getBestViewToRead(expressionCompatableViews);
        return new TableScan(viewName);
    }

    private void printAvailableViews(List<String> expressionCompatableViews) {
        System.out.println("printing available views");
        for (String viewName:expressionCompatableViews){
            System.out.println(viewName);
        }
    }

    private boolean isCompatable(Expression viewExpression, Expression selectExpression) {
        List<Expression> viewAndExpressions = new ArrayList<Expression>();
        List<Expression> selectAndExpressions = new ArrayList<Expression>();
        populateAndExpressions(viewExpression, viewAndExpressions);
        populateAndExpressions(selectExpression, selectAndExpressions);
        for (Expression viewAndExpression : viewAndExpressions) {
            if (!isAnySelectImplying(selectAndExpressions, viewAndExpression)) {
                return false;
            }
        }
        return true;

    }

    private boolean isAnySelectImplying(List<Expression> selectAndExpressions, Expression viewAndExpression) {
        BinaryExpression viewBinaryExpression = (BinaryExpression) viewAndExpression;
        for (Expression selectAndExpression : selectAndExpressions) {
            if (selectAndExpression instanceof BinaryExpression) {
                BinaryExpression selectBinaryExp = (BinaryExpression) selectAndExpression;
                if (isThisSelectImplying(selectBinaryExp, viewBinaryExpression)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isThisSelectImplying(BinaryExpression selectBinaryExp, BinaryExpression viewBinaryExpression) {
        String viewExpressionAsRawStr = ViewBuilder.getExpressionAsRawString(viewBinaryExpression).replaceAll(" ", "");
        String selectExpressionAsRawStr = ViewBuilder.getExpressionAsRawString(selectBinaryExp).replaceAll(" ", "");
        if (viewExpressionAsRawStr.equalsIgnoreCase(selectExpressionAsRawStr)) {
            return true;
        }
        Expression selectLeft = selectBinaryExp.getLeftExpression();
        Expression selectRight = selectBinaryExp.getRightExpression();
        Expression viewLeft = viewBinaryExpression.getLeftExpression();
        Expression viewRight = viewBinaryExpression.getRightExpression();
        if (!(selectLeft instanceof Column) || !(viewLeft instanceof Column)) {
            return false;
        } else {
            Column viewColumn = (Column)viewLeft;
            Column selectColumn = (Column)selectLeft;
            if(!viewColumn.toString().equalsIgnoreCase(selectLeft.toString())){
                return false;
            }

        }
        if (!checkIfSameTypesNonCols(selectRight, viewRight)) {
            return false;
        }
        if (selectBinaryExp instanceof MinorThan) {
            return isThisSelectImplying((MinorThan) selectBinaryExp, viewBinaryExpression);
        } else if (selectBinaryExp instanceof MinorThanEquals) {
            return isThisSelectImplying((MinorThanEquals) selectBinaryExp, viewBinaryExpression);
        } else if (selectBinaryExp instanceof GreaterThan) {
            return isThisSelectImplying((GreaterThan) selectBinaryExp, viewBinaryExpression);
        } else if (selectBinaryExp instanceof GreaterThanEquals) {
            return isThisSelectImplying((GreaterThanEquals) selectBinaryExp, viewBinaryExpression);
        } else if (selectBinaryExp instanceof EqualsTo) {
            return isThisSelectImplying((EqualsTo) selectBinaryExp, viewBinaryExpression);
        }
        return false;
    }

    private boolean checkIfSameTypesNonCols(Expression exp1, Expression exp2) {
        return ((exp1 instanceof Function) && (exp2 instanceof Function)) || ((exp1 instanceof PrimitiveValue) && (exp2 instanceof PrimitiveValue));
    }

    private boolean isThisSelectImplying(EqualsTo selectBinaryExp, BinaryExpression viewBinaryExpression) {
        Expression viewRightExp = viewBinaryExpression.getRightExpression();
        Expression selectRightExp = selectBinaryExp.getRightExpression();
        PrimValComp primValComp = new PrimValComp();
        if (viewBinaryExpression instanceof EqualsTo) {
            return primValComp.isEqualTo(selectRightExp, viewRightExp);
        } else if (viewBinaryExpression instanceof GreaterThan) {
            return primValComp.isGreaterThan(selectRightExp, viewRightExp);
        } else if (viewBinaryExpression instanceof GreaterThanEquals) {
            return primValComp.isGreaterThanEqual(selectRightExp, viewRightExp);
        } else if (viewBinaryExpression instanceof MinorThan) {
            return primValComp.isMinorThan(selectRightExp, viewRightExp);
        } else if (viewBinaryExpression instanceof MinorThanEquals) {
            return primValComp.isMinorThanEqual(selectRightExp, viewRightExp);
        } else {
            return false;
        }
    }

    private boolean isThisSelectImplying(MinorThan selectBinaryExp, BinaryExpression viewBinaryExpression) {
        PrimValComp primValComp = new PrimValComp();
        Expression viewRightExp = viewBinaryExpression.getRightExpression();
        Expression selectRightExp = selectBinaryExp.getRightExpression();
        if (viewBinaryExpression instanceof MinorThanEquals || viewBinaryExpression instanceof MinorThan) {
            return primValComp.isGreaterThanEqual(viewRightExp, selectRightExp);
        }
        return false;

    }

    private boolean isThisSelectImplying(MinorThanEquals selectBinaryExp, BinaryExpression viewBinaryExpression) {
        Expression viewRightExp = viewBinaryExpression.getRightExpression();
        Expression selectRightExp = selectBinaryExp.getRightExpression();
        PrimValComp primValComp = new PrimValComp();
        if (viewBinaryExpression instanceof MinorThan) {
            return primValComp.isGreaterThan(viewRightExp, selectRightExp);
        } else if (viewBinaryExpression instanceof MinorThanEquals) {
            return primValComp.isGreaterThanEqual(viewRightExp, selectRightExp);

        }
        return false;

    }

    private boolean isThisSelectImplying(GreaterThan selectBinaryExp, BinaryExpression viewBinaryExpression) {
        Expression viewRightExp = viewBinaryExpression.getRightExpression();
        Expression selectRightExp = selectBinaryExp.getRightExpression();
        PrimValComp primValComp = new PrimValComp();
        if (viewBinaryExpression instanceof GreaterThanEquals || viewBinaryExpression instanceof GreaterThan) {
            return primValComp.isMinorThanEqual(viewRightExp, selectRightExp);
        }
        return false;
    }

    private boolean isThisSelectImplying(GreaterThanEquals selectBinaryExp, BinaryExpression viewBinaryExpression) {
        Expression viewRightExp = viewBinaryExpression.getRightExpression();
        Expression selectRightExp = selectBinaryExp.getRightExpression();
        PrimValComp primValComp = new PrimValComp();
        if (viewBinaryExpression instanceof GreaterThan) {
            return primValComp.isMinorThan(viewRightExp, selectRightExp);
        } else if (viewBinaryExpression instanceof GreaterThanEquals) {
            return primValComp.isMinorThanEqual(viewRightExp, selectRightExp);
        }
        return false;

    }


    private String getBestViewToRead(List<String> expressionCompatableViews) {
        String minLinesView = expressionCompatableViews.get(0);
        int minLines = Utils.tableToLines.get(minLinesView);
        int currNumLines;
        for (String viewName : expressionCompatableViews) {
            currNumLines = Utils.tableToLines.get(viewName);
            if (currNumLines < minLines) {
                minLines = currNumLines;
                minLinesView = viewName;
            }
        }
        return minLinesView;

    }

    private List<String> getSchemaMatchedViews(Map<String, Integer> schema) {
        List<String> validViews = new ArrayList<String>();
        Set<String> availableViews = Utils.viewToSchema.keySet();
        Set<String> tableColNames = schema.keySet();
        for (String viewName : availableViews) {
            Map<String, Integer> viewSchema = Utils.viewToSchema.get(viewName);
            for (String tableColName : tableColNames) {
                if (viewSchema.containsKey(tableColName)) {
                    validViews.add(viewName);
                    break;
                }
            }
        }
        return validViews;
    }
}
