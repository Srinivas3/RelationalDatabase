package preCompute;

import dubstep.Main;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Select;
import operators.Operator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import optimizer.QueryOptimizer;
import utils.Constants;
import utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

import static utils.Constants.BASE_DIR;

public class ViewBuilder {

    private Join ordersLineItemJoin;
    private List<String> queries;
    private String queriesFile = "queriesFile";
    private int viewCnt = 1;
    private PreProcessor preProcessor;

    public ViewBuilder() {
        preProcessor = new PreProcessor();
    }

    public void buildViews() {
        buildLineItemViews();
        buildOrderViews();
        /*
        addQueries();
        writeQueriesToDisk();
        File viewQueriesFile = new File(queriesFile);
        try {
            FileInputStream fileInputStream = new FileInputStream(viewQueriesFile);
            runQueries(fileInputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        */
        saveViewNumLines();
        saveViewToExps();
    }

    private void saveViewToExps() {
        Set<String> viewNames = Utils.viewToExpression.keySet();
        String queryPrefix = "Select * from blah where ";
        for (String viewName : viewNames) {
            File viewExpFile = new File(Constants.VIEW_EXPS_DIR, viewName);
            try {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(viewExpFile));
                bufferedWriter.write(queryPrefix + getExpressionAsRawString(Utils.viewToExpression.get(viewName)) + ";");
                bufferedWriter.newLine();
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getExpressionAsRawString(Expression expression) {
        if (expression instanceof PrimitiveValue) {
            if (expression instanceof DateValue) {
                DateValue dateValue = (DateValue) expression;
                return "DATE('" + dateValue.toRawString() + "')";
            }
            if (expression instanceof  StringValue){
                StringValue stringValue = (StringValue)expression;
                return stringValue.toString();
            }

            return ((PrimitiveValue) expression).toRawString();
        }
        if (expression instanceof Function) {
            Function function = (Function) expression;
            return function.toString();
        }
        if (expression instanceof Column) {
            return expression.toString();
        }
        BinaryExpression binaryExpression = (BinaryExpression) expression;
        String leftExpAsRawString = getExpressionAsRawString(binaryExpression.getLeftExpression());
        String rightExpAsRawString = getExpressionAsRawString(binaryExpression.getRightExpression());
        return leftExpAsRawString + " " + binaryExpression.getStringExpression() + " " + rightExpAsRawString;
    }

    private void buildLineItemViews() {
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", "");
        String whereFilter = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE";
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter);
        String[] shipmodes = getShipmodes();
        for (String shipmode : shipmodes) {
            String shipmodeFilter = " WHERE LINEITEM.SHIPMODE = '" + shipmode + "'";
            buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", shipmodeFilter);
        }
    }

    private String[] getShipmodes() {
        String[] shipmodes = {"MAIL", "RAIL", "FOB", "TRUCK", "AIR", "SHIP", "REG AIR"};
        return shipmodes;

    }

    private void buildOrderViews() {
        buildDateViews("ORDERS", "ORDERS.ORDERDATE", "");
    }

    private Expression getCommitReceiptExp() {
        Table table = new Table("LINEITEM");
        Column commitDate = new Column(table, "COMMITDATE");
        Column receiptDate = new Column(table, "RECEIPTDATE");
        Column shipDate = new Column(table, "SHIPDATE");
        MinorThan minorThan1 = new MinorThan(commitDate, receiptDate);
        MinorThan minorThan2 = new MinorThan(shipDate, commitDate);
        return new AndExpression(minorThan1, minorThan2);
    }

    private void buildDateViews(String tableName, String dateTableColName, String whereExp) {
        String projectedCols = getImportantCols(tableName);
        String lineItemQuery = "Select " + projectedCols + " from " + tableName + whereExp;
        Operator operator = buildQueryTree(lineItemQuery);
        new QueryOptimizer().projectionPushdown(operator);
        Map<String, Integer> schema = operator.getSchema();
        List<DateValue> dateValuePartitions = getDateValuePartitions();
        List<String> viewNames = getViewNames(dateValuePartitions.size());
        TreeMap<String, String> dateToViewName = getDateToViewName(dateValuePartitions, viewNames);
        TreeMap<String, DataOutputStream[]> partitionToStreams = getPartitionToStreams(dateValuePartitions, schema, viewNames);
        Map<String, PrimitiveValue> tuple;
        Set<String> tableColNames = schema.keySet();
        while ((tuple = operator.next()) != null) {
            DateValue dateValue = (DateValue) tuple.get(dateTableColName);
            String dateValStr = dateValue.toRawString();
            String dateValFloorKey = partitionToStreams.floorKey(dateValStr);
            DataOutputStream[] dataOutputStreams = partitionToStreams.get(dateValFloorKey);
            writeTuple(tableColNames, dataOutputStreams, tuple);
            String viewName = dateToViewName.get(dateValFloorKey);
            Integer viewNumLines = Utils.tableToLines.get(viewName);
            if (viewNumLines != null) {
                Utils.tableToLines.put(viewName, viewNumLines + 1);
            } else {
                Utils.tableToLines.put(viewName, 1);
            }
        }
        Set<String> datePartitions = partitionToStreams.keySet();
        for (String dateValStr : datePartitions) {
            preProcessor.flushAndClose(partitionToStreams.get(dateValStr));
        }
        saveViewColDefs(operator.getSchema(), viewNames);
        for (String viewName : viewNames) {
            saveViewToSchema(viewName, operator);
        }
        Expression additionalWhereExp = getAdditionalWhereExp((ProjectionOperator) operator);
        insertViewToExpression(dateToViewName, additionalWhereExp, dateTableColName);

    }

    private void insertViewToExpression(TreeMap<String, String> dateToViewName, Expression additionalWhereExp, String tableColName) {
        Set<String> dateStrings = dateToViewName.keySet();
        for (String dateStr : dateStrings) {
            DateValue firstDateVal = new DateValue(dateStr);
            DateValue secondDateVal = getNextYearDateVal(firstDateVal);
            Expression rangeExp = getRangeExp(firstDateVal, secondDateVal, tableColName);
            String viewName = dateToViewName.get(dateStr);
            if (additionalWhereExp != null) {
                Utils.viewToExpression.put(viewName, new AndExpression(rangeExp, additionalWhereExp));
            } else {
                Utils.viewToExpression.put(viewName, rangeExp);
            }
        }
    }

    private DateValue getNextYearDateVal(DateValue firstDateVal) {
        String parts[] = firstDateVal.toRawString().split("-");
        String year = String.valueOf(Integer.valueOf(parts[0]) + 1);
        String dateStr = year + "-" + parts[1] + "-" + parts[2];
        return new DateValue(dateStr);
    }

    private Expression getRangeExp(DateValue firstDateVal, DateValue secondDateVal, String tableColName) {
        String[] parts = tableColName.split("\\.");
        Table table = new Table(parts[0]);
        String colName = parts[1];
        Column column = new Column(table, colName);
        GreaterThanEquals firstExp = new GreaterThanEquals(column, firstDateVal);
        MinorThan secondExp = new MinorThan(column, secondDateVal);
        return new AndExpression(firstExp, secondExp);
    }

    private Expression getAdditionalWhereExp(ProjectionOperator projectionOperator) {
        Operator child = projectionOperator.getChild();
        if (child instanceof SelectionOperator) {
            SelectionOperator selectionOperator = (SelectionOperator) child;
            return selectionOperator.getWhereExp();
        } else {
            return null;
        }
    }

    private void setAdditionalWhereExp(ProjectionOperator projectionOperator, Expression additionalExp) {
        Operator child = projectionOperator.getChild();
        if (child instanceof SelectionOperator) {
            SelectionOperator selectionOperator = (SelectionOperator) child;
            AndExpression andExpression = new AndExpression();
            andExpression.setLeftExpression(selectionOperator.getWhereExp());
            andExpression.setRightExpression(additionalExp);
            selectionOperator.setWhereExp(andExpression);
        } else {
            projectionOperator.setChild(new SelectionOperator(additionalExp, child));
        }
    }

    private String getImportantCols(String tableName) {
        StringJoiner stringJoiner = new StringJoiner(",");
        List<ColumnDefinition> colDefs = Utils.nameToColDefs.get(tableName);
        for (ColumnDefinition columnDefinition : colDefs) {
            String colName = columnDefinition.getColumnName();
            if (!colName.equalsIgnoreCase("comment")) {
                stringJoiner.add(tableName + "." + colName);
            }

        }
        return stringJoiner.toString();

    }

    private void saveViewColDefs(Map<String, Integer> schema, List<String> viewNames) {
        for (String viewName : viewNames) {
            addColDefs(schema, viewName);
        }
    }

    private void saveViewNumLines() {
        Set<String> tableNames = Utils.tableToLines.keySet();
        for (String tableName : tableNames) {
            preProcessor.populateTupleCount(tableName);
        }
    }

    private TreeMap<String, String> getDateToViewName(List<DateValue> dateValuePartitions, List<String> viewNames) {
        TreeMap<String, String> dateToViewName = new TreeMap<String, String>();
        Iterator<DateValue> dateValueIterator = dateValuePartitions.iterator();
        Iterator<String> viewNameIterator = viewNames.iterator();
        while (dateValueIterator.hasNext()) {
            dateToViewName.put(dateValueIterator.next().toRawString(), viewNameIterator.next());
        }
        return dateToViewName;
    }

    private List<String> getViewNames(int numOfViews) {
        List<String> viewNames = new ArrayList<String>();
        for (int i = 0; i < numOfViews; i++) {
            viewNames.add("view" + viewCnt);
            viewCnt++;
        }
        return viewNames;
    }

    private List<DateValue> getDateValuePartitions() {
        List<DateValue> dateValues = new ArrayList<DateValue>();
        int year;
        String month = "01";
        String day = "01";
        for (year = 1992; year <= 1998; year++) {
            String dateStr = year + "-" + month + "-" + day;
            dateValues.add(new DateValue(dateStr));
        }
        return dateValues;
    }

    private TreeMap<String, DataOutputStream[]> getPartitionToStreams(List<DateValue> dateValueParitions, Map<String, Integer> schema, List<String> viewNames) {
        TreeMap<String, DataOutputStream[]> partitionToStreams = new TreeMap<String, DataOutputStream[]>();
        Iterator<DateValue> dateValueIterator = dateValueParitions.iterator();
        for (String viewName : viewNames) {
            DataOutputStream[] dataOutputStreams = preProcessor.openDataOutputStreams(schema.keySet(), viewName);
            String dateValStr = dateValueIterator.next().toRawString();
            partitionToStreams.put(dateValStr, dataOutputStreams);
        }
        return partitionToStreams;
    }


    private void runQueries(InputStream inputStream) throws ParseException {
        CCJSqlParser ccjSqlParser = new CCJSqlParser(inputStream);
        Statement statement = null;
        int i = 1;
        while ((statement = ccjSqlParser.Statement()) != null) {
            Select select = (Select) statement;
            Operator operator = Main.handleSelect(select);
            try {
                writeViewToColumnStore(operator);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(i + " view built");
            i++;
        }

    }

    private void addColDefs(Map<String, Integer> schema, String viewName) {
        List<ColumnDefinition> colDefs = new ArrayList<ColumnDefinition>();
        Set<String> tableColNames = schema.keySet();
        for (String tableColName : tableColNames) {
            ColumnDefinition columnDefinition = new ColumnDefinition();
            ColumnDefinition originalColDef = Utils.colToColDef.get(tableColName);
            columnDefinition.setColDataType(originalColDef.getColDataType());
            columnDefinition.setColumnName(tableColName);
            colDefs.add(columnDefinition);
        }
        Utils.nameToColDefs.put(viewName, colDefs);
        preProcessor.saveColDefsToDisk(colDefs, viewName);
    }

    private void writeViewToColumnStore(Operator operator) {
        String viewName = "view" + ++viewCnt;
        addColDefs(operator.getSchema(), viewName);
        saveViewToSchema(viewName, operator);
        List<String> tableColNames = new ArrayList<String>();
        tableColNames.addAll(operator.getSchema().keySet());
        DataOutputStream[] dataOutputStreams = preProcessor.openDataOutputStreams(tableColNames, viewName);
        Map<String, PrimitiveValue> tuple = null;
        int numLines = 0;
        while ((tuple = operator.next()) != null) {
            writeTuple(tableColNames, dataOutputStreams, tuple);
            numLines++;
        }
        Utils.tableToLines.put(viewName, numLines);
        preProcessor.populateTupleCount(viewName);
        preProcessor.flushAndClose(dataOutputStreams);
    }

    private void writeTuple(Collection<String> tableColNames, DataOutputStream[] dataOutputStreams, Map<String, PrimitiveValue> tuple) {
        int i = 0;
        for (String tableColName : tableColNames) {
            preProcessor.writeBytes(dataOutputStreams[i], tuple.get(tableColName));
            i++;
        }
    }

    private void saveViewToSchema(String viewName, Operator operator) {
        Utils.viewToSchema.put(viewName, operator.getSchema());
        Set<String> schemaKeySet = operator.getSchema().keySet();
        File file = new File(Constants.VIEW_SCHEMA_DIR, viewName);
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
            for (String tableColName : schemaKeySet) {
                bufferedWriter.write(tableColName);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void writeQueriesToDisk() {
        File viewQueriesFile = new File(queriesFile);
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(viewQueriesFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            for (String query : queries) {
                bufferedWriter.write(query);
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void addQueries() {
        queries = new ArrayList<String>();
    }

    private Operator buildQueryTree(String query) {
        try {
            String queryFileName = "queryFile";
            FileWriter queryFileWriter = new FileWriter(new File(BASE_DIR, queryFileName));
            BufferedWriter bufferedWriter = new BufferedWriter(queryFileWriter);
            bufferedWriter.write(query + ";");
            bufferedWriter.newLine();
            bufferedWriter.flush();
            bufferedWriter.close();
            FileInputStream fileInputStream = new FileInputStream(new File(BASE_DIR, queryFileName));
            CCJSqlParser ccjSqlParser = new CCJSqlParser(fileInputStream);
            Statement statement = ccjSqlParser.Statement();
            if (statement instanceof Select) {
                Select selectStatement = (Select) statement;
                return Main.handleSelect(selectStatement);
            } else {
                return null;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }
}
