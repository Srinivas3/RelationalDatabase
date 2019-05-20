package preCompute;

import dubstep.Main;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

import static utils.Constants.*;

public class ViewBuilder {

    private Join ordersLineItemJoin;
    private String queriesFile = "queriesFile";
    private String[] specialColumns = {"ORDERS.ORDERDATE"};
    private int viewCnt = 0;
    private static final int PARTITION_SIZE = 4;
    private PreProcessor preProcessor;


    public ViewBuilder() {
        preProcessor = new PreProcessor();
    }

    public void buildViews() {
//        computeStatistics();
//        List<String> queries = null;
//        if (specialColumns != null){
//            queries = getSpecialViewsQueries();
//        }
//        else{
//            queries = getGeneralViewQueries();
//        }
//        writeQueriesToDisk(queries);
//        try {
//            FileInputStream fileInputStream = new FileInputStream(new File(Constants.BASE_DIR,queriesFile));
//            runQueries(fileInputStream);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        saveViewToExps();



        buildLineItemViews();
        buildOrderViews();
        saveViewNumLines();
        saveViewToExps();
    }
    private List<String> getSpecialViewsQueries(){
        return getQueriesForViewsOnCols(Arrays.asList(specialColumns));
    }
    private List<String> getQueriesForViewsOnCols(Collection<String> cols){
        List<String> queries = new ArrayList<String>();
        for(String rangeCol: cols){
            List<String> rangeColQueries = getRangeColViewQueries(rangeCol);
            if (rangeColQueries != null){
                queries.addAll(rangeColQueries);
            }
        }
        return queries;
    }
    private List<String> getGeneralViewQueries(){
        Set<String> rangeCols = Utils.colToMin.keySet();
        return getQueriesForViewsOnCols(rangeCols);
    }
    private List<String> getRangeColViewQueries(String rangeCol){
        PrimitiveValue minVal = Utils.colToMin.get(rangeCol);
        PrimitiveValue maxVal = Utils.colToMax.get(rangeCol);
        List<PrimitiveValue> partitions = getPartitions(minVal,maxVal);
        if (partitions == null){
            return null;
        }
        String[] parts = rangeCol.split("\\.");
        String tableName = parts[0];
        Column column = Utils.getColumn(rangeCol);
        List<String> whereConds = new ArrayList<String>();
        int i,j;
        int partitionsSize = partitions.size();
        for(i = 0,j= 1;j<partitionsSize;j++,i++){
            GreaterThanEquals expression1 = new GreaterThanEquals(column,partitions.get(i));
            MinorThanEquals expression2 = new MinorThanEquals(column,partitions.get(j));
            AndExpression andExpression = new AndExpression(expression1,expression2);
            if (i != 0){
                whereConds.add(getExpressionAsRawString(expression1));
            }
            if (j != partitionsSize-1 ){
                whereConds.add(getExpressionAsRawString(expression2));
            }
            whereConds.add(getExpressionAsRawString(andExpression));
        }
        List<String> queries = getQueries(tableName,whereConds);
        return queries;
    }

    private List<String> getQueries(String tableName, List<String> whereConds) {
        List<String> queries = new ArrayList<>();
        for (String whereCond: whereConds){
           StringJoiner queryJoiner = new StringJoiner(" ");
           queryJoiner.add("SELECT");
           queryJoiner.add(getImportantCols(tableName));
           queryJoiner.add("FROM");
           queryJoiner.add(tableName);
           queryJoiner.add("WHERE");
           queryJoiner.add(whereCond);
           queries.add(queryJoiner.toString());
        }
        return queries;
    }

    private List<PrimitiveValue> getPartitions(PrimitiveValue minVal, PrimitiveValue maxVal){
        try {
            if (minVal instanceof  DoubleValue){
                return getPartitions(minVal.toDouble(),maxVal.toDouble());
            }
            if (minVal instanceof  LongValue){
                return getPartitions(minVal.toLong(),maxVal.toLong());
            }
            if (minVal instanceof DateValue){
                return getPartitions((DateValue)minVal,(DateValue)maxVal);
            }

        }
        catch (PrimitiveValue.InvalidPrimitive throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    private List<PrimitiveValue> getPartitions(long minVal, long maxVal) {
        long offSet  =  (maxVal-minVal)/5;
        if (offSet == 0){
            return null;
        }
        List<PrimitiveValue> primitiveValues = new ArrayList<PrimitiveValue>();
        long curr = minVal;
        while (curr < maxVal){
            primitiveValues.add(new LongValue(curr));
            curr = curr + offSet;
        }
        primitiveValues.add(new LongValue(maxVal));
        return primitiveValues;
    }

    private List<PrimitiveValue> getPartitions(double minVal, double maxVal){
        double offSet = (maxVal-minVal)/PARTITION_SIZE;
        List<PrimitiveValue> primitiveValues = new ArrayList<PrimitiveValue>();
        double curr = minVal;
        while (curr < maxVal){
            primitiveValues.add(new DoubleValue(curr));
            curr = curr + offSet;
        }
        primitiveValues.add(new DoubleValue(maxVal));
        return primitiveValues;
    }
    private List<PrimitiveValue> getPartitions(DateValue minDate, DateValue maxDate){
        List<PrimitiveValue> longPartitions = null;
        List<PrimitiveValue> datePartitions = new ArrayList<PrimitiveValue>();
        String minDateString = minDate.toRawString();
        String maxDateString = maxDate.toRawString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date minJavaDate = sdf.parse(minDateString);
            Date maxJavaDate = sdf.parse(maxDateString);
            longPartitions = getPartitions(minJavaDate.getTime(),maxJavaDate.getTime());
            for (PrimitiveValue primitiveValue: longPartitions){
                Date date = new Date(primitiveValue.toLong());
                String dateString = sdf.format(date);
                datePartitions.add(new DateValue(dateString));
            }
        } catch (PrimitiveValue.InvalidPrimitive throwables) {
            throwables.printStackTrace();
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }
        return datePartitions;
    }



    private void computeStatistics() {
        Collection<String> cols = null;
        if (specialColumns == null){
            cols = Utils.colToColDef.keySet();
        }
        else{
            cols = Arrays.asList(specialColumns);
        }
        for (String col: cols){
            ColumnDefinition colDef = Utils.colToColDef.get(col);
            String dataType = colDef.getColDataType().getDataType();
            boolean isRange = dataType.equalsIgnoreCase("int")
                    || dataType.equalsIgnoreCase("float")
                    || dataType.equalsIgnoreCase("decimal")
                    || dataType.equalsIgnoreCase("date");
            if (isRange) {
                populateMinMax(col);
            }
        }
        saveMinMaxToDisk();
    }

    private void saveMinMaxToDisk() {
        File minFile = new File(MIN_MAX_COL_DIR,MIN_FILE_NAME);
        File maxFile = new File(MIN_MAX_COL_DIR,MAX_FILE_NAME);
        try{
            BufferedWriter minBufWriter = new BufferedWriter(new FileWriter(minFile));
            BufferedWriter maxBufWriter = new BufferedWriter(new FileWriter(maxFile));
            Set<String> cols = Utils.colToMin.keySet();
            for(String col: cols){
                minBufWriter.write(col);
                minBufWriter.write(",");
                minBufWriter.write(Utils.colToMin.get(col).toRawString());
                minBufWriter.newLine();
                maxBufWriter.write(col);
                maxBufWriter.write(",");
                maxBufWriter.write(Utils.colToMax.get(col).toRawString());
                maxBufWriter.newLine();
            }
            minBufWriter.flush();
            minBufWriter.close();
            maxBufWriter.flush();
            maxBufWriter.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }


    }


    private void populateMinMax(String col) {
      String parts[] = col.split("\\.");
      String tableName = parts[0];
      StringJoiner queryJoiner = new StringJoiner(" ");
      queryJoiner.add("SELECT");
      String minExp = "MIN(" + col + ") as min";
      String maxExp = "MAX(" + col + ") as max";
      queryJoiner.add(minExp);
      queryJoiner.add(",");
      queryJoiner.add(maxExp);
      queryJoiner.add("from");
      queryJoiner.add(tableName);
      String query = queryJoiner.toString();
      Operator operator = buildQueryTree(query);
      new QueryOptimizer().projectionPushdown(operator);
      Map<String,PrimitiveValue> minMax = operator.next();
      Utils.colToMin.put(col,minMax.get("min"));
      Utils.colToMax.put(col,minMax.get("max"));
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
        buildDateViews("LINEITEM", "LINEITEM.RECEIPTDATE", "");
        String whereFilter = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE";
        buildDateViews("LINEITEM", "LINEITEM.RECEIPTDATE", whereFilter);
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", "");
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter);
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter);
        String whereFilter1 = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE AND LINEITEM.SHIPMODE = 'RAIL' ";
        String whereFilter2 = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE AND LINEITEM.SHIPMODE = 'MAIL' ";
        String whereFilter3 = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE AND LINEITEM.SHIPMODE = 'SHIP' ";
        String whereFilter4 = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE AND LINEITEM.SHIPMODE = 'TRUCK' ";
        String whereFilter5 = " WHERE LINEITEM.COMMITDATE < LINEITEM.RECEIPTDATE AND LINEITEM.SHIPDATE < LINEITEM.COMMITDATE AND LINEITEM.SHIPMODE = 'FOB' ";
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter1);
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter2);
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter3);
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter4);
        buildDateViews("LINEITEM", "LINEITEM.SHIPDATE", whereFilter5);
//
//        String[] shipmodes = getShipmodes();
//        for (String shipmode : shipmodes) {
//            String shipmodeFilter = " WHERE LINEITEM.SHIPMODE = '" + shipmode + "'";
//            buildDateViews("LINEITEM", "LINEITEM.RECEIPTDATE", shipmodeFilter);
//        }
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
            QueryOptimizer queryOptimizer = new QueryOptimizer();
            queryOptimizer.projectionPushdown(operator);
            operator = queryOptimizer.replaceWithSelectionViews(operator);
            try {
                writeViewToColumnStore(operator);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        List<String> tableColNames = new ArrayList<String>();
        tableColNames.addAll(operator.getSchema().keySet());
        DataOutputStream[] dataOutputStreams = preProcessor.openDataOutputStreams(tableColNames, viewName);
        Map<String, PrimitiveValue> tuple = null;
        int numLines = 0;
        while ((tuple = operator.next()) != null) {
            writeTuple(tableColNames, dataOutputStreams, tuple);
            numLines++;
        }
        ProjectionOperator projectionOperator = (ProjectionOperator)operator;
        SelectionOperator selectionOperator = (SelectionOperator)projectionOperator.getChild();
        Utils.viewToExpression.put(viewName,selectionOperator.getWhereExp());
        saveViewToSchema(viewName, operator);
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

    private void writeQueriesToDisk(List<String> queries) {
        File viewQueriesFile = new File(BASE_DIR,queriesFile);
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
