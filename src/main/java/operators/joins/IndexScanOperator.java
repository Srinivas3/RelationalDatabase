package operators.joins;

import Indexes.PrimaryIndex;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import operators.Operator;
import operators.TableScan;
import utils.Constants;
import utils.PrimValComp;
import utils.Utils;

import java.io.*;
import java.sql.SQLException;
import java.util.Map;

public class IndexScanOperator extends Eval implements Operator {

    private FileInputStream fileInputStream;
    String conType;
    PrimitiveValue primitiveValue;
    PrimaryIndex primaryIndex;
    boolean isFirstCall;
    String filePath;
    Map<String, PrimitiveValue> tuple;
    PrimValComp primValComp = new PrimValComp();
    TableScan tableScan;
    int[] filePositions;
    Expression filterCondition;
    Table table;

    public IndexScanOperator(String conType, PrimitiveValue primitiveValue, Expression filterCondition, PrimaryIndex primaryIndex) {
        this(conType, filterCondition, primaryIndex);
        this.primitiveValue = primitiveValue;
    }

    public IndexScanOperator(String conType, Expression filterCondition, PrimaryIndex primaryIndex) {
        this.conType = conType;
        this.primaryIndex = primaryIndex;
        this.tableScan = new TableScan(primaryIndex.getTable());
        this.table = tableScan.getTable();
        filePositions = primaryIndex.getPositions();
        this.filterCondition = filterCondition;
        setFileInputStream();
        closeAndSetDataInputStream();
        isFirstCall = true;
    }

    private void closeAndSetDataInputStream() {

        try {
            tableScan.getDataInputStream().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        tableScan.setDataInputStream(getDataInputStream());
    }

    private void setFileInputStream() {
        File compressedTableFile = new File(Constants.COMPRESSED_TABLES_DIR, table.getName());
        try {
            this.fileInputStream = new FileInputStream(compressedTableFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void setPrimitiveValue(PrimitiveValue primitiveValue) {
        this.primitiveValue = primitiveValue;
    }

    public Map<String, PrimitiveValue> next() {
        if (isFirstCall) {
            int position = primaryIndex.getPosition(primitiveValue);
            try {
                if (position >= filePositions.length) {
                    return null;
                }
                fileInputStream.getChannel().position(filePositions[position]);
            } catch (IOException e) {
                e.printStackTrace();
            }
            tableScan.setLinesScanned(0);
            tableScan.setTotalLines(filePositions.length - position);
            isFirstCall = false;
        }
        tuple = getNextTuple();
        if (filterCondition != null) {
            while (tuple != null) {
                try {
                    if (eval(filterCondition).toBool()) {
                        return tuple;
                    } else {
                        tuple = getNextTuple();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return tuple;

    }

    private DataInputStream getDataInputStream() {
        DataInputStream dataInputStream;
        if (!conType.equalsIgnoreCase("EqualsTo")) {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            dataInputStream = new DataInputStream(bufferedInputStream);
        } else {
            dataInputStream = new DataInputStream(fileInputStream);
        }
        return dataInputStream;

    }

    public Map<String, PrimitiveValue> getNextTuple() {
        tuple = tableScan.next();
        if (tuple != null) {
            PrimitiveValue compareValue = Utils.getColValue(table.getName(), primaryIndex.getColName(), tuple);
            if (conType.equalsIgnoreCase("GreaterThan")) {
                if (primValComp.compare(compareValue, primitiveValue) > 0) {
                    return tuple;
                } else if (primValComp.compare(compareValue, primitiveValue) == 0) {
                    return getNextTuple();
                } else {
                    return null;
                }
            } else if (conType.equalsIgnoreCase("EqualsTo")) {
                if (primValComp.compare(compareValue, primitiveValue) == 0) {
                    return tuple;
                } else {
                    return null;
                }
            } else if (conType.equalsIgnoreCase("GreaterEqualsTo")) {
                if (primValComp.compare(compareValue, primitiveValue) >= 0) {
                    return tuple;
                } else {
                    return null;
                }
            } else return null;
        } else {
            return null;
        }
    }

    public void init() {
        isFirstCall = true;
    }

    public Map<String, Integer> getSchema() {
        return tableScan.getSchema();
    }

    public PrimitiveValue eval(Column column) throws SQLException {
        String colName = column.getColumnName();
        String tableName = column.getTable().getName();
        return Utils.getColValue(tableName, colName, tuple);
    }
}
