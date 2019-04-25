package operators.joins;

import Indexes.PrimaryIndex;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operators.Operator;
import operators.TableScan;
import utils.Constants;
import utils.PrimValComp;
import utils.Utils;

import java.io.*;
import java.sql.SQLException;
import java.util.Map;

public class IndexScanOperator extends Eval implements Operator {

    String conType;
    PrimitiveValue primitiveValue;
    PrimaryIndex primaryIndex;
    boolean isFirstCall;
    String filePath;
    Map<String,PrimitiveValue> tuple;
    PrimValComp primValComp = new PrimValComp();
    TableScan tableScan;
    int [] filePositions;
    Expression filterCondition;
    Table table;

    public IndexScanOperator(String conType, PrimitiveValue primitiveValue,Expression filterCondition,PrimaryIndex primaryIndex){
        this.conType = conType;
        this.primitiveValue = primitiveValue;
        this.primaryIndex = primaryIndex;
        this.tableScan = new TableScan(primaryIndex.getTable());
        this.table = tableScan.getTable();
        filePositions = primaryIndex.getPositions();
        this.filterCondition = filterCondition;
        isFirstCall = true;
    }


    public Map<String, PrimitiveValue> next() {
        if(isFirstCall){
            try {
                File compressedTableFile = new File(Constants.COMPRESSED_TABLES_DIR, table.getName());
                int position = primaryIndex.getPosition(primitiveValue);
                FileInputStream fileInputStream = new FileInputStream(compressedTableFile);
                fileInputStream.getChannel().position(filePositions[position]);
                tableScan.getDataInputStream().close();
                tableScan.setDataInputStream(getDataInputStream(fileInputStream));
                tableScan.setTotalLines(filePositions.length - position);
                isFirstCall = false;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        tuple = getNextTuple();
        if(filterCondition!=null){
            while(tuple!=null){
                try {
                    if(eval(filterCondition).toBool()){
                        return tuple;
                    }
                    else {
                        tuple = getNextTuple();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return tuple;

    }

    private DataInputStream getDataInputStream (FileInputStream fileInputStream){
        DataInputStream dataInputStream;
        if(!conType.equalsIgnoreCase("EqualsTo")){
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            dataInputStream = new DataInputStream(bufferedInputStream);
        }
        else {
            dataInputStream = new DataInputStream(fileInputStream);
        }
        return dataInputStream;

    }

    public Map<String, PrimitiveValue> getNextTuple(){
        tuple = tableScan.next();
        if(tuple!=null){
            PrimitiveValue compareValue = Utils.getColValue(table.getName(), primaryIndex.getColName(), tuple);
            if(conType.equalsIgnoreCase("GreaterThan")){
                if(primValComp.compare(compareValue,primitiveValue) > 0 ) {
                    return tuple;
                }
                else if(primValComp.compare(compareValue,primitiveValue)==0){
                        return getNextTuple();
                }
                else{
                    return null;
                }
            }else if(conType.equalsIgnoreCase("EqualsTo")){
                if(primValComp.compare(compareValue,primitiveValue) == 0 ){
                    return tuple;
                }else {
                    return null;
                }
            }
            else if(conType.equalsIgnoreCase("GreaterEqualsTo")){
                if(primValComp.compare(compareValue,primitiveValue) >= 0 ){
                    return tuple;
                }else {
                    return null;
                }
            }
            else return null;
        }
        else {
            return null;
        }
    }

    public void init() {

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
