package Indexes;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Utils;

import java.io.File;
import java.util.HashSet;
import java.util.List;

public class IndexFactory {
    public PrimaryIndex getIndex(Table table, String colName) {
        PrimaryIndex primaryIndex = createIndexObject(table, colName);
        primaryIndex.setPrimaryKeyPositions();
        return primaryIndex;
    }

    public PrimaryIndex createIndexObject(Table table, String colName) {
        String dataType = getColDatatype(colName, table.getName());
        if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("date")) {
            return new StringIndex(table, colName);
        } else if (dataType.equalsIgnoreCase("int")) {
            return new IntegerIndex(table, colName);
        } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float")) {
            return new DoubleIndex(table, colName);
        } else {
            return null;
        }
    }

    public PrimaryIndex getIndex(File indexFile) {
        String parts[] = indexFile.getName().split("\\.");
        String tableName = parts[0];
        String colName = parts[1];
        Table table = new Table();
        table.setName(tableName);
        PrimaryIndex primaryIndex = createIndexObject(table,colName);
        primaryIndex.deserializeFromFile(indexFile);
        return primaryIndex;
    }

    private String getColDatatype(String colName, String tableName) {
        List<ColumnDefinition> columnDefinitions = Utils.nameToColDefs.get(tableName);
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if (columnDefinition.getColumnName().equalsIgnoreCase(colName)) {
                return columnDefinition.getColDataType().getDataType();
            }
        }
        return null;
    }


}
