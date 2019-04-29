package preCompute;

import Indexes.IndexFactory;
import Indexes.PrimaryIndex;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Constants;
import utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.*;

public class PreComputeLoader {

    private IndexFactory indexFactory = new IndexFactory();

    public void loadSavedState() {
        loadSchemas();
        loadTableLines();
        loadColToByteCnt();
        //loadIndexes();
    }

    private void loadColToByteCnt() {
        File dir = new File(Constants.COLUMN_BYTES_DIR);
        File[] files = dir.listFiles();
        for(File file: files){
            try{
                FileInputStream fileInputStream = new FileInputStream(file);
                DataInputStream dataInputStream = new DataInputStream(fileInputStream);
                Long byteCnt = dataInputStream.readLong();
                Utils.colToByteCnt.put(file.getName(),byteCnt);
            }
            catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    private void loadTableLines() {
        File dir = new File(Constants.LINES_DIR);
        File[] files = dir.listFiles();
        for (File file : files) {
            try {
                DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file));
                Utils.tableToLines.put(file.getName(), dataInputStream.readInt());
                dataInputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void loadIndex(File indexFile) {
        PrimaryIndex primaryIndex = indexFactory.getIndex(indexFile);
        Utils.colToPrimIndex.put(indexFile.getName(), primaryIndex);
    }

    private void loadIndexes() {
        File primaryIndexesDirFile = new File(Constants.PRIMARY_INDICES_DIR);
        File[] files = primaryIndexesDirFile.listFiles();
        for (File indexFile : files) {
            loadIndex(indexFile);
        }
    }

    private void loadSchemas() {
        File dir = new File(Constants.COL_DEFS_DIR);
        File[] colDefFiles = dir.listFiles();
        for (File colDefsFile : colDefFiles) {
            saveTableSchema(colDefsFile);
        }
        File viewSchemasDir = new File(Constants.VIEW_SCHEMA_DIR);
        File[] viewSchemaFiles = viewSchemasDir.listFiles();
        for(File viewSchemaFile: viewSchemaFiles){
            loadViewSchema(viewSchemaFile);
        }
    }

    private void loadViewSchema(File viewSchemaFile) {
        String viewName = viewSchemaFile.getName();
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(viewSchemaFile));
            Map<String,Integer> viewSchema = new LinkedHashMap<String,Integer>();
            int colTabCnt = 0;
            String tableColName = null;
            while((tableColName = bufferedReader.readLine())!= null){
                viewSchema.put(tableColName,colTabCnt);
                colTabCnt++;
            }
            Utils.joinViewToSchema.put(viewName,viewSchema);
            bufferedReader.close();
        }
        catch (Exception e){

        }
    }

    private void saveTableSchema(File colDefsFile) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(colDefsFile));
            List<ColumnDefinition> columnDefinitions = new ArrayList<ColumnDefinition>();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String parts[] = line.split(",");
                ColumnDefinition columnDefinition = new ColumnDefinition();
                columnDefinition.setColumnName(parts[0]);
                ColDataType colDataType = new ColDataType();
                colDataType.setDataType(parts[1]);
                columnDefinition.setColDataType(colDataType);
                String tableColName = colDefsFile.getName() + "." + columnDefinition.getColumnName();
                Utils.colToColDef.put(tableColName,columnDefinition);
                Utils.colToColDef.put(tableColName,columnDefinition);
                columnDefinitions.add(columnDefinition);
            }
            Utils.nameToColDefs.put(colDefsFile.getName(), columnDefinitions);
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
