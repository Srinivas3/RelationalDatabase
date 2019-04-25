package preCompute;

import Indexes.IndexFactory;
import Indexes.PrimaryIndex;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import utils.Constants;
import utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PreComputeLoader {

    private IndexFactory indexFactory = new IndexFactory();

    public void loadSavedState() {
        loadSchemas();
        loadTableLines();
        loadIndexes();
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
        //printIndex(primaryIndex);
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
                columnDefinitions.add(columnDefinition);
            }
            Utils.nameToColDefs.put(colDefsFile.getName(), columnDefinitions);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
