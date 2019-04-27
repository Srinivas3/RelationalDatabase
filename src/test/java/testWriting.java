import net.sf.jsqlparser.expression.StringValue;

import java.io.*;

public class testWriting {


    public static void main(String args[]) throws IOException {
        BufferedWriter fos;
        File file = new File("/Users/srinivasrishindra/Desktop/CourseWork/Spring_2019/Database_Systems/abc.csv");
        FileWriter fw = new FileWriter(file);
        fos = new BufferedWriter(fw);

        fos.write(new StringValue("dcbjdbcjd").toString());
        fos.write("\n");
        fos.write("dcbjdbcjd");
        fos.write("\n");
        fos.write("dcbjdbcjd");
        fos.write("\n");
        fos.write("dcbjdbcjd");
        fos.write("\n");
        fos.write("dcbjdbcjd");
        fos.write("\n");
        fos.flush();
        fos.close();

        FileReader fis = new FileReader("/Users/srinivasrishindra/Desktop/CourseWork/Spring_2019/Database_Systems/abc.csv");
        BufferedReader bis = new BufferedReader(fis);
        System.out.println(bis.readLine());
    }

}
