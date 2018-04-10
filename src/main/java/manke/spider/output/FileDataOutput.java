package manke.spider.output;

import java.io.*;
import java.util.Properties;

/**
 * Created by luozhi on 2018/3/22.
 */
public class FileDataOutput implements  DataOutput<String> {

    private BufferedWriter writer;

    public  FileDataOutput(){
        try {
            writer=new BufferedWriter(new FileWriter(new File("./result.csv")));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public  FileDataOutput(String path ,String fileName){
        try {
            writer=new BufferedWriter(new FileWriter(new File(path+fileName)));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Override
    public void context(Properties properties) {

    }

    @Override
    public void output(String s) {

        try {
            writer.write(s);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public  void  close(){
        if (writer!=null){


            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
