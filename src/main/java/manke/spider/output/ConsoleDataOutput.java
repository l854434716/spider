package manke.spider.output;

import java.util.Properties;

/**
 * Created by luozhi on 2018/3/22.
 */
public class ConsoleDataOutput implements  DataOutput<String> {
    @Override
    public void context(Properties properties) {

    }

    @Override
    public void output(String s) {

        System.out.println(s);
    }
}
