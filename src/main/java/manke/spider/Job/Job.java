package manke.spider.Job;

import manke.spider.input.DataInput;
import manke.spider.output.DataOutput;

import java.util.Properties;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface Job<T,V> {

    void  start();


    void  setInput(DataInput<T> dataInput);


    void  setOutput(DataOutput<V> dataOutput);


    void   etl();


    void   setJobConfig(Properties properties);
}