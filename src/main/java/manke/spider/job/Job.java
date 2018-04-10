package manke.spider.job;

import manke.spider.input.DataInput;
import manke.spider.output.DataOutput;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface Job<T,V>  extends Configurable {

    void  start();


    void  setInput(DataInput<T> dataInput);


    void  setOutput(DataOutput<V> dataOutput);


    void   etl();
    
}
