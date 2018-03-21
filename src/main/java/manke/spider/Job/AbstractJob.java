package manke.spider.Job;

import manke.spider.input.DataInput;
import manke.spider.output.DataOutput;

import java.util.Properties;
import java.util.Vector;

/**
 * Created by LENOVO on 2018/3/21.
 */
public   abstract  class AbstractJob<T,V>  implements Job<T,V> {

    protected Properties jobConfig=new Properties();


    protected DataInput<T> dataInput;


    protected DataOutput<V> dataOutput;


    @Override
    public void setOutput(DataOutput<V> dataOutput) {
        this.dataOutput=dataOutput;
    }

    @Override
    public void setInput(DataInput<T> dataInput) {
        this.dataInput=dataInput;
    }

    @Override
    public void setJobConfig(Properties properties) {
        jobConfig=properties;
    }

    @Override
    public void start() {
        etl();
    }
}
