package manke.spider.Job;

import manke.spider.input.DataInput;
import manke.spider.output.DataOutput;

import java.util.Properties;

/**
 * Created by luozhi on 2018/3/21.
 */
public class JobFactory {

    public  static <M,V,T extends Job<M,V>> T createJob(Class<T> cls,
                                                        DataInput<M> dataInput,
                                                        DataOutput<V> dataOutput,
                                                        Properties properties) {
        T t= null;

        try {
            t= (T) Class.forName(cls.getName()).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        t.context(properties);
        t.setInput(dataInput);
        t.setOutput(dataOutput);
        dataInput.context(properties);
        dataOutput.context(properties);
        return  t;

    }
}
