package manke.spider.output;

import manke.spider.job.Configurable;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface DataOutput<T> extends Configurable {

    void  output(T t);


    void  close();
}
