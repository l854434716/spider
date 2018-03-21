package manke.spider.output;

import manke.spider.Job.Configurable;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface DataOutput<T> extends Configurable {

    void  output(T t);
}
