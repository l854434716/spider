package manke.spider.input;

import manke.spider.job.Configurable;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface DataInput<T> extends Configurable {


   T   input();

}
