package manke.spider.input;

import com.mongodb.client.MongoCollection;
import com.mongodb.connection.Connection;
import manke.spider.Job.Configurable;
import org.bson.Document;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface DataInput<T> extends Configurable {


   T   input();

}
