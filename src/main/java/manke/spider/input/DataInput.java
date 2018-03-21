package manke.spider.input;

import com.mongodb.client.MongoCollection;
import com.mongodb.connection.Connection;
import org.bson.Document;

/**
 * Created by LENOVO on 2018/3/21.
 */
public interface DataInput<T> {


   T   input();

}
