package manke.spider.input.qq;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import manke.spider.input.DataInput;
import org.bson.Document;

import java.util.Properties;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoQqSeasonInfoInput implements DataInput<FindIterable<Document>> {

    private  MongoCollection<Document> collection=null;

    public MongoQqSeasonInfoInput(MongoCollection<Document> collection){
        this.collection=collection;
    }

    @Override
    public FindIterable<Document> input() {
        return collection.find();
    }

    @Override
    public void context(Properties properties) {

    }
}
