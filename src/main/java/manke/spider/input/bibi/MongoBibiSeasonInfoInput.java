package manke.spider.input.bibi;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import manke.spider.input.DataInput;
import org.bson.Document;

import java.util.Properties;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiSeasonInfoInput implements DataInput<FindIterable<Document>> {

    private  MongoCollection<Document> collection=null;

    public MongoBibiSeasonInfoInput(MongoCollection<Document> collection){
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
