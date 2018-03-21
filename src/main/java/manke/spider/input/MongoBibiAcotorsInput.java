package manke.spider.input;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.Properties;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiAcotorsInput  implements DataInput<FindIterable<Document>> {

    private  MongoCollection<Document> collection=null;

    public  MongoBibiAcotorsInput(MongoCollection<Document> collection){
        this.collection=collection;
    }

    @Override
    public FindIterable<Document> input() {
        return collection.find().projection(Projections.fields(Projections.include("actor","season_id"),Projections.excludeId()));
    }

    @Override
    public void context(Properties properties) {

    }
}
