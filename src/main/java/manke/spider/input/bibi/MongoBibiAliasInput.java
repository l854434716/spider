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
public class MongoBibiAliasInput implements DataInput<FindIterable<Document>> {

    private  MongoCollection<Document> collection=null;

    public MongoBibiAliasInput(MongoCollection<Document> collection){
        this.collection=collection;
    }

    @Override
    public FindIterable<Document> input() {
        return collection.find().projection(Projections.fields(Projections.include("alias","season_id"),Projections.excludeId()));
    }

    @Override
    public void context(Properties properties) {

    }
}
