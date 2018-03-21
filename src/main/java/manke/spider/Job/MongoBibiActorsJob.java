package manke.spider.Job;

import com.mongodb.client.FindIterable;
import manke.spider.input.DataInput;
import org.bson.Document;

import java.util.List;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiActorsJob extends  AbstractJob<FindIterable<Document>,List<String>> {


    @Override
    public void etl() {

    }



    private  class  MongoBibiAcotorsInput  implements DataInput<FindIterable<Document>>{


        @Override
        public FindIterable<Document> input() {
            return null;
        }
    }



}
