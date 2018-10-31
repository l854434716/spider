package manke.spider.job.bibi;

import com.mongodb.client.FindIterable;
import manke.spider.job.AbstractJob;
import org.bson.Document;

public class TFIDFBibiJob extends AbstractJob<FindIterable<Document>,String> {

    @Override
    public void etl() {

    }
}
