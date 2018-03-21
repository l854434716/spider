package manke.spider.pipeline;

import com.mongodb.MongoClient;
import manke.spider.mongo.MongoClinetSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by luozhi on 2017/6/1.
 */
public abstract  class AbstractMongodbPipeline implements Pipeline{

    private  static Logger logger= LoggerFactory.getLogger(AbstractMongodbPipeline.class);

    protected  static MongoClient  mongoClient= MongoClinetSingleton.getMongoClinetInstance();



}
