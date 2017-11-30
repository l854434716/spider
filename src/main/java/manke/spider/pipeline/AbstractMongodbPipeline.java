package manke.spider.pipeline;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by luozhi on 2017/6/1.
 */
public abstract  class AbstractMongodbPipeline implements Pipeline{

    private  static Logger logger= LoggerFactory.getLogger(AbstractMongodbPipeline.class);

    protected  static MongoClient  mongoClient=null;

    static {

        try{
            // 连接到 mongodb 服务
             mongoClient = new MongoClient( "localhost" , 27017 );

             Runtime.getRuntime().addShutdownHook(new Thread(){

                 @Override
                 public void run() {
                     if (mongoClient!=null){
                         mongoClient.close();
                     }
                 }
             });

        }catch(Exception e){
            logger.error("create mongodb client error {}",e.getCause());
        }
    }
}
