package manke.spider.pipeline;

import com.mongodb.MongoClient;
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

    protected  static MongoClient  mongoClient=null;

    protected  static Properties  mongodbProperties=new Properties();

    static {

        try{
            // 连接到 mongodb 服务
             InputStream is= AbstractMongodbPipeline.class
                    .getClassLoader().getResourceAsStream("mongodb.properties");

             mongodbProperties.load(is);

            mongoClient = new MongoClient(mongodbProperties.getProperty("ip")
                    ,Integer.parseInt(mongodbProperties.getProperty("port")));

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
            System.exit(1);
        }
    }
}
