package manke.spider.mongo;

import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zhiluo on 2018/3/21.
 *
 *  创建单例mongo client
 */
public class MongoClinetSingleton {

    private  static Logger logger= LoggerFactory.getLogger(MongoClinetSingleton.class);

    private  static  MongoClient mongoClient=null;

    private  static Properties mongodbProperties=new Properties();

    static {

        try{
            // 连接到 mongodb 服务
            InputStream is= MongoClinetSingleton.class
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
        }
    }

    public static MongoClient getMongoClinetInstance() {
        return mongoClient;
    }

    private MongoClinetSingleton() {
    }
}
