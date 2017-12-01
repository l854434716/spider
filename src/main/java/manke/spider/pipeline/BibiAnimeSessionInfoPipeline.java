package manke.spider.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by luozhi on 2017/5/25.
 *
 * 处理番剧sessionInfo json的数据
 */
public class BibiAnimeSessionInfoPipeline extends AbstractMongodbPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(BibiAnimeSessionInfoPipeline.class);

    //番剧sessionInfo json存放到 resultItems  中的key 名称
    public  final static  String  bibiSessionInfoJsonStr="bibiSessionInfoJsonStr";

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        String  bibiSessionInfoJsonStrData=resultItems.get(bibiSessionInfoJsonStr);

        if (bibiSessionInfoJsonStrData!=null){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("bibi_sessioninfo_animes");

                Document document=Document.parse(bibiSessionInfoJsonStrData);
                document.remove("seasons");
                document.remove("user_season");
                logger.info("commit {} to queue",document.toJson());
                collection.insertOne(document);
                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }


        }

    }
}