package manke.spider.pipeline.youku;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.model.youku.YoukuConstant;
import manke.spider.pipeline.AbstractMongodbPipeline;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Map;

/**
 * Created by luozhi on 2017/5/25.
 *
 * 处理番剧sessionInfo json的数据
 */
public class YoukuAnimeSessionInfoPipeline extends AbstractMongodbPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(YoukuAnimeSessionInfoPipeline.class);

    private UpdateOptions updateOptions=new UpdateOptions().upsert(true);


    // 番剧详细属性kv 放入ResultItems 标识key名称
    public  final  static  String  season_info_kv="season_info_kv";

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        Map<String,String>   season_info_kv=resultItems.get(YoukuAnimeSessionInfoPipeline.season_info_kv);
        if (season_info_kv!=null){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("youku_sessioninfo_animes1");

                Document document=new Document();
                document.put("_id",season_info_kv.get(YoukuConstant.SEASON_ID));
                document.putAll(season_info_kv);
                logger.info("commit {} to queue",document.toJson());
                collection.replaceOne(Filters.eq("_id",season_info_kv.get(YoukuConstant.SEASON_ID)),document,updateOptions);
                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }

        }

    }
}