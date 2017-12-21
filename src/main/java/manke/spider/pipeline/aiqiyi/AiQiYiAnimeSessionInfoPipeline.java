package manke.spider.pipeline.aiqiyi;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.model.AnimeKeyNameConstant;
import manke.spider.pipeline.AbstractMongodbPipeline;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Map;

/**
 * Created by htlan on 2017/12/14.
 * 处理爱奇艺番剧
 */
public class AiQiYiAnimeSessionInfoPipeline extends AbstractMongodbPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(AiQiYiAnimeSessionInfoPipeline.class);
    private UpdateOptions updateOptions=new UpdateOptions().upsert(true);
    // 番剧详细属性kv 放入ResultItems 标识key名称
    public  final  static  String  season_info_kv="sesaon_info_kv";

    @Override
    public void process(ResultItems resultItems, Task task) {
        Map<String,Map<String,String>> season_info_kv=resultItems.get(AiQiYiAnimeSessionInfoPipeline.season_info_kv);
        if (season_info_kv!=null){
            try {
                MongoDatabase mongoDatabase = mongoClient.getDatabase("htlan");
                MongoCollection<Document> collection = mongoDatabase.getCollection("aiqiyi_sessioninfo");

                Document document=new Document();
                if(season_info_kv.get(AnimeKeyNameConstant.SEASON_ID)!=null){
                    document.put("_id",season_info_kv.get(AnimeKeyNameConstant.SEASON_ID));
                    document.putAll(season_info_kv);
                    logger.info("commit {} to queue",document.toJson());
                    collection.replaceOne(Filters.eq("_id",season_info_kv.get(AnimeKeyNameConstant.SEASON_ID)),document,updateOptions);
                    logger.info("commit data success");
                }
            } catch (Exception e) {
                logger.error("store data error ", e);
            }
        }
    }
}
