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

import java.util.List;
import java.util.Map;

/**
 * Created by htlan on 2017/12/21.
 */
public class AiQiYiAnimeTimelinePipeline extends AbstractMongodbPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(AiQiYiAnimeTimelinePipeline.class);
    private UpdateOptions updateOptions=new UpdateOptions().upsert(true);
    // 番剧详细属性kv 放入ResultItems 标识key名称
    public  final  static  String  season_info_kv="season_info_kv_list";

    @Override
    public void process(ResultItems resultItems, Task task) {
        List<Map<String,String>> season_info_kvs=resultItems.get(season_info_kv);
        try {
            if(season_info_kvs!=null||season_info_kvs.size()>0){
                for (Map<String,String> season_info_kv:season_info_kvs) {
                    MongoDatabase mongoDatabase = mongoClient.getDatabase("htlan");
                    MongoCollection<Document> collection = mongoDatabase.getCollection("aiqiyi_update");
                    Document document=new Document();
                    document = new Document();
                    document.put("_id",season_info_kv.get(AnimeKeyNameConstant.SEASON_ID));
                    document.putAll(season_info_kv);
                    logger.info("commit {} to queue",document.toJson());
                    if(document.get(AnimeKeyNameConstant.SEASON_ID)!=null){
                        collection.replaceOne(Filters.eq("_id",document.get(AnimeKeyNameConstant.SEASON_ID)),document,updateOptions);
                        logger.info("commit data success");
                        continue;
                    }
                    logger.error("错误Id{}",document.get(AnimeKeyNameConstant.SEASON_ID));
                }
            }
        }catch (Exception e) {
            logger.error("store data error ", e);
        }
    }
}
