package manke.spider.pipeline.qq;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.model.qq.QqConstant;
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
 * Created by luozhi on 2017/5/22.
 * 处理将要更新的番剧数据
 */
public class QqAnimeTimelinePipeline extends AbstractMongodbPipeline implements Pipeline {
    private  static Logger logger= LoggerFactory.getLogger(QqAnimeTimelinePipeline.class);


    public final static  String  SEASON_INFO_KVS="season_info_kvs";

    private  UpdateOptions updateOptions=new UpdateOptions().upsert(true);

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        List<Map<String,String>> season_info_kvs=resultItems.get(SEASON_INFO_KVS);

        String updata_time=resultItems.get(QqConstant.UPDATE_TIME);
        if (season_info_kvs!=null&&season_info_kvs.size()>0){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("qq_timeline_animes");
                Document document=null;
                for (Map<String,String>  season_info_kv:season_info_kvs){
                    document = new Document();
                    document.put("_id",season_info_kv.get(QqConstant.SEASON_ID));  //指定_id 为 bibi sessionid
                    document.put(QqConstant.UPDATE_TIME,updata_time);
                    document.putAll(season_info_kv);
                    logger.info("commit {} to queue",document.toJson());
                    collection.replaceOne(Filters.eq("_id",document.get(QqConstant.SEASON_ID)),document,updateOptions);
                }

                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }

        }

    }
}
