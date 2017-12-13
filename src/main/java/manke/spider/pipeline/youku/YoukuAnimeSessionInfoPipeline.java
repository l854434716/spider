package manke.spider.pipeline.youku;

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

import java.util.Map;

/**
 * Created by luozhi on 2017/5/25.
 *
 * 处理番剧sessionInfo json的数据
 */
public class YoukuAnimeSessionInfoPipeline extends AbstractMongodbPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(YoukuAnimeSessionInfoPipeline.class);

    private UpdateOptions updateOptions=new UpdateOptions().upsert(true);

    //番剧sessionInfo json存放到 resultItems  中的key 名称
    public  final static  String  qqSessionInfoJsonStr="qqSessionInfoJsonStr";

    // 番剧唯一标识
    public  final  static  String  season_info_kv="season_info_kv";

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        String  qqSessionInfoJsonStrData=resultItems.get(qqSessionInfoJsonStr);
        Map<String,String>   season_info_kv=resultItems.get(YoukuAnimeSessionInfoPipeline.season_info_kv);
        if (qqSessionInfoJsonStrData!=null){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("qq_sessioninfo_animes");

                Document document=Document.parse(qqSessionInfoJsonStrData);
                document.put("_id",season_info_kv.get(QqConstant.SEASON_ID));
                document.putAll(season_info_kv);
                document.remove(QqConstant.SEASON_ID);
                document.remove(QqConstant.COMMENT);
                logger.info("commit {} to queue",document.toJson());
                collection.replaceOne(Filters.eq("_id",season_info_kv.get(QqConstant.SEASON_ID)),document,updateOptions);
                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }

        }

    }
}