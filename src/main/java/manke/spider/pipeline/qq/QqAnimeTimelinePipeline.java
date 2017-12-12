package manke.spider.pipeline.qq;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.model.bibi.BibiConstant;
import manke.spider.pipeline.AbstractMongodbPipeline;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.List;

/**
 * Created by luozhi on 2017/5/22.
 * 处理将要更新的番剧数据
 */
public class QqAnimeTimelinePipeline extends AbstractMongodbPipeline implements Pipeline {
    private  static Logger logger= LoggerFactory.getLogger(QqAnimeTimelinePipeline.class);


    public final static  String  bibiTimelineLastDaySesssionArrayJsonStr="bibiTimelineLastDaySesssionArrayJsonStr";

    private  UpdateOptions updateOptions=new UpdateOptions().upsert(true);

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        List<String> sessions=resultItems.get(bibiTimelineLastDaySesssionArrayJsonStr);

        if (sessions!=null&&sessions.size()>0){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("bibi_timeline_animes");
                Document document=null;
                for (String  json:sessions){
                    document = Document.parse(json);
                    document.put("_id",document.get(BibiConstant.SEASON_ID));  //指定_id 为 bibi sessionid
                    document.put(BibiConstant.DATE_TS,resultItems.get(BibiConstant.DATE_TS));
                    document.put(BibiConstant.DAY_OF_WEEK,resultItems.get(BibiConstant.DAY_OF_WEEK));
                    logger.info("commit {} to queue",document.toJson());
                    collection.replaceOne(Filters.eq("_id",document.get(BibiConstant.SEASON_ID)),document,updateOptions);
                }

                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }

        }

    }
}
