package manke.spider.pipeline;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luozhi on 2017/5/22.
 * 处理将要更新的番剧数据
 */
public class BibiAnimeTimelinePipeline extends AbstractMongodbPipeline implements Pipeline {
    private  static Logger logger= LoggerFactory.getLogger(BibiAnimeTimelinePipeline.class);


    public final static  String  bibiTimelineLastDaySesssionArrayJsonStr="bibiTimelineLastDaySesssionArrayJsonStr";

    public  final  static  String  date_ts="date_ts";

    public  final  static  String  day_of_week="day_of_week";

    public  final  static  String  season_id="season_id";


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
                    document.put("_id",document.get(season_id));  //指定_id 为 bibi sessionid
                    document.put(date_ts,resultItems.get(date_ts));
                    document.put(day_of_week,resultItems.get(day_of_week));
                    logger.info("commit {} to queue",document.toJson());
                    collection.updateOne(Filters.eq("_id",document.get(season_id)),document,updateOptions);
                }

                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }

        }

    }
}
