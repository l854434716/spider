package manke.spider.pipeline;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luozhi on 2017/5/25.
 *
 * 处理番剧详情页的数据
 */
public class BibiAnimeDetailPipeline extends AbstractMongodbPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(BibiAnimeDetailPipeline.class);


    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        if (resultItems.get("bangumi_preview")!=null){
            MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
            MongoCollection<Document> collection=mongoDatabase.getCollection("bibi_detail_animes");
            Document document=new Document();
            document.put("bangumi_preview","http:"+resultItems.get("bangumi_preview"));
            document.put("info-title",resultItems.get("info-title"));
            document.put("info-style-items",resultItems.get("info-style-items"));
            document.put("playCount",resultItems.get("playCount"));
            document.put("fans",resultItems.get("fans"));
            document.put("reviewCount",resultItems.get("reviewCount"));
            document.put("info_update_time",resultItems.get("info_update_time"));
            document.put("info_update_statue",resultItems.get("info_update_statue"));
            document.put("info_cvList",resultItems.get("info_cvList"));
            document.put("info_desc",resultItems.get("info_desc"));
            collection.insertOne(document);
            logger.info(document.toString());
        }

    }
}
