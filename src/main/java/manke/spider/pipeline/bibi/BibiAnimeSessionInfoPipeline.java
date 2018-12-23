package manke.spider.pipeline.bibi;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.model.bibi.BibiConstant;
import manke.spider.mongo.MongoHelper;
import manke.spider.pipeline.AbstractMongodbPipeline;
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

    private UpdateOptions updateOptions=new UpdateOptions().upsert(true);

    //番剧sessionInfo json存放到 resultItems  中的key 名称
    public  final static  String  bibiSessionInfoJsonStr="data";

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        String  bibiSessionInfoJsonStrData=resultItems.get(bibiSessionInfoJsonStr);

        if (bibiSessionInfoJsonStrData!=null){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("bibi_sessioninfo_animes_v2");

                Document document=Document.parse(bibiSessionInfoJsonStrData);
                document.put("_id", MongoHelper.getDocumentValue(document,"mediaInfo.seasons[0].season_id",Integer.class));
                document.remove("ver");
                document.remove("loginInfo");
                document.remove("userStatus");
                logger.info("commit {} to queue",document.toJson());
                //collection.replaceOne(Filters.eq("_id",document.get(BibiConstant.SEASON_ID)),document,updateOptions);
                logger.info("commit data success");
            }catch (Exception e){
                logger.error("store data error ",e);

            }


        }

    }
}