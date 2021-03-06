package manke.spider.pipeline.bibi;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.pipeline.AbstractMongodbPipeline;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.List;

/**
 * Created by luozhi on 2017/5/22.
 */
public class BibiAnimeShortCommentPipeline extends AbstractMongodbPipeline implements Pipeline {
    private  static Logger logger= LoggerFactory.getLogger(BibiAnimeShortCommentPipeline.class);

    private UpdateOptions updateOptions=new UpdateOptions().upsert(true);

    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        List<String> comments=resultItems.get("comments");
        String  commentDetail= resultItems.get("commentdetail");
        String media_id= resultItems.get("media_id");
        String total= resultItems.get("total");
        String folded_count= resultItems.get("folded_count");

        if (comments!=null){

            try{
                MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
                MongoCollection<Document> collection=mongoDatabase.getCollection("bibi_animes_short_comment_info");


                Document document=null;
                for (String  comment:comments){
                    document = Document.parse(comment);
                    document.put("_id", StringUtils.join(media_id,document.get("review_id")));
                    logger.debug("commit {} to queue",document.toJson());
                    collection.replaceOne(Filters.eq("_id",document.get("_id")),document,updateOptions);
                    logger.info("commit {} success",document.toJson());
                }


            }catch (Exception e){
                logger.error("store data error ",e);

            }

        }

    }
}