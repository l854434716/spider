package manke.spider.pipeline.douban;

import com.alibaba.fastjson.JSON;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import manke.spider.model.douban.DoubanConstant;
import manke.spider.pipeline.AbstractMongodbPipeline;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Map;

public class DoubanAnimeSessionInfoPipeline extends AbstractMongodbPipeline implements Pipeline {

    private Logger logger = LoggerFactory.getLogger(DoubanAnimeSessionInfoPipeline.class);

    private UpdateOptions updateOptions = new UpdateOptions().upsert(true);


    @Override
    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        Map<String, Object> result = resultItems.get("result");
        if (result == null) {
            return;
        }

        if (StringUtils.equals(resultItems.get(DoubanConstant.BIZKEY), DoubanConstant.SEASONINFO)) {
            Document document = new Document();
            document.putAll(result);
            document.put("_id", document.getString("season_id"));

            save(document, "spider", "douban_sessioninfo_animes");

        }


        if (StringUtils.equals(resultItems.get(DoubanConstant.BIZKEY), DoubanConstant.CELEBRITIES)) {
            Document document = new Document();
            document.putAll(result);
            document.put("_id", document.getString("season_id"));

            save(document, "spider", "douban_session_celebrities");

        }


        if (StringUtils.equals(resultItems.get(DoubanConstant.BIZKEY), DoubanConstant.CELEBRITYINFO)) {

            Document document = new Document();
            document.putAll(result);
            document.put("_id", document.getString("celebrity_id"));

            save(document, "spider", "douban_celebrity_info");

        }


        if (StringUtils.equals(resultItems.get(DoubanConstant.BIZKEY), DoubanConstant.WORKS)) {

            Document document = Document.parse(JSON.toJSONString(result));
            document.put("_id", document.getString("celebrity_id"));
            save(document, "spider", "douban_celebrity_works");

        }


    }


    private void save(Document document, String dbName, String collectionName) {

        try {
            MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            logger.info("commit {} to queue", document.toJson());
            collection.replaceOne(Filters.eq("_id", document.getString("_id")), document, updateOptions);
            logger.info("commit data success");
        } catch (Exception e) {
            logger.error("store data error ", e);

        }
    }

}
