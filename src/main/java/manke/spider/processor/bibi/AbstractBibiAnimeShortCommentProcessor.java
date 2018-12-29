package manke.spider.processor.bibi;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.mongo.MongoHelper;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract   class AbstractBibiAnimeShortCommentProcessor extends AbstractPageProcessor {
    private    static   Logger  logger= LoggerFactory.getLogger(AbstractBibiAnimeShortCommentProcessor.class);

    private  static MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();

       final  static  String   commentURLPrefix="https://bangumi.bilibili.com/review/web_api/short/list?media_id=";

       final  static   String commentURLSuffix ="&folded=0&page_size=20&sort=1";

    public  static   List<String>  getCommentURls(){

        MongoCollection<Document> collection=mongoClient
                .getDatabase("spider").getCollection("bibi_sessioninfo_animes_v2");

        FindIterable<Document>  documents= collection
                .find().projection(Projections.fields(Projections.include("mediaInfo")));


        MongoCursor<Document> resultCursor=documents.batchSize(1000).iterator();


        List<String> urls=new ArrayList<String>();
        while (resultCursor.hasNext()){


            Document document= resultCursor.next();

            if (MongoHelper.getDocumentValue(document,"mediaInfo.media_id",Integer.class)!=null){
                urls.add(StringUtils.join(commentURLPrefix,MongoHelper.getDocumentValue(document,"mediaInfo.media_id",Integer.class), commentURLSuffix));
            }else{
                logger.error("data {} have no  media info",document.toJson());
            }
        }


        return  urls;
    }


}
