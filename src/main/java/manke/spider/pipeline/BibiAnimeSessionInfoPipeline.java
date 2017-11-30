package manke.spider.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luozhi on 2017/5/22.
 */
public class BibiAnimeSessionInfoPipeline extends AbstractMongodbPipeline implements Pipeline {
    private  static Logger logger= LoggerFactory.getLogger(BibiAnimeSessionInfoPipeline.class);

    //番剧索引url 中所有的番剧列表数据存放到 resultItems  中的key 名称
    public  static  String  bibiSeasoninfoJsonStr="bibiSeasoninfoJsonStr";

    ObjectMapper mapper = new ObjectMapper();
    public void process(ResultItems resultItems, Task task) {

        if (resultItems.isSkip())
            return;

        // 处理番剧索引页数据  数据来源url 例如 http://bangumi.bilibili.com/web_api/season/index_global?page=0
        /*if (bibiIndexGlobalSeasonJsonStrList!=null){
            List<BibiIndexGlobalSeason> bibiIndexGlobalSeasonList=new ArrayList<BibiIndexGlobalSeason>();
            BibiIndexGlobalSeason  bibiIndexGlobalSeason=null;
            for (String  json:bibiIndexGlobalSeasonJsonStrList){
                try {
                    bibiIndexGlobalSeason=mapper.readValue(json,BibiIndexGlobalSeason.class);
                    bibiIndexGlobalSeasonList.add(bibiIndexGlobalSeason);
                    page.addTargetRequest(bibiIndexGlobalSeason.getUrl());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            page.putField("bibiIndexGlobalSeasonList",bibiIndexGlobalSeasonList);
        }*/

        List<String> bibiIndexGlobalSeasonJsonStrs=resultItems.get(bibiSeasoninfoJsonStr);

        if (bibiIndexGlobalSeasonJsonStrs!=null){

            MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
            MongoCollection<Document> collection=mongoDatabase.getCollection("bibi_index_animes");

            List<Document> docs= new ArrayList<Document>();
            Document document=null;
            Map<String,String>  bibiIndexGlobalSeasonMap=null;
            for (String  json:bibiIndexGlobalSeasonJsonStrs){
                try {
                    bibiIndexGlobalSeasonMap=mapper.readValue(json, HashMap.class);
                    document= new Document();
                    document.putAll(bibiIndexGlobalSeasonMap);
                    docs.add(document);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            collection.insertMany(docs);

        }

    }
}
