import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

/**
 * Created by luozhi on 2017/6/1.
 */
public class MongoDBJDBC {

    static  MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

    static {

        Runtime.getRuntime().addShutdownHook(new Thread(){

            @Override
            public void run() {
                if (mongoClient!=null){
                    mongoClient.close();
                }
            }
        });
    }

    static ObjectMapper mapper = new ObjectMapper();
    public static void main( String args[] ){
        /*try{

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
            System.out.println("Connect to database successfully");
            mongoDatabase.createCollection("bibi_index_animes");
            System.out.println("集合创建成功");
            mongoClient.close();
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }*/

        List<String>  types=new ArrayList<String>();
        List<String>  includeFidlds=new ArrayList<String>();
        //热血 后宫 校园  装逼  战斗 魔法  奇幻
        types.add("战斗");
        types.add("奇幻");
        types.add("热血");
        //types.add("装逼");
        includeFidlds.add("info-title");
        includeFidlds.add("info-style-items");
        includeFidlds.add("info_update_time");
        includeFidlds.add("info_desc");

        findAnime(types,includeFidlds,new Block<Document>() {
            public void apply(final Document document) {
                try {
                    Map<String,Object> result= mapper.readValue(document.toJson(), HashMap.class);
                    System.out.println(result.get("info-title")+":"+result.get("info-style-items")
                            +":"+result.get("info_update_time")+":"+result.get("info_desc"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        });
    }


    public  static  void  findAnime(List<String> types,List<String > includeFidlds,Block<Document> printBlock ) {

        MongoDatabase mongoDatabase = mongoClient.getDatabase("spider");
        MongoCollection<Document> collection = mongoDatabase.getCollection("bibi_detail_animes");

        if (printBlock == null) {
            printBlock = new Block<Document>() {
                public void apply(final Document document) {
                    System.out.println(document.toJson());
                }

            };


        }

        collection.find(all("info-style-items", types))
                .projection(fields(include(includeFidlds), excludeId()))
                .forEach(printBlock);
    }
}
