package manke.spider.Job.qq;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.Job.AbstractJob;
import manke.spider.Job.JobFactory;
import manke.spider.input.bibi.MongoBibiSeasonInfoInput;
import manke.spider.input.qq.MongoQqSeasonInfoInput;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.mongo.MongoHelper;
import manke.spider.output.FileDataOutput;
import manke.spider.transform.DateTransform;
import manke.spider.transform.RegionTransform;
import manke.spider.transform.TextTransform;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoQqSeasonJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();


        String badge=null;

        String cover=null;

        String  is_finish;

        float  score=0;

        String  title=null;

        long  play_count=0;

        String pub_time=null;

        String  webplayurl=null;

        int  season_id=0;

        String  regionCode=null;

        List<Object>  results=new ArrayList<>();

        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            badge=document.getString("mark_v");
            results.add(badge);


            cover=document.get("c",Document.class).getString("pic");
            results.add(cover);

            is_finish= document.getString("update_info");
            results.add(TextTransform.isSeasonFinish(is_finish));


            score=NumberUtils.toFloat(document.getString("score"),0f);

            results.add(score);
            title=document.get("c",Document.class).getString("title");
            results.add(title);

            play_count=NumberUtils.toLong(TextTransform.parsePlayCount(document.getString("play_count")));//1
            results.add(play_count);
            pub_time=document.get("c",Document.class).getString("year");
            results.add(pub_time);

            webplayurl=document.getString("webplayurl");
            results.add(webplayurl);

            season_id=NumberUtils.toInt(document.getString("_id"));
            results.add(season_id);
            regionCode=RegionTransform.getRegionCodeByName(MongoHelper.getDocumentValue(document,"typ[1]",String.class));
            results.add(regionCode);
            dataOutput.output(StringUtils.join(results,"$"));
            results.clear();
        }



    }



    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoQqSeasonInfoInput mongoQqSeasonInfoInput=
                new MongoQqSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("qq_sessioninfo_animes"));
        JobFactory
                .createJob(MongoQqSeasonJob.class,mongoQqSeasonInfoInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
