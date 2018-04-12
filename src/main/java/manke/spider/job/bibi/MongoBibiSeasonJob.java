package manke.spider.job.bibi;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.bibi.MongoBibiSeasonInfoInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.mongo.MongoHelper;
import manke.spider.output.FileDataOutput;
import manke.spider.transform.DateTransform;
import manke.spider.transform.RegionTransform;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiSeasonJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        int  allow_download=0;

        int  arealimit=0;

        String badge=null;

        String  copyright=null;

        int  coins=0;

        String cover=null;

        int danmaku_count=0;

        int  favorites=0;

        int  is_finish=0;

        double  score=0;

        int  score_critic_num=0;

        String  title=null;

        String  price="-1";

        long  play_count=0;

        String pub_time=null;

        String  bangumi_title=null;

        String  season_title=null;

        String  webplayurl=null;

        int  season_id=0;

        String  regionCode=null;

        List<Object>  results=new ArrayList<>();

        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            allow_download= NumberUtils.toInt(document.getString("allow_download"),0);

            results.add(allow_download);

            arealimit=NumberUtils.toInt(document.get("arealimit").toString(),0);

            results.add(arealimit);
            badge=document.getString("badge");
            results.add(badge);
            copyright=document.getString("copyright");
            results.add(copyright);
            coins=NumberUtils.toInt(document.getString("coins"),0);
            results.add(coins);
            cover=document.getString("cover");
            results.add(cover);
            danmaku_count=NumberUtils.toInt(document.getString("danmaku_count"),0);
            results.add(danmaku_count);
            favorites=NumberUtils.toInt(document.getString("favorites"),0);
            results.add(favorites);
            is_finish= NumberUtils.toInt(document.getString("is_finish"),0);
            results.add(is_finish);
            //
            score=MongoHelper.getDocumentValue(document,"media.rating.score",Double.class, (double) 0);

            results.add(score);
            score_critic_num=MongoHelper.getDocumentValue(document,"media.rating.count",Integer.class,0);
            results.add(score_critic_num);
            title=document.getString("title");
            results.add(title);
            price=MongoHelper.getDocumentValue(document,"payment.price",String.class,"0");
            results.add(price);
            play_count=NumberUtils.toLong(document.getString("play_count"),0);
            results.add(play_count);
            pub_time=document.getString("pub_time");
            results.add(pub_time);
            bangumi_title=document.getString("bangumi_title");
            results.add(bangumi_title);
            season_title=document.getString("season_title");
            results.add(season_title);
            webplayurl=document.getString("share_url");
            results.add(webplayurl);

            season_id=NumberUtils.toInt(document.getString("season_id"));
            results.add(season_id);
            if (!DateTransform.isPub_Time(pub_time)){

                System.out.println(season_id);
            }
            regionCode=RegionTransform.getRegionCodeByName(document.getString("area"));
            results.add(regionCode);
            dataOutput.output(StringUtils.join(results,"$"));
            results.clear();
        }



    }



    public  static void  main(String[] args){
        String outPutPath="/tmp/manke/bibi_anime_season/"+ DateFormatUtils.format(new Date(),"yyyy-MM-dd")+"/";
        String fileName="t_bibi_anime_season_info.csv";
        FileDataOutput fileDataOutput=new FileDataOutput(outPutPath,fileName);
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiSeasonInfoInput mongoBibiSeasonInfoInput=
                new MongoBibiSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes"));
        JobFactory
                .createJob(MongoBibiSeasonJob.class,mongoBibiSeasonInfoInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
