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

            document=MongoHelper.getDocumentValue(document,"mediaInfo",Document.class);

            allow_download= NumberUtils.toInt(document.getString("allow_download"),0);

            results.add(allow_download);

            arealimit=0;

            results.add(arealimit);
            badge=MongoHelper.getDocumentValue(document,"payment.tip",String.class);
            if (StringUtils.isNotEmpty(badge))
                badge="付费观看";
            results.add(badge);
            copyright=MongoHelper.getDocumentValue(document,"rights.copyright",String.class);
            results.add(copyright);
            coins=NumberUtils.toInt(document.getString("coins"),0);
            results.add(coins);
            cover=document.getString("cover");
            results.add(cover);
            danmaku_count=MongoHelper.getDocumentValue(document,"stat.danmakus",Integer.class);
            results.add(danmaku_count);
            favorites=MongoHelper.getDocumentValue(document,"stat.favorites",Integer.class);
            results.add(favorites);
            is_finish= MongoHelper.getDocumentValue(document,"copyright.is_finish",Integer.class);
            results.add(is_finish);
            //
            score=MongoHelper.getDocumentValue(document,"rating.score",Double.class, (double) 0);

            results.add(score);
            score_critic_num=MongoHelper.getDocumentValue(document,"rating.count",Integer.class,0);
            results.add(score_critic_num);
            title=document.getString("title");
            results.add(title);
            price=MongoHelper.getDocumentValue(document,"payment.price",String.class,"0");
            results.add(price);
            play_count=MongoHelper.getDocumentValue(document,"stat.views",Integer.class);
            results.add(play_count);
            pub_time=MongoHelper.getDocumentValue(document,"publish.pub_date",String.class);
            results.add(pub_time+" 00:00:00");
            bangumi_title=document.getString("bangumi_title");
            results.add(bangumi_title);
            season_title=document.getString("season_title");
            results.add(season_title);
            webplayurl=document.getString("share_url");
            results.add(webplayurl);

            season_id=MongoHelper.getDocumentValue(document,"param.season_id",Integer.class);
            results.add(season_id);
            if (!DateTransform.isPub_Time(pub_time)){

                System.out.println(season_id);
            }
            regionCode=RegionTransform.getRegionCodeByName(MongoHelper.getDocumentValue(document,"area[0].name",String.class));
            results.add(regionCode);
            dataOutput.output(StringUtils.join(results,"$"));
            results.clear();
        }



    }



    public  static void  main(String[] args){
        String outPutPath="/tmp/manke/bibi_anime_season/"+ DateFormatUtils.format(new Date(),"yyyy-MM-dd")+"/";
        String fileName="t_bibi_anime_season_info.csv";
        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiSeasonInfoInput mongoBibiSeasonInfoInput=
                new MongoBibiSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes_v2"));
        JobFactory
                .createJob(MongoBibiSeasonJob.class,mongoBibiSeasonInfoInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
