package manke.spider.job.youku;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.youku.MongoYoukuSeasonInfoInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.mongo.MongoHelper;
import manke.spider.output.FileDataOutput;
import manke.spider.transform.RegionTransform;
import manke.spider.transform.TextTransform;
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
public class MongoYoukuSeasonJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();


        String articulation=null;

        String badge=null;

        String  edition;

        String screen_time=null;

        String thumbs_up_num=null;

        String is_finish=null;

        float  score=0;

        String cover=null;

        String  title=null;

        String  exclusive=null;

        long  play_count=0;

        String pub_time=null;

        String  webplayurl=null;

        String  season_id=null;

        String  regionCode=null;

        List<Object>  results=new ArrayList<>();

        int limit_age_down;

        int  limit_age_up;
        String  range_of_application;
        String[]  range_of_applications;

        String comment_num;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            articulation=document.getString("articulation");
            results.add(articulation);
            badge=document.getString("mark_v");
            results.add(badge);
            edition=document.getString("edition");
            results.add(edition);
            screen_time=document.getString("screen_time");
            if (StringUtils.isEmpty(screen_time)){
                screen_time="3222-12-31";
            }
            results.add(screen_time);
            thumbs_up_num=document.getString("thumbs_up_num");
            thumbs_up_num=TextTransform.removeTextComma(TextTransform.gainStrAfterChColon(thumbs_up_num));
            if (!StringUtils.isNumeric(thumbs_up_num)){
                thumbs_up_num="0";
            }
            results.add(thumbs_up_num);
            is_finish= document.getString("update_info");
            results.add(TextTransform.isSeasonFinish(is_finish));
            score=NumberUtils.toFloat(document.getString("score"),0f);
            results.add(score);
            cover=document.getString("cover");
            results.add(cover);
            title=document.getString("title");
            results.add(title);
            exclusive=document.getString("exclusive");
            results.add(exclusive);
            play_count=NumberUtils.toLong(TextTransform.parsePlayCount(document.getString("play_count")));
            results.add(play_count);
            pub_time=document.getString("pub_web");
            if (StringUtils.isEmpty(pub_time)){
                pub_time="3222-12-31";
            }
            results.add(pub_time);
            webplayurl=document.getString("webplayurl");
            results.add(webplayurl);
            season_id=document.getString("_id");
            results.add(season_id);
            regionCode=RegionTransform.getRegionCodeByName(MongoHelper.getDocumentValue(document,"region",String.class));
            results.add(regionCode);

            range_of_application=document.getString("range_of_application");
            range_of_application=StringUtils.remove(range_of_application,"岁");
            range_of_application=StringUtils.substringAfter(range_of_application,"：");
            if (StringUtils.contains(range_of_application,"到")){
                range_of_applications=StringUtils.split(range_of_application,"到");
                if (StringUtils.isNumeric(range_of_applications[0]))
                    limit_age_down=Integer.parseInt(range_of_applications[0]);
                else
                    limit_age_down=0;
                if (StringUtils.isNumeric(range_of_applications[1]))
                    limit_age_up=Integer.parseInt(range_of_applications[1]);
                else
                    limit_age_up=0;
            }else if(StringUtils.contains(range_of_application,"以上")){
                limit_age_down=Integer.parseInt(StringUtils.substringBefore(range_of_application,"以上"));
                limit_age_up=0;
            }else{
                limit_age_down=0;
                limit_age_up=0;
            }

            results.add(limit_age_down);
            results.add(limit_age_up);

            comment_num=TextTransform.removeTextComma(TextTransform.gainStrAfterChColon(document.getString("comment_num")));

            if (!StringUtils.isNumeric(comment_num)){
                comment_num="0";
            }
            results.add(comment_num);
            dataOutput.output(StringUtils.join(results,"$"));
            results.clear();
        }



    }



    public  static void  main(String[] args){

        String outPutPath="/tmp/manke/youku_anime_season/"+DateFormatUtils.format(new Date(),"yyyy-MM-dd")+"/";
        String fileName="t_youku_anime_season_info.csv";
        FileDataOutput fileDataOutput=new FileDataOutput(outPutPath,fileName);
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoYoukuSeasonInfoInput mongoYoukuSeasonInfoInput=
                new MongoYoukuSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("youku_sessioninfo_animes"));
        JobFactory
                .createJob(MongoYoukuSeasonJob.class,mongoYoukuSeasonInfoInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
