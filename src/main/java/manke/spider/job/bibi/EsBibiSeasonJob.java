package manke.spider.job.bibi;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.bibi.MongoBibiSeasonInfoInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.model.es.AnimeActorModel;
import manke.spider.model.es.AnimeNameStaffModel;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.AnimeNameStaffESOutput;
import manke.spider.output.ConsoleDataOutput;
import manke.spider.output.DataOutput;
import manke.spider.output.FileDataOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.bson.Document;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;

/**
 * Created by luozhi on 2018/8/26.
 *
 * 抽取番剧文本信息到ES中
 */
public class EsBibiSeasonJob extends AbstractJob<FindIterable<Document>,AnimeNameStaffModel> {

    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();


        AnimeNameStaffModel  animeNameStaffModel;
        ArrayList<Document>  raw_actors;
        ArrayList<AnimeActorModel>  actors;

        AnimeActorModel animeActorModel;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();
            animeNameStaffModel=new AnimeNameStaffModel();

            animeNameStaffModel.setSeason_id(StringUtils.join(document.getString("season_id"),"BI"));

            animeNameStaffModel.setTitle(document.getString("title"));

            animeNameStaffModel
                    .setAlias(Lists.newArrayList(StringUtils.split(document.getString("alias"),",")));



            raw_actors=document.get("actor",ArrayList.class);
            actors=Lists.newArrayList();
            for (Document  actor:raw_actors){
                animeActorModel= new AnimeActorModel();
                animeActorModel.setActorName(actor.getString("actor"));
                animeActorModel.setRole(actor.getString("role"));
                actors.add(animeActorModel);
            }

            animeNameStaffModel.setActors(actors);

            animeNameStaffModel
                    .setStaff(StringUtils.replaceChars(document.getString("staff"),'\n',' '));


            dataOutput.output(animeNameStaffModel);


        }



    }



    public  static void  main(String[] args){
        RestClientBuilder restClientBuilder=RestClient
                .builder(new HttpHost("localhost",9200,"http"));

        DataOutput<AnimeNameStaffModel>  animeNameStaffESOutput=new AnimeNameStaffESOutput(restClientBuilder);
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiSeasonInfoInput mongoBibiSeasonInfoInput=
                new MongoBibiSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes"));
        JobFactory
                .createJob(EsBibiSeasonJob.class,mongoBibiSeasonInfoInput,animeNameStaffESOutput,null).start();

        animeNameStaffESOutput.close();
    }


}
