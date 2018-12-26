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
        String  raw_actors;
        ArrayList<AnimeActorModel>  actors;

        AnimeActorModel animeActorModel;
        Document document;
        String[] actorRoles;
        while(resultCursor.hasNext()){

            document= resultCursor.next();
            animeNameStaffModel=new AnimeNameStaffModel();

            animeNameStaffModel.setSeason_id(StringUtils.join(document.getInteger("_id"),"BI"));

            document=document.get("mediaInfo",Document.class);
            animeNameStaffModel.setTitle(document.getString("title"));

            animeNameStaffModel
                    .setAlias(Lists.newArrayList(StringUtils.split(document.getString("alias"),",")));



            raw_actors=document.getString("actors");
            actors=Lists.newArrayList();
            for (String  actorInfo:StringUtils.splitPreserveAllTokens(raw_actors,'\n')){
                animeActorModel= new AnimeActorModel();
                actorRoles=StringUtils.splitPreserveAllTokens(actorInfo,'：');
                if (actorRoles.length==2){
                    animeActorModel.setActorName(actorRoles[1]);
                    animeActorModel.setRole(actorRoles[0]);
                    actors.add(animeActorModel);
                }

            }

            animeNameStaffModel.setActors(actors);

            animeNameStaffModel
                    .setStaff(StringUtils.replaceChars(document.getString("staff"),'\n',' '));

            animeNameStaffModel.setOriginal_website("bilibili");
            dataOutput.output(animeNameStaffModel);


        }



    }



    public  static void  main(String[] args){
        RestClientBuilder restClientBuilder=RestClient
                .builder(new HttpHost("localhost",9200,"http"));

        DataOutput<AnimeNameStaffModel>  animeNameStaffESOutput=new AnimeNameStaffESOutput(restClientBuilder);
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiSeasonInfoInput mongoBibiSeasonInfoInput=
                new MongoBibiSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes_v2"));
        JobFactory
                .createJob(EsBibiSeasonJob.class,mongoBibiSeasonInfoInput,animeNameStaffESOutput,null).start();

        animeNameStaffESOutput.close();
    }


}
