package manke.spider.job.qq;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.bibi.MongoBibiSeasonInfoInput;
import manke.spider.input.qq.MongoQqSeasonInfoInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.model.es.AnimeActorModel;
import manke.spider.model.es.AnimeNameStaffModel;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.AnimeNameStaffESOutput;
import manke.spider.output.DataOutput;
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
public class EsQqSeasonJob extends AbstractJob<FindIterable<Document>,AnimeNameStaffModel> {

    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();


        AnimeNameStaffModel  animeNameStaffModel;
        ArrayList<String>  raw_actors;
        ArrayList<AnimeActorModel>  actors;

        AnimeActorModel animeActorModel;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();
            animeNameStaffModel=new AnimeNameStaffModel();

            animeNameStaffModel.setSeason_id(document.getString("_id"));

            animeNameStaffModel.setTitle(document.get("c",Document.class).getString("title"));

            animeNameStaffModel
                    .setAlias(Lists.newArrayListWithCapacity(0));


            ArrayList<Object> nam=document.get("nam", ArrayList.class);
            actors=Lists.newArrayList();
            if (nam!=null&&nam.size()>0){

                raw_actors= (ArrayList<String>) nam.get(0);

                for (String actor:raw_actors ){

                    if (StringUtils.contains(actor,";")){

                        for(String _a:StringUtils.split(actor,';')){
                            animeActorModel= new AnimeActorModel();

                            animeActorModel.setActorName(_a);
                            actors.add(animeActorModel);
                        }
                    }else{
                        animeActorModel= new AnimeActorModel();
                        animeActorModel.setActorName(actor);
                        actors.add(animeActorModel);

                    }


                }

            }


            animeNameStaffModel.setActors(actors);

            animeNameStaffModel
                    .setStaff("");

            animeNameStaffModel.setOriginal_website("腾讯视频");
            dataOutput.output(animeNameStaffModel);


        }



    }



    public  static void  main(String[] args){
        RestClientBuilder restClientBuilder=RestClient
                .builder(new HttpHost("localhost",9200,"http"));

        DataOutput<AnimeNameStaffModel>  animeNameStaffESOutput=new AnimeNameStaffESOutput(restClientBuilder);
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoQqSeasonInfoInput mongoQqSeasonInfoInput=
                new MongoQqSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("qq_sessioninfo_animes"));
        JobFactory
                .createJob(EsQqSeasonJob.class,mongoQqSeasonInfoInput,animeNameStaffESOutput,null).start();

        animeNameStaffESOutput.close();
    }


}
