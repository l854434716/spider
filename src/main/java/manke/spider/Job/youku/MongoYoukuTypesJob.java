package manke.spider.Job.youku;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.Job.AbstractJob;
import manke.spider.Job.JobFactory;
import manke.spider.input.youku.MongoYoukuTypesInput;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.FileDataOutput;
import manke.spider.transform.AnimeTypeTransform;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoYoukuTypesJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String  outPutResult=null;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            String types=document.getString("types");
            String[] typeArray=StringUtils.split(types,"/");

            for (String type:typeArray ){

                outPutResult=StringUtils.join(document.getString("_id"),",", AnimeTypeTransform.getTypeCodeByName(type));
                dataOutput.output(outPutResult);
            }


        }




    }



    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoYoukuTypesInput mongoYoukuTypesInput=
                new MongoYoukuTypesInput(mongoClient.getDatabase("spider").getCollection("youku_sessioninfo_animes"));
        JobFactory
                .createJob(MongoYoukuTypesJob.class,mongoYoukuTypesInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
