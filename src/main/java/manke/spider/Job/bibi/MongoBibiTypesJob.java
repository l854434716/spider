package manke.spider.Job.bibi;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.Job.AbstractJob;
import manke.spider.Job.JobFactory;
import manke.spider.input.bibi.MongoBibiTypesInput;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.FileDataOutput;
import manke.spider.transform.AnimeTypeTransform;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiTypesJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String  outPutResult=null;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            ArrayList<Object> tags=document.get("tags", ArrayList.class);
            String tagName=null;
            String role=null;
            for(Object object:tags){

                Document tag= (Document) object;
                tagName=tag.getString("tag_name");
                if(StringUtils.isEmpty(tagName)){
                    break;
                }
                outPutResult=StringUtils.join(document.getString("season_id")+","+ AnimeTypeTransform.getTypeCodeByName(tagName));
                dataOutput.output(outPutResult);
            }
        }



    }



    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiTypesInput mongoBibiTypesInput=
                new MongoBibiTypesInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes"));
        JobFactory
                .createJob(MongoBibiTypesJob.class,mongoBibiTypesInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
