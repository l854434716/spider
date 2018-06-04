package manke.spider.job.qq;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.qq.MongoQqTypesInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.FileDataOutput;
import manke.spider.transform.AnimeTypeTransform;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoQqTypesJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String  outPutResult=null;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            ArrayList<Object> typ=document.get("typ", ArrayList.class);
            String tagName=null;
            if (typ!=null&&typ.size()>1){
                if (typ.get(0) instanceof String){

                     System.out.println(((String) typ.get(0)).toString()+document.getString("_id"));
                }else{
                    ArrayList<String> types= (ArrayList<String>) typ.get(0);

                    for (String type:types ){

                        outPutResult=StringUtils.join(document.getString("_id"),",", AnimeTypeTransform.getTypeCodeByName(type));
                        dataOutput.output(outPutResult);
                    }


                }

            }


        }




    }



    public  static void  main(String[] args){

        String  basePath=System.getProperty("base.dir");
        String  path=basePath+"/data/season_type/qq/";
        FileDataOutput fileDataOutput=new FileDataOutput(path,"result.csv");
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoQqTypesInput mongoQqTypesInput=
                new MongoQqTypesInput(mongoClient.getDatabase("spider").getCollection("qq_sessioninfo_animes"));
        JobFactory
                .createJob(MongoQqTypesJob.class,mongoQqTypesInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
