package manke.spider.job.qq;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.qq.MongoQqAcotorsInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.FileDataOutput;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoQqActorsJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String  outPutResult=null;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            ArrayList<Object> nam=document.get("nam", ArrayList.class);

            if (nam!=null&&nam.size()>0){

                ArrayList<String> actors= (ArrayList<String>) nam.get(0);

                for (String actor:actors ){

                    if (StringUtils.contains(actor,";")){

                        for(String _a:StringUtils.split(actor,';')){
                            outPutResult=StringUtils.join(_a,",",document.getString("_id"));
                            dataOutput.output(outPutResult);
                        }
                    }else{

                        outPutResult=StringUtils.join(actor,",",document.getString("_id"));
                        dataOutput.output(outPutResult);
                    }


                }

            }

        }



    }



    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoQqAcotorsInput mongoQqAcotorsInput=
                new MongoQqAcotorsInput(mongoClient.getDatabase("spider").getCollection("qq_sessioninfo_animes"));
        JobFactory
                .createJob(MongoQqActorsJob.class,mongoQqAcotorsInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
