package manke.spider.Job;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.DataInput;
import manke.spider.input.MongoBibiAcotorsInput;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.ConsoleDataOutput;
import manke.spider.output.FileDataOutput;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiActorsJob extends  AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String  outPutResult=null;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            ArrayList<Object> actors=document.get("actor", ArrayList.class);

            for(Object object:actors){

                Document actor= (Document) object;
                //System.out.println(actor.getString("actor")+" role as "+actor.getString("role")+"anime id is "+d.getString("season_id"));
                outPutResult=StringUtils.join(actor.getString("actor"),","+actor.getString("role")+","+document.getString("season_id"));
                dataOutput.output(outPutResult);
            }
        }



    }



    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiAcotorsInput mongoBibiAcotorsInput=
                new MongoBibiAcotorsInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes"));
        JobFactory
                .createJob(MongoBibiActorsJob.class,mongoBibiAcotorsInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
