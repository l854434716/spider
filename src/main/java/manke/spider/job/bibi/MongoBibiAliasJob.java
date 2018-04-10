package manke.spider.job.bibi;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.bibi.MongoBibiAliasInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.FileDataOutput;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

/**
 * Created by LENOVO on 2018/3/21.
 */
public class MongoBibiAliasJob extends AbstractJob<FindIterable<Document>,String> {


    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String  outPutResult=null;
        String[] aliasList=null;
        while(resultCursor.hasNext()){

            Document document= resultCursor.next();

            String alias=document.getString("alias");
            if (StringUtils.isEmpty(alias)){
                continue;
            }
            aliasList=StringUtils.split(alias,',');

            for(String a:aliasList){
                outPutResult=StringUtils.join(document.getString("season_id")+","+a);
                dataOutput.output(outPutResult);
            }


        }



    }



    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput();
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiAliasInput mongoBibiAliasInput=
                new MongoBibiAliasInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes"));
        JobFactory
                .createJob(MongoBibiAliasJob.class,mongoBibiAliasInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }


}
