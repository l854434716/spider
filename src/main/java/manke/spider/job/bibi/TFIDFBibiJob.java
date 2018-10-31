package manke.spider.job.bibi;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import manke.spider.input.bibi.MongoBibiAcotorsInput;
import manke.spider.input.bibi.MongoBibiSeasonInfoInput;
import manke.spider.job.AbstractJob;
import manke.spider.job.JobFactory;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.output.FileDataOutput;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;

public class TFIDFBibiJob extends AbstractJob<FindIterable<Document>,String> {

    private   final  static    String    dataSeparate="|";
    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        ArrayList<Document>  raw_actors;
        String  staff=null;
        String  outPutResult=null;
        while(resultCursor.hasNext()){
            outPutResult=null;
            Document document= resultCursor.next();

            ArrayList<Object> tags=document.get("tags", ArrayList.class);
            String tagName=null;
            for(Object object:tags){

                Document tag= (Document) object;
                tagName=tag.getString("tag_name");
                if(StringUtils.isEmpty(tagName)){
                    break;
                }
                if (outPutResult!=null)
                    outPutResult=StringUtils.join(outPutResult,dataSeparate,tagName);
                else
                    outPutResult=tagName;
            }


            raw_actors=document.get("actor",ArrayList.class);
            for (Document  actor:raw_actors){
                if (outPutResult!=null)
                    outPutResult=StringUtils.join(outPutResult,dataSeparate,actor.getString("actor"));
                else
                    outPutResult=actor.getString("actor");
            }

            staff=StringUtils.replaceChars(document.getString("staff"),'\n',' ');

            if (outPutResult!=null)
                outPutResult=StringUtils.join(outPutResult,dataSeparate,staff);
            else
                outPutResult=staff;

            if (outPutResult!=null){
                outPutResult=StringUtils.join(document.getString("season_id"),"",outPutResult);
                dataOutput.output(outPutResult);

            }


        }

    }




    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput("/tmp/","tfidf.txt");
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiSeasonInfoInput mongoBibiSeasonInfoInput=
                new MongoBibiSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes"));
        JobFactory
                .createJob(TFIDFBibiJob.class,mongoBibiSeasonInfoInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }
}
