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
import manke.spider.mongo.MongoHelper;
import manke.spider.output.FileDataOutput;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TFIDFBibiJob extends AbstractJob<FindIterable<Document>,String> {

    private   final  static    String    dataSeparate=",";
    @Override
    public void etl() {

        FindIterable<Document> resultIter= dataInput.input();

        MongoCursor<Document> resultCursor=resultIter.batchSize(1000).iterator();

        String[] actorRoles;
        String  staff=null;
        String  outPutResult=null;
        String  actorName=null;
        Document document;

        String  raw_actors;
        while(resultCursor.hasNext()){
            outPutResult=null;
            document= resultCursor.next();

            ArrayList<Object> tags= MongoHelper.getDocumentValue(document,"mediaInfo.style",ArrayList.class);
            String tagName=null;
            for(Object object:tags){

                Document tag= (Document) object;
                tagName=tag.getString("name");
                if(StringUtils.isEmpty(tagName)){
                    break;
                }
                if (StringUtils.isNotEmpty(outPutResult))
                    outPutResult=StringUtils.join(outPutResult,dataSeparate,tagName);
                else
                    outPutResult=tagName;
            }


            raw_actors=MongoHelper.getDocumentValue(document,"mediaInfo.actors",String.class);

            for (String  actorInfo:StringUtils.splitPreserveAllTokens(raw_actors,'\n')){
                actorRoles=StringUtils.splitPreserveAllTokens(actorInfo,'：');

                if(actorRoles.length!=2){
                    continue;
                }
                actorName=StringUtils.remove(actorRoles[1],'\r');
                actorName=StringUtils.remove(actorName,'\n');
                actorName=StringUtils.replaceChars(actorName,'、',',');
                actorName=StringUtils.replaceChars(actorName,'，',',');

                if (StringUtils.isNotEmpty(outPutResult))
                    outPutResult=StringUtils
                            .join(outPutResult,dataSeparate,actorName);
                else
                    outPutResult=actorName;
            }
//  staff  save
            staff=StringUtils.remove(MongoHelper.getDocumentValue(document,"mediaInfo.staff",String.class),'\r');
            staff=StringUtils.replaceChars(staff,'\n','$');
            staff=StringUtils.replaceChars(staff,':','$');
            staff=StringUtils.replaceChars(staff,'：','$');
            staff=StringUtils.removeEnd(staff,"$");
            staff=StringUtils.replaceChars(staff,'、',',');
            staff=StringUtils.replaceChars(staff,'，',',');
            staff=StringUtils.replaceChars(staff,'；',',');
            if (StringUtils.isNotEmpty(staff)){
                String[]  content=StringUtils.split(staff,"$");
                String   _s=null;
                for(int i=0;i<content.length;i++){
                    if (i%2!=0){
                        if (_s!=null)
                            _s=StringUtils.join(_s,",",StringUtils.trim(content[i]));
                        else
                            _s=StringUtils.trim(content[i]);
                    }
                }

                if (StringUtils.contains(staff,"$"))
                    staff=_s;

                if (StringUtils.isNotEmpty(outPutResult))
                    outPutResult=StringUtils.join(outPutResult,dataSeparate,staff);
                else
                    outPutResult=staff;
            }

            if (StringUtils.isNotEmpty(outPutResult)){
                outPutResult=StringUtils.join(document.getInteger("_id"),"|",outPutResult);
                dataOutput.output(outPutResult);

            }


        }

    }


    public  static void  main(String[] args){

        FileDataOutput fileDataOutput=new FileDataOutput("/tmp/","tfidf.txt");
        MongoClient mongoClient= MongoClinetSingleton.getMongoClinetInstance();
        MongoBibiSeasonInfoInput mongoBibiSeasonInfoInput=
                new MongoBibiSeasonInfoInput(mongoClient.getDatabase("spider").getCollection("bibi_sessioninfo_animes_v2"));
        JobFactory
                .createJob(TFIDFBibiJob.class,mongoBibiSeasonInfoInput,fileDataOutput,null).start();

        fileDataOutput.close();
    }
}
