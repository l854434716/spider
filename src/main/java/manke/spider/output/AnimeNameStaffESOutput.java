package manke.spider.output;

import com.google.gson.Gson;
import manke.spider.model.es.AnimeNameStaffModel;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The type Anime name staff es output.
 */
public class AnimeNameStaffESOutput  extends   AbstractESRestOutput<AnimeNameStaffModel>{
    private  static Logger logger=LoggerFactory.getLogger(AnimeNameStaffESOutput.class);

    private   int    requestNumbers=0;

    private   int    bulkCommitSize=100;

    private BulkRequest bulkRequest=null;

    /**
     * Instantiates a new Anime name staff es output.
     *
     * @param restClientBuilder the rest client builder
     */
    public AnimeNameStaffESOutput(RestClientBuilder restClientBuilder) {
        super(restClientBuilder);
    }

    @Override
    public void output(AnimeNameStaffModel animeNameStaffModel) {
        if (requestNumbers<bulkCommitSize){
            if (bulkRequest==null)
                bulkRequest=new BulkRequest();
            bulkRequest.add(createIndexRequest(animeNameStaffModel));
            requestNumbers++;
        }else{

            if (requestNumbers>0){
                try {
                    BulkResponse  bulkResponse= blukData(bulkRequest);
                    if (bulkResponse.hasFailures())
                        logger.error("data commit respose is ",bulkResponse.buildFailureMessage());
                    else
                        logger.info("commit  data  status is ",bulkResponse.status().getStatus());
                } catch (IOException e) {
                    logger.error("commit data  error",e);
                }
                bulkRequest=null;
                requestNumbers=0;
            }else{
                logger.info("there  is  no  data  to  commit");
            }

        }
    }


    private final Gson  gson= new  Gson();
    /**
     * create a .
     *
     * @param animeNameStaffModel the AnimeNameStaffModel
     */
    private IndexRequest createIndexRequest(AnimeNameStaffModel animeNameStaffModel){
        return Requests
                .indexRequest("anime_name_staff")
                .id(animeNameStaffModel.getSeason_id())
                .type("AnimeNameStaffModel")
                .opType(DocWriteRequest.OpType.INDEX)
                .source(gson.toJson(animeNameStaffModel),XContentType.JSON);

    }

    @Override
    public void context(Properties properties) {
        if (properties!=null)
        bulkCommitSize=NumberUtils.toInt(properties.getProperty("bulkCommitSize"),100);
    }
}
