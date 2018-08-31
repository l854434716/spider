package manke.spider.output;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * The type Abstract es rest output.
 *
 * @param <T> the type parameter
 */
public  abstract  class AbstractESRestOutput<T> implements  DataOutput<T> {

    private  static Logger logger=LoggerFactory.getLogger(AbstractESRestOutput.class);
    private final RestHighLevelClient restClient;


    /**
     * Instantiates a new Abstract es rest output.
     *
     * @param restClientBuilder the rest client builder
     */
    protected AbstractESRestOutput(RestClientBuilder  restClientBuilder) {
        restClient = new  RestHighLevelClient(restClientBuilder);;
    }

    @Override
    public void close()  {
        if (restClient!=null) {
            try {
                restClient.close();
            } catch (IOException e) {
                logger.error("close  restClient  error",e);
            }
        }
        logger.info("restClient is  close");
    }


    /**
     * Bluk data bulk response.
     *
     * @param bulkRequest the bulk request
     * @return the bulk response
     * @throws IOException the io exception
     */
    BulkResponse   blukData(BulkRequest  bulkRequest) throws IOException {

        BulkResponse bulkResponse;
        bulkResponse= restClient.bulk(bulkRequest);

        return   bulkResponse;
    }
}
