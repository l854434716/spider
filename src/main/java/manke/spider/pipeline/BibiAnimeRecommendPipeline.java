package manke.spider.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by luozhi on 2017/5/25.
 *
 * 处理番剧推荐的数据
 */
public class BibiAnimeRecommendPipeline implements Pipeline {

    private  static Logger logger= LoggerFactory.getLogger(BibiAnimeRecommendPipeline.class);


    public void process(ResultItems resultItems, Task task) {


        if (resultItems.isSkip())
            return;

        if (resultItems.get("bibiSeasonRecommandJsonStr")!=null){

            logger.info(resultItems.get("bibiSeasonRecommandJsonStr").toString());
        }

    }
}
