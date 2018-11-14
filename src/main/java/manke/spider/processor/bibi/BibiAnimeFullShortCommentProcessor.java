package manke.spider.processor.bibi;

import manke.spider.pipeline.bibi.BibiAnimeShortCommentPipeline;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Spider;

import java.util.List;

/**
 * Created by luozhi on 2018/11/11.
 *  抓取全量的番剧短评信息
 *  https://bangumi.bilibili.com/review/web_api/short/list?media_id=5997&folded=0&page_size=20&sort=1
 *  https://bangumi.bilibili.com/review/web_api/short/list?media_id=5997&folded=0&page_size=20&sort=1&cursor=1531848168000
 */
public class BibiAnimeFullShortCommentProcessor extends AbstractBibiAnimeShortCommentProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeFullShortCommentProcessor.class);

    public void process(Page page) {


        if(StringUtils.contains(page.getRequest().getUrl(),"list")){//评论列表

            try {
                doCommentListPage(page);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


    }


    //处理评论列表数据
    private      void     doCommentListPage(Page   page) {

        String url = page.getRequest().getUrl();
        String total_str = page.getJson().jsonPath("$.result.total").get();



        if (!StringUtils.contains(url,"cursor")){//爬取的首条url
            page.putField("total",total_str);
        }

        String nextUrl = null;
        List<String> comments = page.getJson().jsonPath("$.result.list[*]").all();
        String media_id=StringUtils.substringBetween(url,"media_id=","&folded");

        if (comments.size() == 0) {

            logger.info("url {} is  oldest ", url);

        } else {
            page.putField("media_id",media_id);

            String cursor_str =page.getJson().jsonPath("$.result.list[(@.length-1)].cursor").get();
            if (StringUtils.contains(url, "cursor")) {
                nextUrl = StringUtils.substringBeforeLast(url, "=")+"="+ cursor_str;
            } else {
                nextUrl = StringUtils.join(url, "&cursor=", cursor_str);
            }
            page.addTargetRequest(nextUrl);
            logger.info("spider  comment  next_url is {}", nextUrl);
            page.putField("comments", comments);

        }
    }


    public static void main(String[] args) {

        Spider   spider= Spider.create(new BibiAnimeFullShortCommentProcessor())
                        .addPipeline(new BibiAnimeShortCommentPipeline());
        //spider.addUrl("https://bangumi.bilibili.com/review/web_api/short/list?media_id=3419&folded=0&page_size=20&sort=1");

        for (String  url:getCommentURls()){

            spider.addUrl(url);

        }

        spider.thread(10).run();

    }
}
