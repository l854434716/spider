package manke.spider.processor.bibi;

import com.mongodb.MongoClient;
import manke.spider.mongo.MongoClinetSingleton;
import manke.spider.pipeline.bibi.BibiAnimeFullCommentPipeline;
import manke.spider.pipeline.bibi.BibiAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Selectable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luozhi on 2018/11/11.
 *  抓取全量的番剧评论信息
 *  https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1
 *  https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1&cursor=1531848168000
 */
public class BibiAnimeFullCommentProcessor extends AbstractBibiAnimeCommentProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeFullCommentProcessor.class);

    public void process(Page page) {


        if(StringUtils.contains(page.getRequest().getUrl(),"list")){//评论列表

            try {
                doCommentListPage(page);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }else{//评论详情



        }

        //https://bangumi.bilibili.com/jsonp/seasoninfo/5050.ver?callback=seasonListCallback
        if(StringUtils.contains(page.getRequest().getUrl(),"seasonListCallback")){

            String  bibiSeasoninfoJsonStr=null;

            try {
                //数据转换出错或者数据来源url 是其他页面
                bibiSeasoninfoJsonStr=page.getJson().regex("result\":(.*)}\\)").toString();
                page.putField(BibiAnimeSessionInfoPipeline.bibiSessionInfoJsonStr,bibiSeasoninfoJsonStr);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


    }


    private      void     doCommentListPage(Page   page) {

        String url = page.getRequest().getUrl();
        String total_str = page.getJson().jsonPath("$.result.total").get();
        String folded_count_str = null;

        if (StringUtils.contains(url,"folded=0")){
            folded_count_str=page.getJson().jsonPath("$.result.folded_count").get();
            page.putField("is_folded",0);
        }else{
            page.putField("is_folded",1);
        }

        int folded_count = NumberUtils.toInt(folded_count_str, -1);//如果是折叠数据则折叠字段值为-1

        if (!StringUtils.contains(url,"cursor")&&folded_count!=-1){//爬取的首条url
            page.putField("total",total_str);
            page.putField("folded_count",folded_count_str);
        }

        String nextUrl = null;
        List<String> comments = page.getJson().jsonPath("$.result.list[*]").all();
        String media_id=StringUtils.substringBetween(url,"media_id=","&folded");

        if (comments.size() == 0) {

            if (StringUtils.contains(url, "folded=0") && folded_count > 0) {//正常评论已经爬完开始爬取被折叠的评论
                nextUrl = StringUtils.split(url, '&')[0] + commentURLSuffix1;
                logger.info("begin spider fold comment  previous url is {}  nextUrl is {}", url, nextUrl);
                page.addTargetRequest(nextUrl);
            }else{
                logger.info("url {} is  oldest ", url);
            }
        } else {
            page.putField("media_id",media_id);

            String cursor_str =page.getJson().jsonPath("$.result.list[(@.length-1)].cursor").get();
                    //.get(comments.size() - 1).jsonPath("$.cursor").get();


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

        Spider   spider= Spider.create(new BibiAnimeFullCommentProcessor())
                        .addPipeline(new BibiAnimeFullCommentPipeline());

        spider.addUrl("https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1");
        /*for (String  url:getCommentURls()){

            spider.addUrl(url);
        }*/

        spider.thread(1).run();

    }
}
