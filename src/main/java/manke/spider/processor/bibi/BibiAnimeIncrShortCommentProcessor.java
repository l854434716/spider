package manke.spider.processor.bibi;

import manke.spider.pipeline.bibi.BibiAnimeShortCommentPipeline;
import manke.spider.transform.DateTransform;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Spider;

import java.util.List;

/**
 * Created by luozhi on 2018/11/11.
 *  抓取增量的番剧短评信息
 *  https://bangumi.bilibili.com/review/web_api/short/list?media_id=5997&folded=0&page_size=20&sort=1
 *  https://bangumi.bilibili.com/review/web_api/short/list?media_id=5997&folded=0&page_size=20&sort=1&cursor=1531848168000
 */
public class BibiAnimeIncrShortCommentProcessor extends AbstractBibiAnimeShortCommentProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeIncrShortCommentProcessor.class);

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
        String nextUrl = null;
        List<String> comments = null;
        String media_id=StringUtils.substringBetween(url,"media_id=","&folded");
        String total_str = page.getJson().jsonPath("$.result.total").get();
        List<String>  ctimes=null;
        int  stopIndex=-1;//增量爬取的截止索引
        if (StringUtils.contains(url,"folded=0")){//爬取未被折叠的数据
            comments = page.getJson().jsonPath("$.result.list[*]").all();
            if (comments.size()==0){
                logger.info("url {} is  oldest_incr ", url);
            }else{
                ctimes=page.getJson().jsonPath("$.result.list[*].ctime").all();
                for(String  ctime:ctimes){
                    if (determineCommentIsNewByCtime(ctime))  stopIndex++;
                }

                if (stopIndex==-1){//该页面所有数据已爬完
                    logger.info("url {} is  oldest_incr comments  have been stored", url);
                }else if (stopIndex==comments.size()-1){//还需要继续爬取下一页
                    page.putField("media_id",media_id);
                    String cursor_str =page.getJson().jsonPath("$.result.list[(@.length-1)].cursor").get();
                    if (StringUtils.contains(url, "cursor")) {
                        nextUrl = StringUtils.substringBeforeLast(url, "=")+"="+ cursor_str;
                    } else {
                        nextUrl = StringUtils.join(url, "&cursor=", cursor_str);
                    }

                    page.addTargetRequest(nextUrl);
                    logger.info("spider_incr  comment  next_url is {}", nextUrl);
                    page.putField("comments", comments);
                }else{//存储本页的部分数据
                    page.putField("media_id",media_id);
                    comments=page.getJson().jsonPath("$.result.list[:"+(stopIndex+1)+"]").all();
                    page.putField("comments", comments);
                    logger.info("spider_incr  incr {} comments  from the url {}", comments.size(),url);
                }

            }


        }
    }


    //判定当前评论是否是新增评论，策略是通过评论的ctime时间与前一天的0点0分0秒时间戳做对比
    private   static  final   long   currentTime=System.currentTimeMillis();
    private boolean determineCommentIsNewByCtime(String ctime) {

        return  DateTransform.getDayFirstTimeMills(currentTime,-1)<=NumberUtils.toLong(StringUtils.join(ctime,"000"));
    }


    public static void main(String[] args) {

        Spider   spider= Spider.create(new BibiAnimeIncrShortCommentProcessor())
                        .addPipeline(new BibiAnimeShortCommentPipeline());

        //spider.addUrl("https://bangumi.bilibili.com/review/web_api/short/list?media_id=3419&folded=0&page_size=20&sort=1");
        for (String  url:getCommentURls()){
            spider.addUrl(url);
        }

        spider.thread(10).run();

    }
}
