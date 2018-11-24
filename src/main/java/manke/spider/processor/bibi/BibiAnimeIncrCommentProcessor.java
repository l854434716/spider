package manke.spider.processor.bibi;

import manke.spider.pipeline.bibi.BibiAnimeFullCommentPipeline;
import manke.spider.pipeline.bibi.BibiAnimeSessionInfoPipeline;
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
 *  抓取增量的番剧评论信息
 *  https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1
 *  https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1&cursor=1531848168000
 */
public class BibiAnimeIncrCommentProcessor extends AbstractBibiAnimeCommentProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeIncrCommentProcessor.class);

    public void process(Page page) {


        if(StringUtils.contains(page.getRequest().getUrl(),"list")){//评论列表

            try {
                doCommentListPage(page);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }else{//评论详情

            try {
                doCommentDetailPage(page);
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
        String   detailCommentUrl=null;
        String total_str = page.getJson().jsonPath("$.result.total").get();
        String folded_count_str = null;
        List<String>  ctimes=null;
        int  stopIndex=-1;//增量爬取的截止索引
        if (StringUtils.contains(url,"folded=0")){//爬取未被折叠的数据
            comments = page.getJson().jsonPath("$.result.list[*]").all();
            if (comments.size()==0){
                folded_count_str=page.getJson().jsonPath("$.result.folded_count").get();
                if (NumberUtils.toInt(folded_count_str, -1)>0){//有必要爬取折叠数据
                    nextUrl = StringUtils.split(url, '&')[0] + commentURLSuffix1;
                    logger.info("begin spider_incr fold comment  previous url is {}  nextUrl is {}", url, nextUrl);
                    page.addTargetRequest(nextUrl);
                }else{
                    logger.info("url {} is  oldest_incr ", url);
                }
            }else{
                ctimes=page.getJson().jsonPath("$.result.list[*].ctime").all();
                for(String  ctime:ctimes){
                    if (determineCommentIsNewByCtime(ctime))  stopIndex++;
                }

                if (stopIndex==-1){//该页面所有数据已爬完,开始爬取折叠的数据
                    folded_count_str=page.getJson().jsonPath("$.result.folded_count").get();
                    if (NumberUtils.toInt(folded_count_str, -1)>0){//有必要爬取折叠数据
                        nextUrl = StringUtils.split(url, '&')[0] + commentURLSuffix1;
                        logger.info("begin spider_incr fold comment  previous url is {}  nextUrl is {}", url, nextUrl);
                        page.addTargetRequest(nextUrl);
                    }else{
                        logger.info("url {} is  oldest_incr ", url);
                    }
                }else if (stopIndex==comments.size()-1){//还需要继续爬取下一页
                    page.putField("media_id",media_id);
                    List<String>  reviewIds=page.getJson().jsonPath("$.result.list[*].review_id").all();
                    for (String   reviewId:reviewIds){
                        detailCommentUrl=createCommentDetailUrl(media_id,reviewId);
                        page.addTargetRequest(detailCommentUrl);
                        logger.info("spider  comment_detail_info  url {}",detailCommentUrl);
                    }
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
                    List<String>  reviewIds=page.getJson().jsonPath("$.result.list[:"+(stopIndex+1)+"].review_id").all();
                    for (String   reviewId:reviewIds){
                        detailCommentUrl=createCommentDetailUrl(media_id,reviewId);
                        page.addTargetRequest(detailCommentUrl);
                        logger.info("spider  comment_detail_info  url {}",detailCommentUrl);
                    }
                    comments=page.getJson().jsonPath("$.result.list[:"+(stopIndex+1)+"]").all();
                    page.putField("comments", comments);
                    logger.info("spider_incr  incr {} comments  from the url {}", comments.size(),url);

                    folded_count_str=page.getJson().jsonPath("$.result.folded_count").get();
                    if (NumberUtils.toInt(folded_count_str, -1)>0){//有必要爬取折叠数据
                        nextUrl = StringUtils.split(url, '&')[0] + commentURLSuffix1;
                        logger.info("begin spider_incr fold comment  previous url is {}  nextUrl is {}", url, nextUrl);
                        page.addTargetRequest(nextUrl);
                    }else{
                        logger.info("url {} is  oldest_incr ", url);
                    }
                }

            }


        }else{//爬取被折叠的数据

            comments = page.getJson().jsonPath("$.result.list[*]").all();
            if (comments.size()==0){
                logger.info("url {} is  oldest_incr ", url);
            }else{
                ctimes=page.getJson().jsonPath("$.result.list[*].ctime").all();
                for(String  ctime:ctimes){
                    if (determineCommentIsNewByCtime(ctime))  stopIndex++;
                }

                if (stopIndex==-1){//该页面所有数据已爬完
                    logger.info("url {}  have no increment_data");
                }else if (stopIndex==comments.size()-1){//还需要继续爬取下一页
                    page.putField("media_id",media_id);
                    List<String>  reviewIds=page.getJson().jsonPath("$.result.list[*].review_id").all();
                    for (String   reviewId:reviewIds){
                        detailCommentUrl=createCommentDetailUrl(media_id,reviewId);
                        page.addTargetRequest(detailCommentUrl);
                        logger.info("spider  comment_detail_info  url {}",detailCommentUrl);
                    }
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
                    List<String>  reviewIds=page.getJson().jsonPath("$.result.list[:"+(stopIndex+1)+"].review_id").all();
                    for (String   reviewId:reviewIds){
                        detailCommentUrl=createCommentDetailUrl(media_id,reviewId);
                        page.addTargetRequest(detailCommentUrl);
                        logger.info("spider  comment_detail_info  url {}",detailCommentUrl);
                    }
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


    private    void    doCommentDetailPage(Page   page){
        String   commentDetailRaw=
        page.getHtml().xpath("//div[@class='review-content']/text()").get();

        String  media_id=getMediaIdFromCommentDetailUrl(page.getRequest().getUrl());

        String  review_id=getReviewIdFromCommentDetailUrl(page.getRequest().getUrl());

        page.putField("media_id",media_id);
        page.putField("review_id",review_id);
        page.putField("commentdetail",commentDetailRaw);

    }
    public static void main(String[] args) {

        Spider   spider= Spider.create(new BibiAnimeIncrCommentProcessor())
                        .addPipeline(new BibiAnimeFullCommentPipeline());

        //spider.addUrl("https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1");
        //spider.addUrl("https://www.bilibili.com/bangumi/media/md102392/review/ld40967");
        //spider.addUrl("https://bangumi.bilibili.com/review/web_api/long/list?media_id=5997&folded=0&page_size=20&sort=1");
        for (String  url:getCommentURls()){
            spider.addUrl(url);
        }

        spider.thread(10).run();

    }
}
