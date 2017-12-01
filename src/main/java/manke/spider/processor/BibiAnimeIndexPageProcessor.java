package manke.spider.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import manke.spider.model.BibiIndexGlobalSeason;
import manke.spider.pipeline.BibiAnimeDetailPipeline;
import manke.spider.pipeline.BibiAnimeIndexPipeline;
import manke.spider.pipeline.BibiAnimeRecommendPipeline;
import manke.spider.pipeline.BibiAnimeSessionInfoPipeline;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Html;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luozhi on 2017/5/21.
 *
 * 从 3种url 获取 信息  1.http://bangumi.bilibili.com/web_api/season/index_global?page=0
 *                   2.https://bangumi.bilibili.com/web_api/season/index_cn?page=0
 *                   3.https://bangumi.bilibili.com/jsonp/seasoninfo/5050.ver?callback=seasonListCallback
 */
public class BibiAnimeIndexPageProcessor implements PageProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeIndexPageProcessor.class);

    private Site site = Site.me()
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36")
            .setRetryTimes(3).setSleepTime(1000).setTimeOut(1000000000).setCharset("UTF-8");

    public void process(Page page) {

        //http://bangumi.bilibili.com/web_api/season/index_global?page=0 索引页数据
        if(StringUtils.contains(page.getRequest().getUrl(),"index_global")){

            List<String> bibiIndexGlobalSeasonJsonStrList=null;

            try {
                //数据转换出错或者数据来源url 是其他页面
                bibiIndexGlobalSeasonJsonStrList=page.getJson().jsonPath("$.result.list[*]").all();
                page.putField(BibiAnimeIndexPipeline.bibiIndexGlobalSeasonJsonStrList,bibiIndexGlobalSeasonJsonStrList);
                List<String>  animeDetailUrls=page.getJson().jsonPath("$.result.list[*].url").all();
                List<String>  sessionIds=page.getJson().jsonPath("$.result.list[*].season_id").all();
                animeDetailUrls=convertSessionIds2Urls(sessionIds);
                if (animeDetailUrls!=null)
                   page.addTargetRequests(animeDetailUrls);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


        //https://bangumi.bilibili.com/web_api/season/index_cn?page=0  国漫索引页数据
        if(StringUtils.contains(page.getRequest().getUrl(),"index_cn")){

            List<String> bibiIndexCnSeasonJsonStrList=null;

            try {
                //数据转换出错或者数据来源url 是其他页面
                bibiIndexCnSeasonJsonStrList=page.getJson().jsonPath("$.result.list[*]").all();
                page.putField(BibiAnimeIndexPipeline.bibiIndexGlobalSeasonJsonStrList,bibiIndexCnSeasonJsonStrList);
                List<String>  animeDetailUrls=page.getJson().jsonPath("$.result.list[*].url").all();
                List<String>  sessionIds=page.getJson().jsonPath("$.result.list[*].season_id").all();
                animeDetailUrls=convertSessionIds2Urls(sessionIds);
                if (animeDetailUrls!=null)
                    page.addTargetRequests(animeDetailUrls);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }



        //处理antime 详情页数据 http://bangumi.bilibili.com/anime/5056 详情页格式
        if (StringUtils.contains(page.getRequest().getUrl(),"anime")){

            Html htmlContent=page.getHtml();
            //bangumi-preview=//i0.hdslb.com/bfs/bangumi/8d594ce60af3068a182d9d498f317de7ff51668e.jpg_225x300.jpg
            String  bangumi_preview=htmlContent.xpath("//div[@class='bangumi-preview']/img/@src").get();
            page.putField("bangumi_preview",bangumi_preview);
            //info-title=少年阿贝 GO!GO!小芝麻 第二季
            String  info_tile=htmlContent.xpath("//div[@class='bangumi-info-r']/div[@class='b-head']/h1/text()").get();
            page.putField("info-title",info_tile);

            List<String> info_style_items=htmlContent.xpath("//span[@class='info-style-item']/text()").all();
            page.putField("info-style-items",info_style_items);

            String playCount=htmlContent.xpath("//span[@class='info-count-item info-count-item-play']/em/text()").get();
            String fans=htmlContent.xpath("//span[@class='info-count-item info-count-item-fans']/em/text()").get();
            String reviewCount=htmlContent.xpath("//span[@class='info-count-item info-count-item-review']/em/text()").get();

            page.putField("playCount",playCount);
            page.putField("fans",fans);
            page.putField("reviewCount",reviewCount);

            //开播时间
            String info_update_time=htmlContent.xpath("//div[@class='info-row info-update']/em/span[1]/text()").get();
            //是否完结
            String info_update_statue=htmlContent.xpath("//div[@class='info-row info-update']/em/span[2]/text()").get();


            page.putField("info_update_time",info_update_time);
            page.putField("info_update_statue",info_update_statue);


            List<String> info_cvList=htmlContent.xpath("//div[@class='info-row info-cv']/em/span/text()").all();

            page.putField("info_cvList",info_cvList);

            String  info_desc=htmlContent.xpath("//div[@class='info-desc']/text()").get();

            page.putField("info_desc",info_desc);


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
    // 根据session id  列表组装出对应该session 番剧的详细信息url 页面
    private  List<String>  convertSessionIds2Urls(List<String> sessionIds){

        if (sessionIds==null||sessionIds.size()==0){

            return  null;
        }
        String  url=null;
        List<String>  urls=new ArrayList<String>(sessionIds.size());
        for (String sessionId:sessionIds){

            url=StringUtils.join("http://bangumi.bilibili.com/jsonp/seasoninfo/",sessionId,".ver?callback=seasonListCallback");
            urls.add(url);
        }

        return urls;
    }


    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new BibiAnimeIndexPageProcessor())
                .addUrl("http://bangumi.bilibili.com/web_api/season/index_global?page=0")
                .addUrl("http://bangumi.bilibili.com/web_api/season/index_cn?page=0")
                .addPipeline(new BibiAnimeIndexPipeline())
                .addPipeline(new BibiAnimeSessionInfoPipeline())
                .thread(50).run();
    }
}
