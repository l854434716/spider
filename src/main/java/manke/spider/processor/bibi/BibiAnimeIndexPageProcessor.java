package manke.spider.processor.bibi;

import manke.spider.pipeline.bibi.BibiAnimeIndexPipeline;
import manke.spider.pipeline.bibi.BibiAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luozhi on 2017/5/21.
 *  抓取全量的番剧数据
 * 从 3种url 获取 信息  1.http://bangumi.bilibili.com/web_api/season/index_global?page=0   国外番剧集
 *                   2.https://bangumi.bilibili.com/web_api/season/index_cn?page=0      国内番剧集
 *                   3.https://bangumi.bilibili.com/jsonp/seasoninfo/5050.ver?callback=seasonListCallback  番剧详细信息
 */
public class BibiAnimeIndexPageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeIndexPageProcessor.class);


    public void process(Page page) {

        //http://bangumi.bilibili.com/web_api/season/index_global?page=0 索引页数据
        if(StringUtils.contains(page.getRequest().getUrl(),"index_global")){

            List<String> bibiIndexGlobalSeasonJsonStrList=null;

            try {
                //数据转换出错或者数据来源url 是其他页面
                bibiIndexGlobalSeasonJsonStrList=page.getJson().jsonPath("$.result.list[*]").all();
                page.putField(BibiAnimeIndexPipeline.bibiIndexGlobalSeasonJsonStrList,bibiIndexGlobalSeasonJsonStrList);
                List<String>  sessionIds=page.getJson().jsonPath("$.result.list[*].season_id").all();
                List<String>  animeDetailUrls=convertSessionIds2Urls(sessionIds);
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
                List<String>  sessionIds=page.getJson().jsonPath("$.result.list[*].season_id").all();
                List<String> animeDetailUrls=convertSessionIds2Urls(sessionIds);
                if (animeDetailUrls!=null)
                    page.addTargetRequests(animeDetailUrls);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


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


    public static void main(String[] args) {
        Spider.create(new BibiAnimeIndexPageProcessor())
                .addUrl("http://bangumi.bilibili.com/web_api/season/index_global?page=0")
                .addUrl("http://bangumi.bilibili.com/web_api/season/index_cn?page=0")
                .addPipeline(new BibiAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
