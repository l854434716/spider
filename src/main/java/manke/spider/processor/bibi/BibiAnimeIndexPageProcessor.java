package manke.spider.processor.bibi;

import manke.spider.pipeline.bibi.BibiAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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
 * 从 3种url 获取 信息  1.https://bangumi.bilibili.com/media/web_api/search/result?page=0&season_type=1&pagesize=20   国外番剧集
 *                   2.https://bangumi.bilibili.com/media/web_api/search/result?page=1&season_type=1&pagesize=20      国内番剧集
 *                   3.https://www.bilibili.com/bangumi/media/md102392/   番剧详情
 */
public class BibiAnimeIndexPageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeIndexPageProcessor.class);


    public void process(Page page) {

        //番剧详情
        if(StringUtils.contains(page.getRequest().getUrl(),"bangumi")){

            try {
                //数据转换出错或者数据来源url 是其他页面
               Element body= page.getHtml().getDocument().body();

               Elements bodyElements = body.children();

               String  data=null;
                for (Element  element:bodyElements){
                    if (StringUtils.contains(element.html(),"__INITIAL_STATE__")){
                        //获取页面番剧详情数据
                        data=element.html();
                        data=StringUtils.split(data,";")[0];
                        data=StringUtils.replace(data,"window.__INITIAL_STATE__=","");

                        page.putField("data",data);
                    }
                }

            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }else{//番剧列表


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
                //.addUrl("http://bangumi.bilibili.com/web_api/season/index_global?page=0")
                //.addUrl("http://bangumi.bilibili.com/web_api/season/index_cn?page=0")
                .addUrl("https://www.bilibili.com/bangumi/media/md102392/")
                .addPipeline(new BibiAnimeSessionInfoPipeline())
                .thread(1).run();
    }
}
