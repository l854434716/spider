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
import us.codecraft.webmagic.selector.Selectable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luozhi on 2017/5/21.
 *  抓取全量的番剧数据
 * 从 3种url 获取 信息  1.https://bangumi.bilibili.com/media/web_api/search/result?page=0&season_type=1&pagesize=20   国外番剧集
 *                   2.https://bangumi.bilibili.com/media/web_api/search/result?page=1&season_type=4&pagesize=20      国内番剧集
 *                   3.https://www.bilibili.com/bangumi/media/md102392  番剧详情
 */
public class BibiAnimeIndexPageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeIndexPageProcessor.class);

    //番剧列表url 前缀
    private   static   final   String   prefixAnimeListUrl="https://bangumi.bilibili.com/media/web_api/search/result?page=";

    //番剧列表url 后缀 国外
    private   static   final   String   suffixAbroadAnimeListUrl="&season_type=1&pagesize=20";

    //番剧列表url 后缀 国内
    private   static   final   String   suffixChinaAnimeListUrl="&season_type=4&pagesize=20";

    //番剧详情 url
    private   static   final  String    prefixAnimeDetailUrl="https://www.bilibili.com/bangumi/media/md";


    private   static   final  String    contentRepleas=";(function(){var s;(s=document.currentScript||document.scripts[document.scripts.length-1]).parentNode.removeChild(s);}());";

    public void process(Page page) {

        //番剧详情
        if(!StringUtils.contains(page.getRequest().getUrl(),"web_api")){

            try {
                //数据转换出错或者数据来源url 是其他页面
               Element body= page.getHtml().getDocument().body();

               Elements bodyElements = body.children();

               String  data=null;
                for (Element  element:bodyElements){
                    if (StringUtils.contains(element.html(),"__INITIAL_STATE__")){
                        //获取页面番剧详情数据
                        data=element.html();
                        data=StringUtils.replace(data,contentRepleas,"");
                        data=StringUtils.replace(data,"window.__INITIAL_STATE__=","");

                        page.putField("data",data);
                    }
                }

            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }else{//番剧列表

            List<String> datas = page.getJson().jsonPath("$.result.data[*].media_id").all();

            String  detailUrl=null;
            for (String  media_id:datas){


                detailUrl=StringUtils.join(prefixAnimeDetailUrl,media_id);
                page.addTargetRequest(detailUrl);

                logger.info("add  {}  detail url",detailUrl);

            }

            String  currentPage="";

            String  nextPage=null;

            String   nextPageUrl=null;
            if (datas.size()>0){

                currentPage=page.getJson().jsonPath("$.result.page.num").get();

                nextPage=Integer.parseInt(currentPage)+1+"";

                logger.info("spider  currentPage is {}  next page  is {}",currentPage,nextPage);

                if (StringUtils.contains(page.getRequest().getUrl(),suffixChinaAnimeListUrl)){

                    nextPageUrl=StringUtils.join(prefixAnimeListUrl,nextPage,suffixChinaAnimeListUrl);

                }else {

                    nextPageUrl=StringUtils.join(prefixAnimeListUrl,nextPage,suffixAbroadAnimeListUrl);
                }

                logger.info("next anime_ page  is {}",nextPageUrl);

                page.addTargetRequest(nextPageUrl);
            }


        }


    }





    public static void main(String[] args) {
        Spider.create(new BibiAnimeIndexPageProcessor())
                //.addUrl("https://bangumi.bilibili.com/media/web_api/search/result?page=0&season_type=1&pagesize=20")
                //.addUrl("https://bangumi.bilibili.com/media/web_api/search/result?page=0&season_type=4&pagesize=20")
                .addUrl("https://www.bilibili.com/bangumi/media/md5286")
                .addPipeline(new BibiAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
