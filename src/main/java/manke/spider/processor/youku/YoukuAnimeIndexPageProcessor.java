package manke.spider.processor.youku;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import manke.spider.pipeline.youku.YoukuAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Selectable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static manke.spider.model.youku.YoukuConstant.*;
/**
 * Created by luozhi on 2017/5/21.
 *  抓取全量的番剧数据
 * 从 3种url 获取 信息  1.https://list.youku.com/category/page?c=100&s=1&d=2&type=show&p=1  获取番剧信息
 *                    2.https://list.youku.com/show/id_z9a8e9ba0605611e2a19e.html  番剧详细信息
 *                   3.https://v.youku.com/v_show/id_XODU2MTEyNjI4.html 从该番剧播放url获取 番剧详细信息url
 *                    z9a8e9ba0605611e2a19e 表示番剧详情信息查询ID  优酷网番剧的唯一标识ID 有多种，这里使用番剧详情信息查询ID
 */
public class YoukuAnimeIndexPageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(YoukuAnimeIndexPageProcessor.class);

    private final static Map<String, JSONObject> seasonCache = new ConcurrentHashMap<>();

    private final static String INDEX_PAGE_PREFIX = "https://list.youku.com/category/page?c=100&s=1&d=2&type=show&p=";

    private final static AtomicInteger INDEX_CURRENT_PAGE = new AtomicInteger(2);

    public void process(Page page) {
        logger.info("get_page_data from url {}",page.getUrl().toString());
        //http://list.youku.com/category/show/c_100_s_1_d_2_p_1.html 索引页数据
        if(StringUtils.contains(page.getRequest().getUrl(),"category")){
            doIndexPageProcessor(page);
        }else if (StringUtils.contains(page.getRequest().getUrl(),"list")){
            /*http://list.youku.com/show/id_z9a8e9ba0605611e2a19e.html  番剧详细信息*/
            doDetailPageProcessor(page);
        }else{
            /*http://v.youku.com/v_show/id_XODU2MTEyNjI4.html 从该url获取 番剧详细信息url*/
            doPlayPageProcessor(page);
        }


    }


    private  void   doIndexPageProcessor(Page  page){
        List<String> season_li_list = null;
        JSONObject season_info_kv = null;
        String videoLink = null;
        try {

            season_li_list = page.getJson().jsonPath("$.data[*]").all();

            if (season_li_list != null && season_li_list.size() > 0) {
                for (String li : season_li_list) {
                    season_info_kv = JSON.parseObject(li);
                    //进入番剧播放页
                    videoLink = season_info_kv.getString("videoLink");
                    seasonCache.putIfAbsent(videoLink, season_info_kv);
                    page.addTargetRequest(videoLink);
                    logger.info("season play url is {}", videoLink);
                }
                //获取下一页url 继续爬取
                String nextPage = INDEX_CURRENT_PAGE.getAndAdd(1) + "";
                page.addTargetRequest(StringUtils.join(INDEX_PAGE_PREFIX, nextPage));
                logger.info("spider next page url is {}", StringUtils.join(INDEX_PAGE_PREFIX, nextPage));
            } else {
                logger.info("spider  to  last page...");
            }
            page.setSkip(true);

        }catch (Exception e){
            logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
        }
    }


    private  void   doDetailPageProcessor(Page  page){
        logger.info("doDetailPageProcessor  url is {}",page.getUrl().toString());
        String url = StringUtils.remove(page.getRequest().getUrl(), "https:");
        JSONObject seasonInfo = seasonCache.getOrDefault(url, new JSONObject());
        //番剧详情html div 模块
        Selectable detail_div_selectable= page.getHtml().xpath("//div[@class='mod mod-new']/div[@class='mod fix']");

        //番剧海报信息
        Selectable  detail_post_div_selectable=detail_div_selectable.xpath("div[@class='p-post']");
        //番剧文字信息
        Selectable  detail_base_div_selectable=detail_div_selectable.xpath("div[@class='p-base']");

        Map<String,String> sesaon_info_kv=new HashMap<>();

        sesaon_info_kv.put(SEASON_ID,page.getUrl().regex(".*id_(.*)\\.html.*").get());
        sesaon_info_kv.put(TITLE, seasonInfo.getString("title"));
        sesaon_info_kv.put(UPDATE_INFO, detail_base_div_selectable.xpath("//li[@class='p-row p-renew']/text()").get());
        sesaon_info_kv.put(WEBPLAYURL,detail_post_div_selectable.xpath("//div[@class='p-thumb']/a/@href").get());
        sesaon_info_kv.put(COVER,detail_post_div_selectable.xpath("//div[@class='p-thumb']/img/@src").get());
        sesaon_info_kv.put(EXCLUSIVE,detail_post_div_selectable.xpath("//span[@class='ico-lt']/text()").get());
        sesaon_info_kv.put(ARTICULATION,detail_post_div_selectable.xpath("//span[@class='ico-SD']/text()").get());
        sesaon_info_kv.put(MARK_V,detail_post_div_selectable.xpath("//span[@class='vip-free']/text()").get());

        //番剧文字信息列
        List<Selectable> detail_base_li_selectables=detail_base_div_selectable.xpath("ul/li").nodes();
        String liContent = null;
        for (Selectable li : detail_base_li_selectables) {
            liContent = li.xpath("li/text()").get();
            if (StringUtils.contains(li.get(), "别名")) {
                sesaon_info_kv.put(ALIAS, li.xpath("li/@title").get());
            }

            if (StringUtils.contains(li.xpath("//label/text()").get(), "上映")) {
                sesaon_info_kv.put(SCREEN_TIME, li.xpath("//span/text()").get());
            }

            if (StringUtils.contains(li.xpath("//label/text()").get(), "优酷开播")) {
                sesaon_info_kv.put(PUB_WEB, li.xpath("//span/text()").get());
            }

            if (StringUtils.contains(liContent, "评分")) {
                sesaon_info_kv.put(SCORE, li.xpath("span[@class='star-num']/text()").get());
            }

            if (StringUtils.contains(liContent, "适用年龄")) {
                sesaon_info_kv.put(RANGE_OF_APPLICATION, liContent);
            }

            if (StringUtils.contains(liContent, "导演")) {
                sesaon_info_kv.put(DIRECTOR, li.xpath("a/@title").get());
            }
            if (StringUtils.contains(liContent, "地区")) {
                sesaon_info_kv.put(REGION, li.xpath("a/text()").get());
            }
            if (StringUtils.contains(liContent, "类型")) {
                sesaon_info_kv.put(TYPES, StringUtils.join(li.xpath("a/text()").all(), "/"));
            }
            if (StringUtils.contains(liContent, "配音") || StringUtils.contains(li.xpath("li/text()").get(), "声优")) {
                sesaon_info_kv.put(DUBBEDS, StringUtils.join(li.xpath("a/text()").all(), "/"));
            }
            if (StringUtils.isNotEmpty(li.xpath("span[@class='intro-more hide']/text()").get())) {
                sesaon_info_kv.put(BRIEF, li.xpath("span[@class='intro-more hide']/text()").get());
            }

        }
        page.putField(YoukuAnimeSessionInfoPipeline.season_info_kv,sesaon_info_kv);



    }


    private  void   doPlayPageProcessor(Page  page){
        logger.info("doPlayPageProcessor  url is {}",page.getUrl().toString());
        try {

            String targetUrl = null;
            for (Selectable span : page.getHtml().xpath("//div[@class='title-wrap']/h1/span").nodes()) {
                targetUrl = span.xpath("a/@href").get();
                if (StringUtils.isNotEmpty(targetUrl)) {
                    break;
                }
            }
            if (page.getHtml().xpath("//div[@class='title-wrap']/h1/span").nodes().size() > 3) {
                logger.error("exception play page  url is {} find target url is {}", page.getRequest().getUrl(), targetUrl);
            }
            String url = StringUtils.remove(page.getRequest().getUrl(), "https:");
            if (targetUrl == null) {
               page.addTargetRequest(page.getUrl().toString());
           }else{
                seasonCache.putIfAbsent(targetUrl, seasonCache.get(url));
                seasonCache.remove(url);
                page.addTargetRequest(targetUrl);
                logger.info("{} get detail page url is {}", page.getRequest().getUrl(), targetUrl);
           }
            page.setSkip(true);
        }catch (Exception e){
            logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
        }
    }


    public static void main(String[] args) {
        Spider.create(new YoukuAnimeIndexPageProcessor())
                .addUrl("https://list.youku.com/category/page?c=100&s=1&d=2&type=show&p=1")
                //.addUrl("https://v.youku.com/v_show/id_XMjkyODAwNzIw.html")
                .addPipeline(new YoukuAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
