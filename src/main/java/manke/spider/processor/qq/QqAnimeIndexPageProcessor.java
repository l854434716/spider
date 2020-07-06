package manke.spider.processor.qq;

import manke.spider.model.qq.QqConstant;
import manke.spider.pipeline.qq.QqAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Selectable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by luozhi on 2017/5/21.
 *  抓取全量的番剧数据
 * 从 3种url 获取 信息  1.http://v.qq.com/x/list/cartoon?offset=0  番剧集
 *                    2.http://node.video.qq.com/x/api/float_vinfo2?cid=p0pcfbdk318ry3m  番剧详细信息
 *                    p0pcfbdk318ry3m 表示番剧ID
 */
public class QqAnimeIndexPageProcessor extends AbstractPageProcessor {
    Logger logger = LoggerFactory.getLogger(QqAnimeIndexPageProcessor.class);

    //缓存从番剧list页面爬取下来的番剧信息，供保存番剧详情信息的pipeline使用  k 为番剧ID v 为番剧属性和值
    private final static Map<String, Map<String, String>> seasonCache = new ConcurrentHashMap<>();

    //番剧详情json信息url 前缀
    private final String preUrlDetailSeason = "http://node.video.qq.com/x/api/float_vinfo2?cid=";

    private static int offset = 0;


    public void process(Page page) {

        //http://v.qq.com/x/list/cartoon?offset=0 索引页数据
        if (StringUtils.contains(page.getRequest().getUrl(), "offset")) {

            List<Selectable> season_div_list = null;
            Map<String, String> season_info_kv = null;

            try {
                //数据转换出错或者数据来源url 是其他页面
                season_div_list = page.getHtml().xpath("//div[@class='list_item']").nodes();

                if (season_div_list != null && season_div_list.size() > 0) {

                    for (Selectable div : season_div_list) {
                        season_info_kv = new HashedMap();
                        season_info_kv.put(QqConstant.WEBPLAYURL, div.xpath("a/@href").get());
                        season_info_kv.put(QqConstant.SEASON_ID, div.xpath("a/@data-float").get());
                        season_info_kv.put(QqConstant.COVER, div.xpath("a/img/@src").get());
                        season_info_kv.put(QqConstant.UPDATE_INFO, div.xpath("div[@class='figure_caption']/text()").get());
                        season_info_kv.put(QqConstant.MARK_V, div.xpath("a/img[@class='mark_v mark_v_VIP']").get() == null ? "0" : "1");
                        //season_info_kv.put(QqConstant.PLAY_COUNT, div.xpath("//span[@class='num']/text()").get());

                        seasonCache.put(season_info_kv.get(QqConstant.SEASON_ID), season_info_kv);
                        logger.info("put  {}  {} to seasonCache ", season_info_kv.get(QqConstant.SEASON_ID), season_info_kv);

                        //抓取番剧评分与播放量
                        //page.addTargetRequest(StringUtils.join("https://v.qq.com/detail/k/", div.xpath("a/@data-float").get(), ".html"));
                        page.addTargetRequest(div.xpath("a/@href").get());

                    }

                    //获取下一页url 继续爬取
                    String nextPageUrlOffset = buildNextPageUrl();
                    page.addTargetRequest(nextPageUrlOffset);
                    logger.info("spider next page url is {}", nextPageUrlOffset);
                } else {
                    logger.info("spider  to  last page...");
                }

            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }

        //https://v.qq.com/detail/k/p0pcfbdk318ry3m.html
        if (StringUtils.contains(page.getRequest().getUrl(), "/detail/k/")) {

            String seasonId = StringUtils.substringBetween(page.getRequest().getUrl(), "https://v.qq.com/detail/k/", ".html");
            String score = page.getHtml().xpath("//div[@class='score_v']/span[@class='score']/text()").get();

            Map<String, String> season_info_kv = seasonCache.get(seasonId);
            season_info_kv.put(QqConstant.SCORE, score);
            //爬取详细信息
            page.addTargetRequest(preUrlDetailSeason + seasonId);

        }

        //https://v.qq.com/x/cover/m441e3rjq9kwpsc.html
        if (StringUtils.contains(page.getRequest().getUrl(), "cover")) {

            String seasonId = StringUtils.substringBetween(page.getRequest().getUrl(), "https://v.qq.com/x/cover/", ".html");

            Map<String, String> season_info_kv = seasonCache.get(seasonId);
            season_info_kv.put(QqConstant.PLAY_COUNT, page.getHtml().xpath("//em[@id='mod_cover_playnum']/text()").get());

            Selectable span = page.getHtml().xpath("//span[@class='video_score']");

            String score1 = span.xpath("//span[@class='units']/text()").get();

            String score2 = span.xpath("//span[@class='decimal']/text()").get();

            season_info_kv.put(QqConstant.SCORE, score1 + score2);

            //爬取详细信息
            page.addTargetRequest(preUrlDetailSeason + seasonId);

        }


        //http://node.video.qq.com/x/api/float_vinfo2?cid=p0pcfbdk318ry3m
        if (StringUtils.contains(page.getRequest().getUrl(), "float_vinfo2")) {

            String qqSeasoninfoJsonStr = null;
            String seasonId = null;
            Map<String, String> season_info_kv = null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                seasonId=page.getUrl().regex(".*cid=(.*)").get();
                season_info_kv=seasonCache.remove(seasonId);
                qqSeasoninfoJsonStr=page.getJson().toString();
                page.putField(QqAnimeSessionInfoPipeline.qqSessionInfoJsonStr,qqSeasoninfoJsonStr);
                page.putField(QqAnimeSessionInfoPipeline.season_info_kv, season_info_kv);
            } catch (Exception e) {
                logger.error("can not  process url {} json data", page.getRequest().getUrl(), e);
            }


        }


    }


    private synchronized static String buildNextPageUrl() {
        offset += 30;
        return StringUtils.join("https://v.qq.com/x/bu/pagesheet/list?append=1&channel=cartoon&offset=", offset, "&pagesize=30");
    }


    public static void main(String[] args) {
        Spider.create(new QqAnimeIndexPageProcessor())
                .addUrl("https://v.qq.com/x/bu/pagesheet/list?append=1&channel=cartoon&offset=0&pagesize=30")
                //.addUrl("https://v.qq.com/x/cover/m441e3rjq9kwpsc.html")
                .addPipeline(new QqAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
