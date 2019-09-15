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
    Logger  logger= LoggerFactory.getLogger(QqAnimeIndexPageProcessor.class);

    //缓存从番剧list页面爬取下来的番剧信息，供保存番剧详情信息的pipeline使用  k 为番剧ID v 为番剧属性和值
    private  final  static  Map<String,Map<String,String>>seasonCache= new ConcurrentHashMap<>();

    //番剧详情json信息url 前缀
    private  final String  preUrlDetailSeason="http://node.video.qq.com/x/api/float_vinfo2?cid=";


    public void process(Page page) {

        //http://v.qq.com/x/list/cartoon?offset=0 索引页数据
        if(StringUtils.contains(page.getRequest().getUrl(),"offset")){

            List<Selectable>  season_li_list=null;
            Map<String,String>  season_info_kv=null;

            try {
                //数据转换出错或者数据来源url 是其他页面
                season_li_list=page.getHtml().xpath("//div[@class='mod_figures mod_figure_v']/ul/li").nodes();

                if (season_li_list!=null){

                    for(Selectable li:season_li_list){

                        /*<li class="list_item">
                                <a href="https://v.qq.com/x/cover/nug3wu4a6zvwy8s.html" class="figure" data-float="nug3wu4a6zvwy8s" >
                                <img src="//puui.qpic.cn/vcover_vt_pic/0/nug3wu4a6zvwy8s1498654532/220" alt="捷德奥特曼 中文版">
                                <div class="figure_caption figure_caption_score">
                                <span class="figure_info">更新至23集</span>
                                </div>
                                <i class="mark_v"><img src="//i.gtimg.cn/qqlive/images/mark/mark_5.png" srcset="//i.gtimg.cn/qqlive/images/mark/mark_5@2x.png 2x" alt="VIP"></i>
                                </a>
                                <div class="figure_title_score">
                                <strong class="figure_title"><a href="https://v.qq.com/x/cover/nug3wu4a6zvwy8s.html">捷德奥特曼 中文版</a></strong>
                                <div class="figure_score">
                                <em class="score_l">8</em>
                                <em class="score_s">.8</em>
                                </div>
                                </div>

                                <div class="figure_desc" title="超酷炫奥特曼战斗">
                                        超酷炫奥特曼战斗
                                        </div>
                                <div class="figure_count">
                                <span class="num">3亿</span>
                                </div>
                        </li>*/
                        season_info_kv=new HashedMap();
                        season_info_kv.put(QqConstant.WEBPLAYURL,li.xpath("a/@href").get());
                        season_info_kv.put(QqConstant.SEASON_ID,li.xpath("a/@data-float").get());
                        season_info_kv.put(QqConstant.COVER,li.xpath("a/img/@src").get());
                        season_info_kv.put(QqConstant.UPDATE_INFO,li.xpath("a/div/span/text()").get());
                        season_info_kv.put(QqConstant.MARK_V,li.xpath("a/i").get()==null?"0":"1");
                        season_info_kv.put(QqConstant.PLAY_COUNT, li.xpath("//span[@class='num']/text()").get());

                        seasonCache.put(season_info_kv.get(QqConstant.SEASON_ID),season_info_kv);
                        logger.info("put  {}  {} to seasonCache ",season_info_kv.get(QqConstant.SEASON_ID),season_info_kv);

                        //抓取番剧评分

                        page.addTargetRequest(StringUtils.join("https://v.qq.com/detail/k/", li.xpath("a/@data-float").get(), ".html"));

                    }

                }
                //获取下一页url 继续爬取
                String  nextPageUrlOffset=
                page.getHtml().xpath("//div[@class='mod_pages']/a[@class='page_next']/@href").get();

                if (StringUtils.isNotEmpty(nextPageUrlOffset)){
                    page.addTargetRequest(nextPageUrlOffset);
                    logger.info("spider next page url is {}",nextPageUrlOffset);
                }else{
                    logger.info("spider  to  last page...");
                }
                // page.putField(BibiAnimeIndexPipeline.bibiIndexGlobalSeasonJsonStrList,bibiIndexGlobalSeasonJsonStrList);

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


        //http://node.video.qq.com/x/api/float_vinfo2?cid=p0pcfbdk318ry3m
        if(StringUtils.contains(page.getRequest().getUrl(),"float_vinfo2")){

            String  qqSeasoninfoJsonStr=null;
            String  seasonId=null;
            Map<String,String>  season_info_kv=null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                seasonId=page.getUrl().regex(".*cid=(.*)").get();
                season_info_kv=seasonCache.remove(seasonId);
                qqSeasoninfoJsonStr=page.getJson().toString();
                page.putField(QqAnimeSessionInfoPipeline.qqSessionInfoJsonStr,qqSeasoninfoJsonStr);
                page.putField(QqAnimeSessionInfoPipeline.season_info_kv,season_info_kv);
            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


    }


    public static void main(String[] args) {
        Spider.create(new QqAnimeIndexPageProcessor())
                .addUrl("http://v.qq.com/x/list/cartoon?offset=0")
                .addPipeline(new QqAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
