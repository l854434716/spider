package manke.spider.processor.youku;

import manke.spider.model.youku.YoukuConstant;
import manke.spider.pipeline.youku.YoukuAnimeSessionInfoPipeline;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Selectable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Created by luozhi on 2017/5/21.
 *  抓取全量的番剧数据
 * 从 3种url 获取 信息  1.http://list.youku.com/category/show/c_100_s_1_d_2_p_1.html  番剧集索引页
 *                    2.http://list.youku.com/show/id_z9a8e9ba0605611e2a19e.html  番剧详细信息
 *                   3.http://v.youku.com/v_show/id_XODU2MTEyNjI4.html 从该番剧播放url获取 番剧详细信息url
 *                    z9a8e9ba0605611e2a19e 表示番剧详情信息查询ID  优酷网番剧的唯一标识ID 有多种，这里使用番剧详情信息查询ID
 */
public class YoukuAnimeIndexPageProcessor implements PageProcessor {
    Logger  logger= LoggerFactory.getLogger(YoukuAnimeIndexPageProcessor.class);

    private Site site = Site.me()
            //.enableHttpProxyPool()
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36")
            .setRetryTimes(3).setSleepTime(1000).setTimeOut(10000).setCharset("UTF-8");

    public void process(Page page) {

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
        List<Selectable>  season_li_list=null;
        Map<String,String>  season_info_kv=null;

        try {

            season_li_list=page.getHtml().xpath("//div[@class='box-series']/ul/li").nodes();

            if (season_li_list!=null){

                for(Selectable li:season_li_list){
                    //进入番剧播放页
                    page.addTargetRequest(li.xpath("//div[@class='p-thumb']/a/@href").get());

                }
            }
            //获取下一页url 继续爬取
            String  nextPageUrlOffset=
                    page.getHtml().xpath("//div[@class='yk-pager']/ul/li[@class='next']/a/@href").get();

            if (StringUtils.isNotEmpty(nextPageUrlOffset)){
                page.addTargetRequest(nextPageUrlOffset);
                logger.info("spider next page url is {}",nextPageUrlOffset);
            }else{
                logger.info("spider  to  last page...");
            }
            page.setSkip(true);

        }catch (Exception e){
            logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
        }
    }


    private  void   doDetailPageProcessor(Page  page){

        //番剧详情html div 模块
      Selectable detail_div_selectable= page.getHtml().xpath("//div[@class='mod mod-new']/div[@class='mod fix']");

        //番剧海报信息
        Selectable  detail_post_div_selectable=detail_div_selectable.xpath("div[@class='p-post']");
        //番剧文字信息
        Selectable  detail_base_div_selectable=detail_div_selectable.xpath("div[@class='p-base']");

        Map<String,String> sesaon_info_kv=new HashMap<>();

        sesaon_info_kv.put(YoukuConstant.TITLE,detail_post_div_selectable.xpath("//div[@class='p-thumb']/a/@title").get());
        sesaon_info_kv.put("",detail_post_div_selectable.xpath("//div[@class='p-thumb']/a/@href").get());
        sesaon_info_kv.put("",detail_post_div_selectable.xpath("//div[@class='p-thumb']/img/@src").get());
        sesaon_info_kv.put("",detail_post_div_selectable.xpath("//span[@class='ico-lt']/text()").get());
        sesaon_info_kv.put("",detail_post_div_selectable.xpath("//span[@class='ico-SD']/text()").get());
        sesaon_info_kv.put("",detail_post_div_selectable.xpath("//span[@class='vip-free']/text()").get());

        //番剧文字信息列
        List<Selectable> detail_base_li_selectables=detail_base_div_selectable.xpath("ul/li").nodes();

        Selectable _li=null;
        if(detail_base_li_selectables.size()==14){
            //番剧信息列表不带番剧上映时间
            _li=detail_base_li_selectables.get(0);//p-title
            _li.xpath("//span[@class='edition']/text()").get();

            _li=detail_base_li_selectables.get(0);

        }else if(detail_base_li_selectables.size()==14){
            //番剧信息列表带番剧上映时间

        }else{
            //未知的列表信息
            logger.error("pageProcessor detail page ERROR ",detail_base_div_selectable.xpath("ul/li").toString());

        }


        //sesaon_info_kv.put("",);


    }


    private  void   doPlayPageProcessor(Page  page){

        try {
            page.addTargetRequest(page.getHtml().xpath("//a[@class='desc-link']/@href").get());
            page.setSkip(true);
        }catch (Exception e){
            logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
        }
    }

    public Site getSite() {
        List<String[]> poolHosts = new ArrayList<String[]>();
        poolHosts.add(new String[]{"username","password","178.140.216.229","8080"});
      //  site.setHttpProxyPool(poolHosts,false);
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new YoukuAnimeIndexPageProcessor())
                .addUrl("http://list.youku.com/category/show/c_100_s_1_d_2_p_1.html")
                .addPipeline(new YoukuAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
