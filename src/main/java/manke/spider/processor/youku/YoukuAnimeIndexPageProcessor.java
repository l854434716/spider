package manke.spider.processor.youku;

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

import static manke.spider.model.youku.YoukuConstant.*;
/**
 * Created by luozhi on 2017/5/21.
 *  抓取全量的番剧数据
 * 从 3种url 获取 信息  1.http://list.youku.com/category/show/c_100_s_1_d_2_p_1.html  番剧集索引页
 *                    2.http://list.youku.com/show/id_z9a8e9ba0605611e2a19e.html  番剧详细信息
 *                   3.http://v.youku.com/v_show/id_XODU2MTEyNjI4.html 从该番剧播放url获取 番剧详细信息url
 *                    z9a8e9ba0605611e2a19e 表示番剧详情信息查询ID  优酷网番剧的唯一标识ID 有多种，这里使用番剧详情信息查询ID
 */
public class YoukuAnimeIndexPageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(YoukuAnimeIndexPageProcessor.class);


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
        List<Selectable>  season_li_list=null;
        Map<String,String>  season_info_kv=null;

        try {

            season_li_list=page.getHtml().xpath("//div[@class='box-series']/ul/li").nodes();

            if (season_li_list!=null){

                for(Selectable li:season_li_list){
                    //进入番剧播放页
                    page.addTargetRequest(li.xpath("//div[@class='p-thumb']/a/@href").get());
                    logger.info("season url is {}",li.xpath("//div[@class='p-thumb']/a/@href").get());
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
        logger.info("doDetailPageProcessor  url is {}",page.getUrl().toString());
        //番剧详情html div 模块
        Selectable detail_div_selectable= page.getHtml().xpath("//div[@class='mod mod-new']/div[@class='mod fix']");

        //番剧海报信息
        Selectable  detail_post_div_selectable=detail_div_selectable.xpath("div[@class='p-post']");
        //番剧文字信息
        Selectable  detail_base_div_selectable=detail_div_selectable.xpath("div[@class='p-base']");

        Map<String,String> sesaon_info_kv=new HashMap<>();

        sesaon_info_kv.put(SEASON_ID,page.getUrl().regex(".*id_(.*)\\.html.*").get());
        sesaon_info_kv.put(TITLE,detail_post_div_selectable.xpath("//div[@class='p-thumb']/a/@title").get());
        sesaon_info_kv.put(WEBPLAYURL,detail_post_div_selectable.xpath("//div[@class='p-thumb']/a/@href").get());
        sesaon_info_kv.put(COVER,detail_post_div_selectable.xpath("//div[@class='p-thumb']/img/@src").get());
        sesaon_info_kv.put(EXCLUSIVE,detail_post_div_selectable.xpath("//span[@class='ico-lt']/text()").get());
        sesaon_info_kv.put(ARTICULATION,detail_post_div_selectable.xpath("//span[@class='ico-SD']/text()").get());
        sesaon_info_kv.put(MARK_V,detail_post_div_selectable.xpath("//span[@class='vip-free']/text()").get());

        //番剧文字信息列
        List<Selectable> detail_base_li_selectables=detail_base_div_selectable.xpath("ul/li").nodes();
        if (detail_base_li_selectables.size()<13){

            page.setSkip(true);
            logger.info("season_detail_info_miss url is {}",page.getUrl().toString());
            return ;
        }
        Selectable _li=null;
        String  _s=detail_base_li_selectables.get(3).xpath("span/label/@data-spm-anchor-id").get();
        if(StringUtils.isEmpty(detail_base_li_selectables.get(4).xpath("span/label/text()").get())){
            //番剧信息列表不带番剧上映时间
            _li=detail_base_li_selectables.get(0);//p-title
            sesaon_info_kv.put(EDITION,_li.xpath("//span[@class='edition']/text()").get());

            _li=detail_base_li_selectables.get(1);//更新情况
            sesaon_info_kv.put(UPDATE_INFO, _li.xpath("li/text()").get());

            _li=detail_base_li_selectables.get(2); //别名
            sesaon_info_kv.put(ALIAS,_li.xpath("li/@title").get());

            _li=detail_base_li_selectables.get(3); //优酷开播时间
            sesaon_info_kv.put(PUB_WEB,_li.xpath("span/text()").get());

            _li=detail_base_li_selectables.get(4); //评分
            sesaon_info_kv.put(SCORE,_li.xpath("span[@class='star-num']/text()").get());

            _li=detail_base_li_selectables.get(5); //适用年龄
            sesaon_info_kv.put(RANGE_OF_APPLICATION,_li.xpath("li/text()").get());

            _li=detail_base_li_selectables.get(6); //导演
            sesaon_info_kv.put(DIRECTOR,_li.xpath("a/@title").get());

            _li=detail_base_li_selectables.get(7); //地区
            sesaon_info_kv.put(REGION,_li.xpath("a/text()").get());

            _li=detail_base_li_selectables.get(8); //类型
            List<String> types=_li.xpath("a/text()").all();
            sesaon_info_kv.put(TYPES,StringUtils.join(types,"/"));

            _li = detail_base_li_selectables.get(9); //配音
            List<String> dubbeds=_li.xpath("a/text()").all();
            sesaon_info_kv.put(DUBBEDS,StringUtils.join(dubbeds,"/"));

            _li = detail_base_li_selectables.get(10); //描述
            sesaon_info_kv.put(BRIEF,_li.xpath("span[@class='intro-more hide']/text()").get());

        }else {
            //番剧信息列表带番剧上映时间
            _li=detail_base_li_selectables.get(0);//p-title
            sesaon_info_kv.put(EDITION,_li.xpath("//span[@class='edition']/text()").get());

            _li=detail_base_li_selectables.get(1);//更新情况
            sesaon_info_kv.put(UPDATE_INFO, _li.xpath("li/text()").get());

            _li=detail_base_li_selectables.get(2); //别名
            sesaon_info_kv.put(ALIAS,_li.xpath("li/@title").get());

            _li=detail_base_li_selectables.get(3); //上映时间
            sesaon_info_kv.put(SCREEN_TIME,_li.xpath("span/text()").get());

            _li=detail_base_li_selectables.get(4); //优酷开播时间
            sesaon_info_kv.put(PUB_WEB,_li.xpath("span/text()").get());

            _li=detail_base_li_selectables.get(5); //评分
            sesaon_info_kv.put(SCORE,_li.xpath("span[@class='star-num']/text()").get());

            _li=detail_base_li_selectables.get(6); //适用年龄
            sesaon_info_kv.put(RANGE_OF_APPLICATION,_li.xpath("li/text()").get());

            _li=detail_base_li_selectables.get(7); //导演
            sesaon_info_kv.put(DIRECTOR,_li.xpath("a/@title").get());

            _li=detail_base_li_selectables.get(8); //地区
            sesaon_info_kv.put(REGION,_li.xpath("a/text()").get());

            _li=detail_base_li_selectables.get(9); //类型
            List<String> types=_li.xpath("a/text()").all();
            sesaon_info_kv.put(TYPES,StringUtils.join(types,"/"));

            _li = detail_base_li_selectables.get(10); //配音
            List<String> dubbeds=_li.xpath("a/text()").all();
            sesaon_info_kv.put(DUBBEDS,StringUtils.join(dubbeds,"/"));

            _li = detail_base_li_selectables.get(11); //描述
            sesaon_info_kv.put(BRIEF,_li.xpath("span[@class='intro-more hide']/text()").get());

        }

        page.putField(YoukuAnimeSessionInfoPipeline.season_info_kv,sesaon_info_kv);



    }


    private  void   doPlayPageProcessor(Page  page){
        logger.info("doPlayPageProcessor  url is {}",page.getUrl().toString());
        try {
            String  tagurl=page.getHtml().xpath("//div[@class='title-wrap']/h1/span[1]/a/@href").get();
           if (tagurl==null){
               page.addTargetRequest(page.getUrl().toString());
           }else{
               page.addTargetRequest(tagurl);
               logger.info("{} get detail page url is {}",page.getUrl().toString(),tagurl);
           }
            page.setSkip(true);
        }catch (Exception e){
            logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
        }
    }


    public static void main(String[] args) {
        Spider.create(new YoukuAnimeIndexPageProcessor())
                .addUrl("http://list.youku.com/category/show/c_100_pt_0_s_1_d_1.html")
                .addPipeline(new YoukuAnimeSessionInfoPipeline())
                .thread(10).run();
    }
}
