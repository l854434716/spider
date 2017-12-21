package manke.spider.processor.aiqiyi;

import manke.spider.model.AnimeKeyNameConstant;
import manke.spider.pipeline.aiqiyi.AiQiYiAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Selectable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by htlan on 2017/12/14.
 * 抓取爱奇艺动漫全部番剧
 */
public class AiQiYiAnimeIndexPageProcessor extends AbstractPageProcessor {

    Logger logger= LoggerFactory.getLogger(AiQiYiAnimeIndexPageProcessor.class);

    private static String title;

    @Override
    public void process(Page page) {
        //http://list.iqiyi.com/www/4/-------------4-1-1-iqiyi--.html
        if (StringUtils.contains(page.getRequest().getUrl(), "list.iqiyi.com")) {
            try {
                List<Selectable> season_li_list = page.getHtml().xpath("//div[@class='wrapper-piclist']/ul/li").nodes();
                if (season_li_list != null) {
                    for (Selectable li : season_li_list) {
                        //抓取番剧详情信息url地址
                        page.addTargetRequest(li.xpath("a/@href").get());
                    }
                }
                //获取下一页url 继续爬取
                String nextPageUrlOffset = page.getHtml().xpath("//div[@class='mod-page']/a[@data-key='down']/@href").get();
                if (StringUtils.isNotEmpty(nextPageUrlOffset)) {
                    page.addTargetRequest(nextPageUrlOffset);
                    logger.info("spider next page url is {}", nextPageUrlOffset);
                } else {
                    logger.info("spider  to  last page... ----{}",nextPageUrlOffset);
                }
            } catch (Exception e) {
                logger.error("can not  process url {} json data", page.getRequest().getUrl(), e);
            }
        }
        //详情页抓取
        //http://www.iqiyi.com/a_19rrh8jc4l.html#vfrm=2-4-0-1
        //http://www.iqiyi.com/a_19rrha8tfx.html#vfrm=2-4-0-1
        //  .*a_(.*).html.*
        if (StringUtils.contains(page.getRequest().getUrl(), "vfrm=2-4-0-1")) {
            //所有信息  k:名称  v:值
            Map<String, String> sesaon_info_kv = new HashMap<>();
            //番剧详情html div 模块
            Selectable detail_div_selectable = page.getHtml().xpath("//div[@class='mod_reuslt clearfix']");
            //result_pic pr
            Selectable result_pic_pr = page.getHtml().xpath("//div[@class='result_pic pr']");
            sesaon_info_kv.put(AnimeKeyNameConstant.SEASON_ID,page.getUrl().regex(".*a_(.*).html.*").get());
            String title = result_pic_pr.xpath("a/@title").get(); //动漫名称
            sesaon_info_kv.put("动漫名称",title);
            String url = result_pic_pr.xpath("a/@href").get(); //动 漫地址连接
            sesaon_info_kv.put("播放链接",url);
            String imgurl = result_pic_pr.xpath("a/img/@src").get(); //动漫图片连接
            sesaon_info_kv.put("图片链接",imgurl);
            String vip = "1";   //是否是vip
            if(StringUtils.isEmpty(result_pic_pr.xpath("//p[@class='site-piclist_icons-lt']").get())){
                vip = "0";
            }
            sesaon_info_kv.put("是否VIP",vip);

            //result_detail
            Selectable result_detail = page.getHtml().xpath("//div[@class='result_detail']/div[@class='result_detail-minH']");
            String year = result_detail.xpath("//span[@class='sub_title']/text()").get();//动漫年份
            sesaon_info_kv.put("年份",year);
            //动漫评分
            String score = result_detail.xpath("//div[@class='topic_item topic_item-rt']/div[@class='score_num_new score_num_new-normal']/span[@class='score_font']/@score_font").get();
            sesaon_info_kv.put("评分",score);
            String play_count = result_detail.xpath("//div[@class='topic_item clearfix']/div[@class='left_col pr']/span[@class='allplayNums-type2']/a/i/text()").get();//播放量
            sesaon_info_kv.put("播放总量",play_count);
            String brief = result_detail.xpath("//div[@class='topic_item clearfix']/span[@class='showMoreText']/span/text()").get();//简介
            sesaon_info_kv.put("简介",brief);
            List<Selectable> detail_base_li_selectables = detail_div_selectable.xpath("//div[@class='topic_item clearfix']").nodes();
            for (Selectable li : detail_base_li_selectables) {
                if (!StringUtils.isEmpty(li.xpath("//div[@class='left_col']").get())||!StringUtils.isEmpty(li.xpath("//div[@class='right_col']").get())){
                    String key = li.xpath("//div[@class='left_col']/em/text()").get();
                    List<String> value = li.xpath("//div[@class='left_col']/em/a/text()").all();
                    if(value != null){
                        if (StringUtils.isNotEmpty(key)){
                            if(key.contains("/")){
                                sesaon_info_kv.put(key.split("：")[0],StringUtils.join(value,"/")+"/"+key.split("\\/")[1]);
                            }else{
                                sesaon_info_kv.put(key.split("：")[0],StringUtils.join(value,"/"));
                            }
                        }

                    }else{
                        if (StringUtils.isNotEmpty(key))
                            sesaon_info_kv.put(key.split("：")[0],key.split("：")[1]);
                    }
                }
                continue;
            }
            //添加处理
            page.putField(AiQiYiAnimeSessionInfoPipeline.season_info_kv,sesaon_info_kv);
        }
    }

    public static void main(String[] args){
        Spider.create(new AiQiYiAnimeIndexPageProcessor())
                .addUrl("http://list.iqiyi.com/www/4/-------------4-1-1-iqiyi--.html")
                .addPipeline(new AiQiYiAnimeSessionInfoPipeline())
                .thread(5)
                .run();
    }
}
