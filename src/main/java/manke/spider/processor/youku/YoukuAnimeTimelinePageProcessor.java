package manke.spider.processor.youku;

import manke.spider.model.qq.QqConstant;
import manke.spider.model.youku.YoukuConstant;
import manke.spider.pipeline.youku.YoukuAnimeTimelinePipeline;
import manke.spider.processor.AbstractPageProcessor;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Selectable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by luozhi on 2017/5/21.
 *  新番时间表
 * 从 url 获取 信息  1.http://comic.youku.com/bangumi
 *
 */
public class YoukuAnimeTimelinePageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(YoukuAnimeTimelinePageProcessor.class);

    public void process(Page page) {

        if(StringUtils.contains(page.getRequest().getUrl(),"comic")){

            //最新的番剧更新列表，优酷视频只显示本周要更新的番剧信息。所以每周1运行一次，将本周新番数据全部获取到即可
            List<Selectable>  season_div_list=null;
            List<Selectable>  season_pack_list=null;
            Map<String,String> season_info_kv=null;
            List< Map<String,String>> season_info_kvs=new ArrayList<>();
            String date_ts=System.currentTimeMillis()+"";
            String day_of_week=null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                season_div_list=
                page.getHtml().xpath("//div[@class='mod mod-new']").nodes();
                if (season_div_list!=null){

                    for(Selectable season_div:season_div_list){
                        day_of_week=season_div.xpath("div[@class='h']/h2/text()").get();

                        season_pack_list=season_div.xpath("//div[@class='yk-pack pack-film']").nodes();
                        if (season_pack_list!=null){

                            for (Selectable season_pack:season_pack_list){

                                season_info_kv=new HashedMap();
                                season_info_kv.put(YoukuConstant.UPDATE_WEEK,day_of_week);
                                season_info_kv.put(YoukuConstant.SPIDER_TS,date_ts);
                                season_info_kv.put(YoukuConstant.UPDATE_INFO,season_pack.xpath("//span[@class='p-num']/text()").get());
                                season_info_kv.put(YoukuConstant.UPDATE_TIME,season_pack.xpath("//li[@class='subtitle']/span/text()").get());
                                season_info_kv.put(YoukuConstant.TITLE,season_pack.xpath("//li[@class='title short-title']/a/text()").get());
                                season_info_kv.put(YoukuConstant.SEASON_ID,season_pack.xpath("//li[@class='title short-title']/a/@href").regex(".*id_(.*)\\.html.*").get());

                                season_info_kvs.add(season_info_kv);

                            }

                        }


                    }
                }

                page.putField(YoukuAnimeTimelinePipeline.SEASON_INFO_KVS,season_info_kvs);


            }catch (Exception e){
                logger.error("can not  process url {}  data",page.getRequest().getUrl(),e);
            }


        }


    }


    public static void main(String[] args) {
        Spider.create(new YoukuAnimeTimelinePageProcessor())
                .addUrl("http://comic.youku.com/bangumi")
                .addPipeline(new YoukuAnimeTimelinePipeline())
                .thread(1).run();
    }
}
