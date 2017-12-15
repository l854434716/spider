package manke.spider.processor.bibi;

import manke.spider.model.bibi.BibiConstant;
import manke.spider.pipeline.bibi.BibiAnimeTimelinePipeline;
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
 *  抓取增量的番剧数据
 * 从 3种url 获取 信息  1.http://bangumi.bilibili.com/web_api/timeline_global   国外番剧集
 *                   2.http://bangumi.bilibili.com/web_api/timeline_cn      国内番剧集
 *
 */
public class BibiAnimeTimelinePageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(BibiAnimeTimelinePageProcessor.class);


    public void process(Page page) {

        if(StringUtils.contains(page.getRequest().getUrl(),"timeline_global")){

            //最新的番剧更新列表（取接口返回的最远的那天的数据）
            List<String> bibiTimelineLastDaySesssionArrayJsonStr=null;
            String date_ts=null;
            String day_of_week=null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                bibiTimelineLastDaySesssionArrayJsonStr=page.getJson().jsonPath("$.result[(@.length-1)].seasons[*]").all();
                date_ts= page.getJson().jsonPath("$.result[(@.length-1)].date_ts").get();
                day_of_week=page.getJson().jsonPath("$.result[(@.length-1)].day_of_week").get();

                page.putField(BibiAnimeTimelinePipeline.bibiTimelineLastDaySesssionArrayJsonStr,bibiTimelineLastDaySesssionArrayJsonStr);
                page.putField(BibiConstant.DATE_TS,date_ts);
                page.putField(BibiConstant.DAY_OF_WEEK,day_of_week);

            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


        if(StringUtils.contains(page.getRequest().getUrl(),"timeline_cn")){

            //最新的番剧更新列表（取接口返回的最远的那天的数据）
            List<String> bibiTimelineLastDaySesssionArrayJsonStr=null;
            String date_ts=null;
            String day_of_week=null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                bibiTimelineLastDaySesssionArrayJsonStr=page.getJson().jsonPath("$.result[(@.length-1)].seasons[*]").all();
                date_ts= page.getJson().jsonPath("$.result[(@.length-1)].date_ts").get();
                day_of_week=page.getJson().jsonPath("$.result[(@.length-1)].day_of_week").get();
                page.putField(BibiAnimeTimelinePipeline.bibiTimelineLastDaySesssionArrayJsonStr,bibiTimelineLastDaySesssionArrayJsonStr);
                page.putField(BibiConstant.DATE_TS,date_ts);
                page.putField(BibiConstant.DAY_OF_WEEK,day_of_week);

            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


    }



    public static void main(String[] args) {
        Spider.create(new BibiAnimeTimelinePageProcessor())
                .addUrl("http://bangumi.bilibili.com/web_api/timeline_global")
                .addUrl("http://bangumi.bilibili.com/web_api/timeline_cn")
                .addPipeline(new BibiAnimeTimelinePipeline())
                .thread(2).run();
    }
}
