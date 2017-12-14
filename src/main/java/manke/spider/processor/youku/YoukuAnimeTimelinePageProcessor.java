package manke.spider.processor.youku;

import manke.spider.model.qq.QqConstant;
import manke.spider.model.youku.YoukuConstant;
import manke.spider.pipeline.youku.YoukuAnimeTimelinePipeline;
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
public class YoukuAnimeTimelinePageProcessor implements PageProcessor {
    Logger  logger= LoggerFactory.getLogger(YoukuAnimeTimelinePageProcessor.class);

    private Site site = Site.me()
            //.enableHttpProxyPool()
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36")
            .setRetryTimes(3).setSleepTime(3000).setTimeOut(10000).setCharset("UTF-8");

    public void process(Page page) {

        if(StringUtils.contains(page.getRequest().getUrl(),"comic")){

            //最新的番剧更新列表，优酷视频只显示本周要更新的番剧信息。所以每周运行一次，将本周新番数据全部获取到即可
            List<Selectable>  season_div_list=null;
            List<Selectable>  season_ul_list=null;
            Map<String,String> season_info_kv=null;
            List< Map<String,String>> season_info_kvs=new ArrayList<>();
            String date_ts=null;
            String day_of_week=null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                season_div_list=
                page.getHtml().xpath("//div[@class='yk-con']/div").nodes();
                if (season_div_list!=null){

                    int  week_index=0;

                    for(Selectable season_div:season_div_list){
                        if (week_index==0){
                            //week_index 为0 代表当前div 是番剧周更表的表头图片  之后 week_index 数字代表周几
                            continue;
                        }

                        season_ul_list=season_div.xpath("//div[@class='yk-pack pack-film']/ul").nodes();

                        season_info_kv.put(YoukuConstant.UPDATE_INFO,season_ul_list.get(0).xpath("li/span/text()").get());
                        season_info_kv.put(YoukuConstant.UPDATE_TIME,season_ul_list.get(1).xpath("li[2]/span/text()").get());
                        season_info_kv.put(YoukuConstant.TITLE,season_ul_list.get(1).xpath("li[1]/a/text()").get());
                        season_info_kv.put(YoukuConstant.SEASON_ID,season_ul_list.get(1).xpath("li[1]/a/@href").regex(".*id_(.*)\\.html.*").get());

                        season_info_kvs.add(season_info_kv);
                        week_index++;
                    }
                }

                page.putField(YoukuAnimeTimelinePipeline.SEASON_INFO_KVS,season_info_kvs);


            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


    }





    public Site getSite() {
        List<String[]> poolHosts = new ArrayList<String[]>();
        poolHosts.add(new String[]{"username","password","178.140.216.229","8080"});
      //  site.setHttpProxyPool(poolHosts,false);
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new YoukuAnimeTimelinePageProcessor())
                .addUrl("http://comic.youku.com/bangumi")
                .addPipeline(new YoukuAnimeTimelinePipeline())
                .thread(1).run();
    }
}
