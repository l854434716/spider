package manke.spider.processor.aiqiyi;

import manke.spider.model.AnimeKeyNameConstant;
import manke.spider.pipeline.aiqiyi.AiQiYiAnimeTimelinePipeline;
import manke.spider.processor.AbstractPageProcessor;
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
 * Created by htlan on 2017/12/21.
 * 爱奇艺增量数据抓取
 */
public class AiQiYiAnimeTimelinePageProcessor extends AbstractPageProcessor {

   Logger logger= LoggerFactory.getLogger(AiQiYiAnimeTimelinePageProcessor.class);

    @Override
    public void process(Page page) {
        if(StringUtils.contains(page.getRequest().getUrl(), "dongman")){
            try {
                List<Map<String,String>> season_info_kv_list = new ArrayList<>();
                List<Selectable> Selectablelist = page.getHtml().xpath("//div[@class='o-hidden']/").nodes();
                int week = 0;
                for (Selectable sel : Selectablelist) {
                    week++;    //周
                    List<Selectable> list = sel.xpath("//div[@class='wrapper-piclist']/ul/li").nodes();
                    for (Selectable li : list) {
                        Map<String,String> season_info_kv = new HashMap<>();
                        String title = li.xpath("//div[@class='site-piclist_pic']/a/@title").get();
                        String season_id = li.xpath("//div[@class='site-piclist_pic']/a/@href").regex(".*v_(.*).html.*").get();
                        String update = li.xpath("//div[@class='site-piclist_pic']/a/div/div/span/text()").get();
                        season_info_kv.put(AnimeKeyNameConstant.SEASON_ID,season_id);
                        season_info_kv.put("日期",String.valueOf(week));
                        season_info_kv.put("名称",title);
                        season_info_kv.put("更新",update);
                        season_info_kv_list.add(season_info_kv);
                    }
                }
                page.putField(AiQiYiAnimeTimelinePipeline.season_info_kv,season_info_kv_list);
            }catch (Exception e){
                logger.error("{}",e.getMessage());
            }
        }
    }

    public static void main(String[] args){
        Spider.create(new AiQiYiAnimeTimelinePageProcessor())
                .addUrl("http://www.iqiyi.com/dongman/")
                .addPipeline(new AiQiYiAnimeTimelinePipeline())
                .thread(1)
                .run();
    }
}
