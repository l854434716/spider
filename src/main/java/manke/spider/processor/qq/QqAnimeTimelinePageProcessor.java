package manke.spider.processor.qq;

import manke.spider.model.qq.QqConstant;
import manke.spider.pipeline.qq.QqAnimeTimelinePipeline;
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
 *  抓取增量的番剧数据
 * 从 url 获取 信息  1.https://v.qq.com/x/channel/cartoon
 *
 */
public class QqAnimeTimelinePageProcessor extends AbstractPageProcessor {
    Logger  logger= LoggerFactory.getLogger(QqAnimeTimelinePageProcessor.class);


    public void process(Page page) {

        if(StringUtils.contains(page.getRequest().getUrl(),"cartoon")){

            //最新的番剧更新列表，腾讯视频只显示本周要更新的番剧信息。所以每周1运行一次，将本周新番数据全部获取到即可
            List<Selectable>  season_ul_list=null;
            Map<String,String> season_info_kv=null;
            List< Map<String,String>> season_info_kvs=new ArrayList<>();
            String date_ts=System.currentTimeMillis()+"";
            String day_of_week=null;
            try {
                //数据转换出错或者数据来源url 是其他页面
                season_ul_list=
                page.getHtml().xpath("//div[@class='mod_figure mod_figure_v mod_figure_v_default']/ul[@data-schedule-tab]").nodes();
                if (season_ul_list!=null){
                    List<Selectable> season_li_list=null;
                    for(int i=1;i<season_ul_list.size();i++){

                        season_li_list=season_ul_list.get(i).xpath("li").nodes();

                        if (season_li_list!=null){

                            /*<li class="list_item" __wind>
                                <a href="https://v.qq.com/x/cover/nubvtawpmjjgvu7.html"  target="_blank" class="figure" tabindex="-1" _stat="schedule:img" data-float="nubvtawpmjjgvu7" >
                                <img class="figure_pic" lz_src="//puui.qpic.cn/vcover_vt_pic/0/nubvtawpmjjgvu71507518295/220" alt="幻界战线 &amp; BEYOND" onerror="picerr(this,'v')" src="//i.gtimg.cn/qqlive/images/20150608/pic_v.png">
                                <div class="figure_count">


                                        更新至10集


                                        </div>

                                <div class="figure_score"><em class="score_l">9</em><em class="score_s">.3</em></div>

                                </a>
                                <div class="figure_detail figure_detail_two_row">
                                <strong class="figure_title figure_title_two_row"><a href="https://v.qq.com/x/cover/nubvtawpmjjgvu7.html" title="幻界战线 &amp; BEYOND" _stat="schedule:title" target="_blank">幻界战线 &amp; BEYOND</a></strong>
                                <div class="figure_desc" title="地球上最危险的城市">
                                        地球上最危险的城市
                                        </div>
                                </div>

                           </li>*/
                            for (Selectable li: season_li_list){

                                season_info_kv=new HashedMap();
                                season_info_kv.put(QqConstant.SEASON_ID,li.xpath("a/@data-float").get());
                                season_info_kv.put(QqConstant.UPDATE_INFO,li.xpath("div[@class='figure_count']/text()").get());
                                season_info_kv.put(QqConstant.TITLE,li.xpath("//strong[@class='figure_title figure_title_two_row']/a/@title").get());
                                season_info_kv.put(QqConstant.UPDATE_WEEK,i+"");
                                season_info_kv.put(QqConstant.SPIDER_TS,date_ts);
                                season_info_kvs.add(season_info_kv);

                            }


                        }

                    }

                }

                page.putField(QqAnimeTimelinePipeline.SEASON_INFO_KVS,season_info_kvs);

            }catch (Exception e){
                logger.error("can not  process url {} json data",page.getRequest().getUrl(),e);
            }


        }


    }



    public static void main(String[] args) {
        Spider.create(new QqAnimeTimelinePageProcessor())
                .addUrl("https://v.qq.com/x/channel/cartoon")
                .addPipeline(new QqAnimeTimelinePipeline())
                .thread(1).run();
    }
}
