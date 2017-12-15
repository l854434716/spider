package manke.spider.processor;

import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

/**
 * Created by zhiluo on 2017/12/15.
 *
 *
 */
public abstract class AbstractPageProcessor implements PageProcessor {


    /**
     * get the site settings
     *
     * @return site
     * @see Site
     */
    @Override
    public Site getSite() {
        Site site = Site.me()
                //.enableHttpProxyPool()
                .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36")
                .setRetryTimes(3).setSleepTime(3000).setTimeOut(10000).setCharset("UTF-8");
        return site;
    }
}
