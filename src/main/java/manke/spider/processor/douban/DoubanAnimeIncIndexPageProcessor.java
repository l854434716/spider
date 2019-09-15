package manke.spider.processor.douban;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import manke.spider.model.douban.DoubanConstant;
import manke.spider.model.douban.SeasonRole;
import manke.spider.pipeline.douban.DoubanAnimeSessionInfoPipeline;
import manke.spider.processor.AbstractPageProcessor;
import manke.spider.redis.LettuceRedisClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.selector.Selectable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Created by luozhi on 2017/5/21.
 * 抓取增量的番剧数据
 * 从 3种url 获取 信息  1.https://movie.douban.com/j/new_search_subjects?sort=R&range=0,10&tags=%E5%8A%A8%E6%BC%AB&start=0  番剧集
 * 2.https://movie.douban.com/subject/1782004  番剧详细信息
 * 1782004 表示番剧ID
 */
public class DoubanAnimeIncIndexPageProcessor extends AbstractPageProcessor {
    Logger logger = LoggerFactory.getLogger(DoubanAnimeIncIndexPageProcessor.class);


    private final String indexPagePreUrl = "https://movie.douban.com/j/new_search_subjects?sort=R&range=0,10&tags=%E5%8A%A8%E6%BC%AB&start=";

    private final String SEASONURLREDISKEY = "douban:season_urls";

    private Map<String, Set<SeasonRole>> celebrityWorksCache = Maps.newConcurrentMap();

    LettuceRedisClient redisClient;

    public DoubanAnimeIncIndexPageProcessor(LettuceRedisClient redisClient) {

        this.redisClient = redisClient;

    }


    public void process(Page page) {
        String currentUrl = page.getRequest().getUrl();
        if (page.getStatusCode() != 200) {

            if (page.getStatusCode() == 404) {
                page.setSkip(true);
                logger.info("页面不存在 {}", currentUrl);
                return;
            } else {
                logger.info("页面一时不可用，继续爬取 {}", currentUrl);
                page.addTargetRequest(currentUrl);
                page.setSkip(true);
                return;
            }

        }

        logger.info("开始处理url {}", currentUrl);
        //索引页数据
        if (StringUtils.contains(currentUrl, "new_search_subjects")) {
            spiderIndexPage(page, currentUrl);
        }
        //番剧详情页数据
        if (StringUtils.contains(currentUrl, "/subject") && !StringUtils.contains(currentUrl, "celebrities")) {
            page.putField(DoubanConstant.BIZKEY, DoubanConstant.SEASONINFO);
            spiderSeasonInfo(page, currentUrl);

        }
        //番剧详细人员列表
        if (StringUtils.contains(currentUrl, "celebrities") && StringUtils.contains(currentUrl, "subject")) {
            page.putField(DoubanConstant.BIZKEY, DoubanConstant.CELEBRITIES);
            spiderCelebrities(page, currentUrl);
        }
        //职员详细信息
        if (StringUtils.contains(currentUrl, "celebrity") && !StringUtils.contains(currentUrl, "format=text")) {
            page.putField(DoubanConstant.BIZKEY, DoubanConstant.CELEBRITYINFO);
            spiderCelebrityInfo(page, currentUrl);

        }

        //职员作品集
        if (StringUtils.contains(currentUrl, "format=text")) {
            page.putField(DoubanConstant.BIZKEY, DoubanConstant.WORKS);
            spiderCelebrityWorks(page, currentUrl);

        }

    }

    /**
     * 爬取所有番剧url
     */
    private void spiderIndexPage(Page page, String currentUrl) {
        List<Selectable> season_urls = null;

        try {

            season_urls = page.getJson().jsonPath("$.data[*].url").nodes();
            int currentPage = Integer.parseInt(StringUtils.splitByWholeSeparator(currentUrl, "start=")[1]);
            int nextPage = currentPage + 20;
            if (season_urls == null || season_urls.size() == 0 || nextPage >= 200) {

                page.setSkip(true);
                logger.info("spider  to  last page... url {} content is {}", currentUrl, JSON.toJSONString(page));

                return;
            }

            for (Selectable url : season_urls) {
                logger.info("url is {}", url.get());
                if (redisClient.sismember(SEASONURLREDISKEY, url.get())) {
                    logger.info("番剧已爬取过停止爬取, url {}", url.get());
                    page.setSkip(true);
                    return;
                }
                page.addTargetRequest(url.get());
            }
            //获取下一页url 继续爬取
            String nextPageUrl = StringUtils.join(indexPagePreUrl, nextPage);
            page.addTargetRequest(nextPageUrl);
            logger.info("next page url is {}", nextPageUrl);

        } catch (Exception e) {
            logger.error("can not  process url {} json data", page.getRequest().getUrl(), e);
        }
        page.setSkip(true);
    }

    /**
     * 爬取番剧信息
     */
    private void spiderSeasonInfo(Page page, String currentUrl) {
        List<String> contents = page.getHtml().xpath("//div[@id='info']").all();
        //官方网站:
        String webSite = null;
        //制片国家/地区:
        String region = null;
        //语言:
        String lang = null;
        //又名:
        String alias = null;
        //IMDb链接:
        String imdbUrl = null;
        if (contents != null && contents.size() == 1) {

            for (String content : contents) {
                //去除回车
                content = StringUtils.replaceChars(content, '\n', ' ');
                //去除<span class="pl">
                content = StringUtils.remove(content, "<span class=\"pl\">");
                //去除</span>
                content = StringUtils.remove(content, "</span>");
                //根据br 分行
                String[] lines = StringUtils.splitByWholeSeparator(content, "<br>");

                if (lines != null) {

                    for (String line : lines) {
                        if (StringUtils.contains(line, "官方网站:")) {
                            line = StringUtils.deleteWhitespace(line);
                            if (StringUtils.split(line, '"') != null && StringUtils.split(line, '"').length > 1) {
                                webSite = StringUtils.split(line, '"')[1];
                            }
                        }

                        if (StringUtils.contains(line, "制片国家/地区:")) {
                            region = StringUtils.remove(line, "制片国家/地区:");
                            region = StringUtils.deleteWhitespace(region);
                        }

                        if (StringUtils.contains(line, "语言:")) {
                            lang = StringUtils.remove(line, "语言:");
                            lang = StringUtils.deleteWhitespace(lang);
                        }

                        if (StringUtils.contains(line, "又名:")) {
                            alias = StringUtils.remove(line, "又名:");
                            alias = StringUtils.trim(alias);
                        }

                        if (StringUtils.contains(line, "IMDb链接:")) {
                            line = StringUtils.deleteWhitespace(line);
                            if (StringUtils.split(line, '"') != null && StringUtils.split(line, '"').length > 1) {
                                imdbUrl = StringUtils.split(line, '"')[1];
                            }
                        }
                    }

                }

            }
        }

        String jsonInfo = page.getHtml().xpath("//script[@type='application/ld+json']").get();
        jsonInfo = StringUtils.remove(jsonInfo, "<script type=\"application/ld+json\">");
        jsonInfo = StringUtils.remove(jsonInfo, "</script>");
        jsonInfo = StringUtils.remove(jsonInfo, '\n');
        jsonInfo = StringUtils.replace(jsonInfo, "@type", "type");

        JSONObject jsonObject = JSONObject.parseObject(jsonInfo, JSONObject.class);

        String[] names = StringUtils.splitPreserveAllTokens(jsonObject.getString("name"));
        String name = names[0];

        if (names.length > 1) {

            for (int i = 1; i < names.length; i++) {
                alias = StringUtils.join(alias, "/", names[i]);
            }
        }

        String season_id = StringUtils.substringBetween(jsonObject.getString("url"), "/subject/", "/");
        String cover = jsonObject.getString("image");
        String datePublished = jsonObject.getString("datePublished");
        List<String> types = jsonObject.getJSONArray("genre").toJavaList(String.class);

        String duration = StringUtils.remove(jsonObject.getString("duration"), "PT");
        String detail = jsonObject.getString("description");
        String tv = jsonObject.getString("type");

        JSONObject aggregateRating = jsonObject.getJSONObject("aggregateRating");

        String ratingCount = aggregateRating.getString("ratingCount");

        String ratingValue = aggregateRating.getString("ratingValue");

        Map<String, Object> result = Maps.newHashMap();

        result.put("webSite", webSite);
        result.put("region", region);
        result.put("lang", lang);
        result.put("alias", alias);
        result.put("imdbUrl", imdbUrl);
        result.put("name", name);
        result.put("season_id", season_id);
        result.put("cover", cover);
        result.put("datePublished", datePublished);
        result.put("types", types);
        result.put("duration", duration);
        result.put("detail", detail);
        result.put("tv", tv);
        result.put("ratingCount", ratingCount);
        result.put("ratingValue", ratingValue);

        logger.info("解析番剧详细页结果 ", JSON.toJSONString(result));
        page.putField("result", result);
        redisClient.sadd(SEASONURLREDISKEY, currentUrl);
        page.addTargetRequest(page.getRequest().getUrl() + "celebrities");
    }

    /**
     * 爬取番剧职员列表
     */
    private void spiderCelebrities(Page page, String currentUrl) {
        List<Selectable> celebritiesDivs = page.getHtml().xpath("//div[@class='mod-bd celebrities']/div").nodes();
        List<Selectable> celebritiesLis = null;
        if (celebritiesDivs == null) {
            logger.info("{}  celebrities empty", currentUrl);
            return;
        }
        Map<String, Object> result = Maps.newHashMap();
        String season_id = StringUtils.substringBetween(currentUrl, "subject/", "/celebrities");
        result.put("season_id", season_id);
        logger.info("开始获取番剧{}的演员数据", season_id);
        String title = null;
        List<Map<String, String>> celebrities = null;
        Map<String, String> celebrity = null;
        String celebrity_id = null;
        String name = null;
        String role = null;
        for (Selectable div : celebritiesDivs) {

            title = div.xpath("//h2/text()").get();
            title = StringUtils.split(title)[1];
            title = StringUtils.lowerCase(title);
            celebritiesLis = div.xpath("//ul/li").nodes();
            if (celebritiesLis == null) {
                logger.info("番剧id {} title {} 内容为空", season_id, title);
                continue;
            }
            celebrities = Lists.newArrayList();
            for (Selectable li : celebritiesLis) {

                celebrity = Maps.newHashMap();

                celebrity_id = li.xpath("//a[@class='name']/@href").get();
                celebrity_id = StringUtils.substringBetween(celebrity_id, "celebrity/", "/");
                name = li.xpath("//a[@class='name']/text()").get();
                role = li.xpath("//span[@class='role']/text()").get();
                celebrity.put("celebrity_id", celebrity_id);
                celebrity.put("name", name);
                celebrity.put("role", role);
                celebrities.add(celebrity);
                logger.info("添加爬取职员信息url {} ", li.xpath("//a[@class='name']/@href").get());
                page.addTargetRequest(li.xpath("//a[@class='name']/@href").get());

            }
            logger.info("番剧id {} title {} 内容为 {}", season_id, title, JSON.toJSONString(celebrities));
            result.put(title, celebrities);
        }
        page.putField("result", result);
    }

    /**
     * 爬取职员信息
     */
    private void spiderCelebrityInfo(Page page, String currentUrl) {
        String celebrity_id = StringUtils.substringBetween(currentUrl, "/celebrity/", "/");

        Selectable contentDiv = page.getHtml().xpath("//div[@id='content']");

        List<Selectable> infoLi = contentDiv.xpath("//ul/li").nodes();

        String name = contentDiv.xpath("//h1/text()").get();

        String picUrl = contentDiv.xpath("//a[@class='nbg']/@href").get();

        String gender = null;

        String constellation = null;

        String birthday = null;

        String homeTown = null;

        String job = null;

        String alias = null;

        String imdbUrl = null;

        if (infoLi != null) {
            for (Selectable li : infoLi) {

                if (StringUtils.equals(li.xpath("//span/text()").get(), "性别")) {
                    gender = StringUtils.remove(li.xpath("li/text()").get(), ":");
                    gender = StringUtils.trim(gender);
                } else if (StringUtils.equals(li.xpath("//span/text()").get(), "星座")) {
                    constellation = StringUtils.remove(li.xpath("li/text()").get(), ":");
                    constellation = StringUtils.trim(constellation);
                } else if (StringUtils.equals(li.xpath("//span/text()").get(), "出生日期")) {
                    birthday = StringUtils.remove(li.xpath("li/text()").get(), ":");
                    birthday = StringUtils.trim(birthday);
                } else if (StringUtils.equals(li.xpath("//span/text()").get(), "出生地")) {
                    homeTown = StringUtils.remove(li.xpath("li/text()").get(), ":");
                    homeTown = StringUtils.trim(homeTown);
                } else if (StringUtils.equals(li.xpath("//span/text()").get(), "职业")) {
                    job = StringUtils.remove(li.xpath("li/text()").get(), ":");
                    job = StringUtils.trim(job);
                } else if (StringUtils.equals(li.xpath("//span/text()").get(), "更多外文名")) {
                    alias = StringUtils.remove(li.xpath("li/text()").get(), ":");
                    alias = StringUtils.trim(alias);
                } else if (StringUtils.equals(li.xpath("//span/text()").get(), "imdb编号")) {
                    imdbUrl = li.xpath("//a/@href").get();
                }

            }
        }
        Map<String, Object> result = Maps.newHashMap();
        result.put("celebrity_id", celebrity_id);
        result.put("name", name);
        result.put("picUrl", picUrl);
        result.put("gender", gender);
        result.put("constellation", constellation);
        result.put("birthday", birthday);
        result.put("homeTown", homeTown);
        result.put("job", job);
        result.put("alias", alias);
        result.put("imdbUrl", imdbUrl);

        logger.info("职员id{} 详细信息为 {}", celebrity_id, JSON.toJSONString(result));
        page.putField("result", result);

        String workListUrl = currentUrl + "movies?sortby=vote&format=text";

        page.addTargetRequest(workListUrl);

        logger.info("添加职员id {} 作品集 url {} ", celebrity_id, workListUrl);

    }


    /**
     * 爬取职员作品集
     */
    private void spiderCelebrityWorks(Page page, String currentUrl) {

        List<String> celebrityUrls = page.getHtml().xpath("//table[@summary]/tbody/tr/td/a/@href").all();
        List<String> roles = page.getHtml().xpath("//table[@summary]/tbody/tr/td[@headers='mc_role']/text()").all();
        int index = 0;
        String celebrity_id = StringUtils.substringBetween(currentUrl, "celebrity/", "/movies");
        Map<String, Object> result = Maps.newHashMap();
        Set<SeasonRole> cacheSr = Sets.newConcurrentHashSet();
        Set<SeasonRole> seasonRoles = Sets.newConcurrentHashSet();
        SeasonRole seasonRole = null;
        String season_id = null;
        String role = null;
        for (String url : celebrityUrls) {
            seasonRole = new SeasonRole();
            season_id = StringUtils.substringBetween(url, "subject/", "/");
            role = roles.get(index++);
            seasonRole.setSeasonId(season_id);
            seasonRole.setRole(role);
            seasonRoles.add(seasonRole);
        }

        celebrityWorksCache.putIfAbsent(celebrity_id, seasonRoles);

        cacheSr = celebrityWorksCache.get(celebrity_id);

        if (cacheSr == null) {

            //其他线程已爬完
            page.setSkip(true);
            logger.info("职员作品集结果已比其他线程处理 职员id {}", celebrity_id);
            return;

        }

        cacheSr.addAll(seasonRoles);


        String nextUrl = page.getHtml().xpath("//span[@class='next']/a/@href").get();
        if (StringUtils.isEmpty(nextUrl)) {
            logger.info("获取职员详细作品集已到最后一页");
            result.put("celebrity_id", celebrity_id);
            //该值如果为空说明其他线程以处理完
            result.put("seasonRoles", celebrityWorksCache.get(celebrity_id));
            if (result.get("seasonRoles") == null) {
                page.setSkip(true);
                logger.info("职员作品集结果已比其他线程处理 职员id {}", celebrity_id);
            } else {
                celebrityWorksCache.remove(celebrity_id);
                logger.info("获取职员详细作品集结果 {}", JSON.toJSONString(result));
                page.putField("result", result);
            }

        } else {
            nextUrl = StringUtils.join(StringUtils.split(currentUrl, '?')[0], nextUrl);
            logger.info("获取职员详细作品集下一页url {}", nextUrl);
            page.addTargetRequest(nextUrl);
            page.setSkip(true);
        }
    }


    @Override
    public Site getSite() {
        Site site = super.getSite();
        Set<Integer> acceptStatCode = new HashSet<>();
        for (int i = 0; i <= 700; i++) {
            acceptStatCode.add(i);
        }
        //防止豆瓣给爬虫喂毒药
        site.setAcceptStatCode(acceptStatCode);
        site.setSleepTime(1500);
        site.setDisableCookieManagement(true);
        return site;
    }

    public static void main(String[] args) {

        LettuceRedisClient redisClient = new LettuceRedisClient("192.168.31.233", 6379);
        redisClient.init();
        Spider.create(new DoubanAnimeIncIndexPageProcessor(redisClient))
                .setScheduler(new LinkedBlockScheduler())
                .addUrl("https://movie.douban.com/j/new_search_subjects?sort=R&range=0,10&tags=%E5%8A%A8%E6%BC%AB&start=0")
                //.addUrl("https://movie.douban.com/subject/26339248/")
                //.addUrl("https://movie.douban.com/subject/26339248/celebrities")
                //.addUrl("https://movie.douban.com/celebrity/1054439/")
                //.addUrl("https://movie.douban.com/celebrity/1348459/movies?sortby=vote&format=text")
                .addPipeline(new DoubanAnimeSessionInfoPipeline())
                .thread(2).run();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                redisClient.close();
            }
        }));


    }
}
