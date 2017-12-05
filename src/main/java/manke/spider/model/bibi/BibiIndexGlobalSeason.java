package manke.spider.model.bibi;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by luozhi on 2017/5/22.
 *
 * bibi index 页面上的每条番剧信息
 *
 * {"cover":"http://i0.hdslb.com/bfs/bangumi/e7f7e5b07441da16e382ea4563f82f63f8632626.jpg","favorites":64590,"is_finish":1,"newest_ep_index":"46","pub_time":1467627900,"season_id":"5025","season_status":2,"title":"智龙迷城X","total_count":51,"update_time":1495448702,"url":"http://bangumi.bilibili.com/anime/5025","week":"1"}
 */
public class BibiIndexGlobalSeason {

    private String cover;
    private int favorites;
    @JsonProperty("is_finish")
    private int isFinish;
    @JsonProperty("newest_ep_index")
    private String newestEpIndex;
    @JsonProperty("pub_time")
    private int pubTime;
    @JsonProperty("season_id")
    private String seasonId;
    @JsonProperty("season_status")
    private int seasonStatus;
    private String title;
    @JsonProperty("total_count")
    private int totalCount;
    @JsonProperty("update_time")
    private int updateTime;
    private String url;
    private String week;
    public void setCover(String cover) {
        this.cover = cover;
    }
    public String getCover() {
        return cover;
    }

    public void setFavorites(int favorites) {
        this.favorites = favorites;
    }
    public int getFavorites() {
        return favorites;
    }

    public void setIsFinish(int isFinish) {
        this.isFinish = isFinish;
    }
    public int getIsFinish() {
        return isFinish;
    }

    public void setNewestEpIndex(String newestEpIndex) {
        this.newestEpIndex = newestEpIndex;
    }
    public String getNewestEpIndex() {
        return newestEpIndex;
    }

    public void setPubTime(int pubTime) {
        this.pubTime = pubTime;
    }
    public int getPubTime() {
        return pubTime;
    }

    public void setSeasonId(String seasonId) {
        this.seasonId = seasonId;
    }
    public String getSeasonId() {
        return seasonId;
    }

    public void setSeasonStatus(int seasonStatus) {
        this.seasonStatus = seasonStatus;
    }
    public int getSeasonStatus() {
        return seasonStatus;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    public String getTitle() {
        return title;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
    public int getTotalCount() {
        return totalCount;
    }

    public void setUpdateTime(int updateTime) {
        this.updateTime = updateTime;
    }
    public int getUpdateTime() {
        return updateTime;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    public String getUrl() {
        return url;
    }

    public void setWeek(String week) {
        this.week = week;
    }
    public String getWeek() {
        return week;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("cover", cover)
                .append("favorites", favorites)
                .append("isFinish", isFinish)
                .append("newestEpIndex", newestEpIndex)
                .append("pubTime", pubTime)
                .append("seasonId", seasonId)
                .append("seasonStatus", seasonStatus)
                .append("title", title)
                .append("totalCount", totalCount)
                .append("updateTime", updateTime)
                .append("url", url)
                .append("week", week)
                .toString();
    }
}
