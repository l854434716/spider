package manke.spider.model.douban;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;

public class SeasonRole {

    private String seasonId;

    private String role;


    public String getSeasonId() {
        return seasonId;
    }

    public void setSeasonId(String seasonId) {
        this.seasonId = seasonId;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("seasonId", seasonId)
                .append("role", role)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SeasonRole that = (SeasonRole) o;
        return seasonId.equals(that.seasonId) &&
                role.equals(that.role);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seasonId, role);
    }
}
