package manke.spider.model.es;

import java.util.ArrayList;

/**
 * anime  id、title、 alias、actors and  staff info model
 */
public class AnimeNameStaffModel {


    /**
     * The Season id.
     */
    private   String   season_id;

    /**
     * The Title.
     */
    private  String   title;

    /**
     * The Alias.
     */
    private ArrayList<String>  alias;

    /**
     * The Actors.
     */
    private ArrayList<AnimeActorModel>   actors;

    /**
     * The Staff.
     */
    private String   staff;

    /**
     * The Original_Website
     */
    private  String   original_website;


    /**
     * Gets season id.
     *
     * @return the season id
     */
    public String getSeason_id() {
        return season_id;
    }

    /**
     * Sets season id.
     *
     * @param season_id the season id
     */
    public void setSeason_id(String season_id) {
        this.season_id = season_id;
    }

    /**
     * Gets title.
     *
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Sets title.
     *
     * @param title the title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Gets alias.
     *
     * @return the alias
     */
    public ArrayList<String> getAlias() {
        return alias;
    }

    /**
     * Sets alias.
     *
     * @param alias the alias
     */
    public void setAlias(ArrayList<String> alias) {
        this.alias = alias;
    }

    /**
     * Gets actors.
     *
     * @return the actors
     */
    public ArrayList<AnimeActorModel> getActors() {
        return actors;
    }

    /**
     * Sets actors.
     *
     * @param actors the actors
     */
    public void setActors(ArrayList<AnimeActorModel> actors) {
        this.actors = actors;
    }

    /**
     * Gets staff.
     *
     * @return the staff
     */
    public String getStaff() {
        return staff;
    }

    /**
     * Sets staff.
     *
     * @param staff the staff
     */
    public void setStaff(String staff) {
        this.staff = staff;
    }

    /**
     * Gets original website.
     *
     * @return the original website
     */
    public String getOriginal_website() {
        return original_website;
    }

    /**
     * Sets original website.
     *
     * @param original_website the original website
     */
    public void setOriginal_website(String original_website) {
        this.original_website = original_website;
    }
}
