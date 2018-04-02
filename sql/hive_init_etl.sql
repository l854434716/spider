use manke_dw;
--清空表
truncate table  bibi_season_dim;
truncate table  bibi_season_watch_fact;
truncate table  qq_season_dim;
truncate table  qq_season_watch_fact;
truncate table  youku_season_dim;
truncate table  youku_season_watch_fact;

--装载bibi_season_dim
set  hivevar:cur_date=current_date();
set  hivevar:max_date=cast('2030-04-07' as  date);
insert  into bibi_season_dim
select
    row_number() over (order by  t_ods.season_id) + t_dim.sk_max,
    t_ods.badge,t_ods.copyright,t_ods.isfinish,t_ods.title,t_ods.price,
    t_ods.pub_time,t_ods.bangumi_title,t_ods.season_title,t_ods.season_id,
    1,${hivevar:cur_date},${hivevar:max_date}
from manke_ods.t_ods_bibi_anime_season_info t_ods
cross join (select coalesce (max(season_sk),0) sk_max  from  bibi_season_dim) t_dim;