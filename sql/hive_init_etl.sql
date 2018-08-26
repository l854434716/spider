use manke_dw;
--清空表
truncate table  bibi_season_dim;
truncate table  bibi_season_watch_fact;
truncate table  qq_season_dim;
truncate table  qq_season_watch_fact;
truncate table  youku_season_dim;
truncate table  youku_season_watch_fact;

--装载bibi_season_dim
set  hivevar:cur_date="2018-07-18";
set  hivevar:max_date=cast('2030-04-07' as  date);
insert  into bibi_season_dim
select
    row_number() over (order by  t_ods.season_id) + t_dim.sk_max,
    t_ods.badge,t_ods.copyright,t_ods.isfinish,t_ods.title,t_ods.price,
    t_ods.pub_time,t_ods.bangumi_title,t_ods.season_title,t_ods.season_id,
    1,${hivevar:cur_date},${hivevar:max_date}
from manke_ods.t_ods_bibi_anime_season_info t_ods
cross join (select coalesce (max(season_sk),0) sk_max  from  bibi_season_dim) t_dim;

--装载qq_season_dim
insert  into  qq_season_dim
select
     row_number() over(order by t_ods.season_id) + t_dim.sk_max,
     t_ods.badge,t_ods.isfinish,t_ods.title,t_ods.pub_year,t_ods.season_id,
     1,${hivevar:cur_date},${hivevar:max_date}
from  manke_ods.t_ods_qq_anime_season_info t_ods
cross  join (select coalesce (max(season_sk),0) sk_max from qq_season_dim) t_dim;

--装载 youku_season_dim
insert  into  youku_season_dim
select
     row_number() over(order by t_ods.season_id) + t_dim.sk_max,
     t_ods.articulation,t_ods.edition,t_ods.badge,t_ods.screen_time,t_ods.isfinish,
     t_ods.title,t_ods.exclusive,t_ods.pub_time,t_ods.season_id,
     t_ods.limit_age_up,t_ods.limit_age_down,
     1,${hivevar:cur_date},${hivevar:max_date}
from manke_ods.t_ods_youku_anime_season_info t_ods
cross join (select  coalesce (max(season_sk),0) sk_max from youku_season_dim)  t_dim;



--装载 bibi_season_watch_fact
insert into bibi_season_watch_fact
PARTITION(day='2018-07-18')
select season_sk,region_sk,date_sk,coins,danmaku_count,favorites,score,score_critic_num,play_count
from  manke_ods.t_ods_bibi_anime_season_info t_ods , bibi_season_dim , t_date_dim ,t_anime_region_dim
where t_ods.season_id=bibi_season_dim.season_id and t_ods.region_code=t_anime_region_dim.code
      and t_date_dim.day_col=${hivevar:cur_date};

--装载 qq_season_watch_fact

insert into qq_season_watch_fact
PARTITION(day='2018-07-18')
select season_sk,region_sk,date_sk,score,play_count
from  manke_ods.t_ods_qq_anime_season_info t_ods, qq_season_dim, t_date_dim,t_anime_region_dim
where t_ods.season_id=qq_season_dim.season_id and t_ods.region_code=t_anime_region_dim.code
      and t_date_dim.day_col=${hivevar:cur_date};


--装载 youku_season_watch_fact

insert into youku_season_watch_fact
PARTITION(day='2018-07-18')
select season_sk,region_sk,date_sk,score,play_count,comment_num
from  manke_ods.t_ods_youku_anime_season_info t_ods , youku_season_dim , t_date_dim ,t_anime_region_dim
where t_ods.season_id=youku_season_dim.season_id and t_ods.region_code=t_anime_region_dim.code
      and t_date_dim.day_col=${hivevar:cur_date};

load  data local inpath './../data/season_type/bibi/'overwrite into table bibi_season_type_associate;
load  data local inpath './../data/season_type/qq/' overwrite  into table qq_season_type_associate;
load  data local inpath './../data/season_type/youku/' overwrite into table youku_season_type_associate;