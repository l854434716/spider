use  manke_dw;
--执行该sql 脚本必须设置变量
set  hivevar:cur_date=cast('${day}' as  date);
set  hivevar:max_date=cast('2030-04-07' as  date);

--装载 bibi_season_watch_fact
insert overwrite table  bibi_season_watch_fact
PARTITION(day='${day}')
select season_sk,region_sk,date_sk,coins,danmaku_count,favorites,score,score_critic_num,play_count
from  manke_ods.t_ods_bibi_anime_season_info t_ods , bibi_season_dim , t_date_dim ,t_anime_region_dim
where t_ods.season_id=bibi_season_dim.season_id and t_ods.region_code=t_anime_region_dim.code
      and t_date_dim.day_col=${hivevar:cur_date} and bibi_season_dim.expiry_date>${cur_date} and bibi_season_dim.effective_date<=${cur_date};

--装载 qq_season_watch_fact

insert overwrite table qq_season_watch_fact
PARTITION(day='${day}')
select season_sk,region_sk,date_sk,score,play_count
from  manke_ods.t_ods_qq_anime_season_info t_ods, qq_season_dim, t_date_dim,t_anime_region_dim
where t_ods.season_id=qq_season_dim.season_id and t_ods.region_code=t_anime_region_dim.code
      and t_date_dim.day_col=${hivevar:cur_date} and qq_season_dim.expiry_date>${cur_date} and qq_season_dim.effective_date<=${cur_date};


--装载 youku_season_watch_fact

insert overwrite table youku_season_watch_fact
PARTITION(day='${day}')
select season_sk,region_sk,date_sk,score,play_count,comment_num
from  manke_ods.t_ods_youku_anime_season_info t_ods , youku_season_dim , t_date_dim ,t_anime_region_dim
where t_ods.season_id=youku_season_dim.season_id and t_ods.region_code=t_anime_region_dim.code
      and t_date_dim.day_col=${hivevar:cur_date} and youku_season_dim.expiry_date>${cur_date} and youku_season_dim.effective_date<=${cur_date};