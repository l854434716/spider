use manke_dw;
--统计地区番剧数量
select region_name,count(1)  num  from   manke_ods.t_ods_youku_anime_season_info  dim
 join  t_anime_region_dim  on  ( dim.region_code=t_anime_region_dim.code)
 group by  region_name order by num desc;


 --播放量top 10
 select  title,play_count,pub_time from  youku_season_dim  dim
 join  youku_season_watch_fact fact
 on  (dim.season_sk=fact.season_sk and  dim.expiry_date='2030-04-07' and  fact.day='2018-04-22')
 order  by   play_count  desc  limit 10;


--评分top 50
 select  title,score,pub_time from  youku_season_dim  dim
 join  youku_season_watch_fact fact
 on  (dim.season_sk=fact.season_sk and  dim.expiry_date='2030-04-07' and fact.day='2018-04-24' )
 order  by  score    desc  limit 50;

 --
  select  title,comment_num,play_count,pub_time from  youku_season_dim  dim
  join  youku_season_watch_fact fact
  on  (dim.season_sk=fact.season_sk and  dim.expiry_date='2030-04-07' and fact.day='2018-04-29' )
  order  by  comment_num    desc  limit 52;