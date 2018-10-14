use  manke_dw;
--执行该sql 脚本必须设置变量
set hivevar:yesterday_date=cast(date_sub(current_date, 1) as  String);
set hivevar:current_date_str=cast(current_date as  String);

--装载 bibi_season_daily_watch_fact
insert  overwrite  table  bibi_season_daily_watch_fact
partition(y=${year},m=${month},d=${day})
select a.season_sk season_sk,a.region_sk region_sk, a.date_sk date_sk,b.coins-a.coins coins, b.danmaku_count-a.danmaku_count danmaku_count,
b.favorites-a.favorites  favorites, b.score score, b.score_critic_num-a.score_critic_num score_critic_num, b.play_count-a.play_count play_count
from bibi_season_watch_fact a  join  bibi_season_watch_fact b   on  a.season_sk=b.season_sk  and  a.day=${yesterday_date} and  b.day=${current_date_str};

--装载 qq_season_daily_watch_fact

insert  overwrite  table  qq_season_daily_watch_fact
partition(y=${year},m=${month},d=${day})
select a.season_sk season_sk,a.region_sk region_sk, a.date_sk date_sk, b.score score, b.play_count-a.play_count play_count
from qq_season_watch_fact a  join  qq_season_watch_fact b   on  a.season_sk=b.season_sk  and  a.day=${yesterday_date} and  b.day=${current_date_str};


--装载 youku_season_daily_watch_fact

insert  overwrite  table  youku_season_daily_watch_fact
partition(y=${year},m=${month},d=${day})
select a.season_sk season_sk,a.region_sk region_sk, a.date_sk date_sk, b.score score, b.play_count-a.play_count play_count,b.comment_num-a.comment_num comment_num
from youku_season_watch_fact a  join  youku_season_watch_fact b   on  a.season_sk=b.season_sk  and  a.day=${yesterday_date} and  b.day=${current_date_str};