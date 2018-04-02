
-- 创建hive 数据仓库表
drop  database  if  exists  manke_ods  cascade;

create  database  manke_ods;


use  manke_ods;

drop  table  if  exists  t_ods_date ;
CREATE  TABLE t_ods_date (
  day date  COMMENT 'date,yyyy-mm-dd',
  weekofyear tinyint  COMMENT '一年的第几周',
  month tinyint COMMENT '一年的第几月',
  month_name  varchar(9)  COMMENT 'month name',
  quarter tinyint  COMMENT '季度',
  year smallint  COMMENT 'year'
) COMMENT '时间表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

drop  table  if  exists  t_ods_anime_region ;
CREATE TABLE `t_ods_anime_region` (
  `code` smallint COMMENT '地区代号',
  `region_name` varchar(50) COMMENT '地区名称'
) COMMENT '地区表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

drop  table  if  exists  t_ods_anime_type ;
CREATE TABLE `t_ods_anime_type` (
  `id` int ,
  `type_name` varchar(20)  COMMENT 'type name'
) COMMENT 'anime type table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

drop  table  if  exists  t_ods_bibi_anime_season_info ;
CREATE TABLE `t_ods_bibi_anime_season_info` (
  `allow_download` tinyint COMMENT '是否允许下载',
  `area_limit` tinyint  COMMENT '地区限制',
  `badge` varchar(50)   COMMENT 'vip标记',
  `copyright` varchar(50)   COMMENT '版权信息',
  `coins` int   COMMENT '番剧硬币个数',
  `cover` varchar(255)   COMMENT '封面链接',
  `danmaku_count` int  COMMENT '弹幕个数',
  `favorites` int COMMENT '喜爱人数',
  `isfinish` tinyint  COMMENT '是否完结',
  `score` float   COMMENT '评分',
  `score_critic_num` int   COMMENT '评分人数',
  `title` varchar(100)   COMMENT '番剧名称-番剧全名',
  `price` decimal(10,0)   COMMENT '番剧价格',
  `play_count` bigint    COMMENT '播放次数',
  `pub_time` timestamp   COMMENT '上架时间',
  `bangumi_title` varchar(100)   COMMENT '番剧系列名',
  `season_title` varchar(100)   COMMENT '番剧副名称',
  `webplayurl` varchar(255)   COMMENT '番剧播放地址',
  `season_id` int   COMMENT '番剧season id',
  `region_code` smallint   COMMENT '地区代码'
) COMMENT 'bibi season info table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '$';

drop  table  if  exists  t_ods_qq_anime_season_info ;
CREATE TABLE `t_ods_qq_anime_season_info` (
  `badge` varchar(50)   COMMENT 'vip标记',
  `cover` varchar(255)   COMMENT '封面链接',
  `isfinish` tinyint   COMMENT '是否完结',
  `score` float   COMMENT '评分',
  `title` varchar(100)   COMMENT '番剧名称-番剧全名',
  `play_count` bigint    COMMENT '播放次数',
  `pub_year` smallint   COMMENT '出品年份',
  `webplayurl` varchar(255)   COMMENT '番剧播放地址',
  `season_id` varchar(30)   COMMENT '番剧season id',
  `region_code` smallint  COMMENT '地区代码'
)COMMENT 'qq season info table'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '$';

drop  table  if  exists  t_ods_youku_anime_season_info;
 CREATE TABLE `t_ods_youku_anime_season_info` (
   `articulation` varchar(20)   COMMENT '番剧清晰度',
   `edition` varchar(20)   COMMENT '番剧版本 比如 TV版',
   `badge` varchar(50)   COMMENT 'vip标记',
   `screen_time` date   COMMENT '番剧上映时间',
   `thumbs_up_num` int   COMMENT '点赞个数',
   `isfinish` tinyint  COMMENT '是否完结',
   `score` float   COMMENT '评分',
   `cover` varchar(255)   COMMENT '封面链接',
   `title` varchar(100)   COMMENT '番剧名称-番剧全名',
   `exclusive` varchar(30)   COMMENT '独家播放信息',
   `play_count` bigint   COMMENT '播放次数',
   `pub_time` date   COMMENT '上架时间',
   `webplayurl` varchar(255)   COMMENT '番剧播放地址',
   `season_id` varchar(30)   COMMENT '番剧season id',
   `region_code` smallint  COMMENT '地区代码',
   `limit_age_up` smallint  COMMENT '适用人群年龄上限',
   `limit_age_down` smallint   COMMENT '适用人群年龄下限',
   `comment_num` bigint   COMMENT '评论个数'
 )COMMENT 'youku season info table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '$';


--创建维度表

drop  database  if  exists  manke_dw  cascade;

create database manke_dw;

use manke_dw;

drop  table  if  exists  t_date_dim;
CREATE  TABLE t_date_dim (
  date_sk  int  COMMENT 'surrogate key',
  day date  COMMENT 'date,yyyy-mm-dd',
  weekofyear tinyint  COMMENT '一年的第几周',
  month tinyint COMMENT '一年的第几月',
  month_name  varchar(9)  COMMENT 'month name',
  quarter tinyint  COMMENT '季度',
  year smallint  COMMENT 'year'
) COMMENT '时间维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

drop  table  if  exists  t_anime_region_dim;
CREATE TABLE `t_anime_region_dim` (
  region_sk int  COMMENT 'surrogate key',
  `code` smallint COMMENT '地区代号',
  `region_name` varchar(50) COMMENT '地区名称'
) COMMENT '地区表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

drop  table  if  exists  t_anime_type_dim;
CREATE TABLE `t_anime_type_dim` (
  `id` int ,
  `type_name` varchar(20)  COMMENT 'type name'
) COMMENT 'anime type table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


drop table  if  exists   bibi_season_dim;

create  table  bibi_season_dim(
    season_sk  int comment 'surrogate key',
   `badge` varchar(50)   COMMENT 'vip标记',
   `copyright` varchar(50)   COMMENT '版权信息',
   `isfinish` tinyint  COMMENT '是否完结',
   `title` varchar(100)   COMMENT '番剧名称-番剧全名',
   `price` decimal(10,0)   COMMENT '番剧价格',
   `pub_time` timestamp   COMMENT '上架时间',
   `bangumi_title` varchar(100)   COMMENT '番剧系列名',
   `season_title` varchar(100)   COMMENT '番剧副名称',
   `season_id` int   COMMENT '番剧season id',
    version int  comment '数据版本',
    effective_date  date comment 'effective_date',
    expiry_date  date comment 'expiry_date'
) comment 'bibi_season_dim  table '
clustered by  (season_sk)  into  8 buckets
stored  as  orc;


drop table  if  exists   bibi_season_watch_fact;

create table bibi_season_watch_fact (

    season_sk  int comment 'season surrogate key',

    region_sk int  COMMENT 'region surrogate key',

    date_sk  int  COMMENT 'date surrogate key',
    `coins` int   COMMENT '番剧硬币个数',
    `danmaku_count` int  COMMENT '弹幕个数',
    `favorites` int COMMENT '喜爱人数',
    `score` float   COMMENT '评分',
    `score_critic_num` int   COMMENT '评分人数',
    `play_count` bigint    COMMENT '播放次数'
)comment 'bibi_season_watch_fact  table '
partitioned by (day string)
clustered by  (season_sk)  into  8 buckets
stored  as  orc;


drop table  if  exists   qq_season_dim;
create  table  qq_season_dim(
    season_sk  int comment 'surrogate key',
   `badge` varchar(50)   COMMENT 'vip标记',
   `isfinish` tinyint   COMMENT '是否完结',
   `title` varchar(100)   COMMENT '番剧名称-番剧全名',
   `pub_year` smallint   COMMENT '出品年份',
   `season_id` varchar(30)   COMMENT '番剧season id',
   `region_code` smallint  COMMENT '地区代码',
    version int  comment '数据版本',
    effective_date  date comment 'effective_date',
    expiry_date  date comment 'expiry_date'
) comment 'qq_season_dim  table '
clustered by  (season_sk)  into  8 buckets
stored  as  orc;


drop  table  if  exists  qq_season_watch_fact;

create  table  qq_season_watch_fact(
    season_sk  int comment 'surrogate key',

    region_sk int  COMMENT 'region surrogate key',

    date_sk  int  COMMENT 'date surrogate key',

    `score` float   COMMENT '评分',

    `play_count` bigint    COMMENT '播放次数'

)comment 'qq_season_watch_fact  table '
partitioned by (day string)
clustered by  (season_sk)  into  8 buckets
stored  as  orc;



drop table  if  exists   youku_season_dim;

create  table  youku_season_dim(
     season_sk  int comment 'surrogate key',
    `articulation` varchar(20)   COMMENT '番剧清晰度',
    `edition` varchar(20)   COMMENT '番剧版本 比如 TV版',
    `badge` varchar(50)   COMMENT 'vip标记',
    `screen_time` date   COMMENT '番剧上映时间',
    `isfinish` tinyint  COMMENT '是否完结',
    `title` varchar(100)   COMMENT '番剧名称-番剧全名',
    `exclusive` varchar(30)   COMMENT '独家播放信息',
    `pub_time` date   COMMENT '上架时间',
    `season_id` varchar(30)   COMMENT '番剧season id',
    `region_code` smallint  COMMENT '地区代码',
    `limit_age_up` smallint  COMMENT '适用人群年龄上限',
    `limit_age_down` smallint   COMMENT '适用人群年龄下限',
    version int  comment '数据版本',
    effective_date  date comment 'effective_date',
    expiry_date  date comment 'expiry_date'
) comment 'youku_season_dim  table '
clustered by  (season_sk)  into  8 buckets
stored  as  orc;


drop  table  if  exists  youku_season_watch_fact;

create  table  youku_season_watch_fact(
    season_sk  int comment 'surrogate key',

    region_sk int  COMMENT 'region surrogate key',

    date_sk  int  COMMENT 'date surrogate key',

   `score` float   COMMENT '评分',

   `play_count` bigint   COMMENT '播放次数',

   `comment_num` bigint   COMMENT '评论个数'

)comment 'youku_season_watch_fact  table '
partitioned by (day string)
clustered by  (season_sk)  into  8 buckets
stored  as  orc;