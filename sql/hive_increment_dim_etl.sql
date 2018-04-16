use  manke_dw;
--增量装载数据并更新维度表

--维度表sd1 与 sd2 更新
set  hivevar:max_date=cast('2030-04-07' as  date);
-- bibi
--1.设置删除和isfinised 列上的过期时间

insert  overwrite table  bibi_season_dim
select   t_dim.season_sk,t_dim.badge,t_dim.copyright,t_dim.isfinish,t_dim.title,t_dim.price,
         t_dim.pub_time,t_dim.bangumi_title,t_dim.season_title,t_dim.season_id,
         t_dim.version,t_dim.effective_date,
         case when  t_ods.season_id is null  or  t_dim.isfinish!=t_ods.isfinish then ${cur_date}
              else  t_dim.expiry_date
         end expiry_date
from
(select  *  from   bibi_season_dim   where expiry_date=${max_date} )  t_dim
left join   manke_ods.t_ods_bibi_anime_season_info  t_ods  on (t_dim.season_id=t_ods.season_id)
union all
select  * from  bibi_season_dim  where  expiry_date<${max_date};


--2.处理sdc2
insert   into  bibi_season_dim
select  row_number()  over  (order by  t1.season_id)+sk_max,
        badge,copyright,isfinish,title,price,
        pub_time,bangumi_title,season_title,season_id,
        version,effective_date,expiry_date
from  (

select  t_ods.badge,t_ods.copyright,t_ods.isfinish,t_ods.title,
        t_ods.price,t_ods.pub_time,t_ods.bangumi_title,t_ods.season_title,t_ods.season_id,
        t_dim.version+1 version,${cur_date}  effective_date,${max_date} expiry_date
from
 (select  version ,season_id  from bibi_season_dim  where  expiry_date=${cur_date})  t_dim
 inner join  manke_ods.t_ods_bibi_anime_season_info  t_ods on  (t_dim.season_id=t_ods.season_id)
 left join  bibi_season_dim t_dim1 on (t_dim.season_id=t_dim1.season_id  and  t_dim1.expiry_date=${max_date})
 where  t_dim1.season_sk is  null

)  t1  cross  join   (select  coalesce (max(season_sk),0) sk_max from bibi_season_dim) t2 ;



--3.处理新增数据
insert   into  bibi_season_dim
select  row_number()  over  (order by  t1.season_id)+sk_max,
                badge,copyright,isfinish,title,price,
                pub_time,bangumi_title,season_title,season_id,
                1 version ,${cur_date}  effective_date,${max_date}  expiry_date
from
(select t_ods.* from  manke_ods.t_ods_bibi_anime_season_info t_ods left  join  bibi_season_dim  t_dim on (t_ods.season_id=t_dim.season_id)
 where  t_dim.season_sk is null )  t1  cross  join   (select  coalesce (max(season_sk),0) sk_max from bibi_season_dim) t2 ;


-- qq
--1. 设置删除和isfinish 的过期时间
insert  overwrite table  qq_season_dim
select season_sk, t_dim.badge,t_dim.isfinish,t_dim.title,
       t_dim.pub_year,t_dim.season_id,t_dim.version,t_dim.effective_date,
       case when  t_ods.season_id is null  or  t_dim.isfinish!=t_ods.isfinish then ${cur_date}
                      else  t_dim.expiry_date
       end expiry_date
from
(select  * from  qq_season_dim  where  expiry_date=${max_date}) t_dim
left join manke_ods.t_ods_qq_anime_season_info  t_ods  on t_dim.season_id=t_ods.season_id
union all
select  *  from  qq_season_dim  where  expiry_date<${max_date};



--2.处理 scd2
insert into  qq_season_dim
select  row_number() over(order  by t1.season_id )+sk_max,
        t1.*
from (
  select  t_ods.badge,t_ods.isfinish,t_ods.title,
          t_ods.pub_year,t_ods.season_id,
          t_dim.version+1 version,
          ${cur_date}  effective_date,${max_date} expiry_date
  from
  (select  version ,season_id  from  qq_season_dim  where  effective_date=${cur_date}) t_dim
  inner join  manke_ods.t_ods_qq_anime_season_info  t_ods on (t_dim.season_id=t_ods.season_id)
  left  join  qq_season_dim  t_dim1  on (t_dim.season_id=t_dim1.season_id  and t_dim1.effective_date=${max_date} )
  where  t_dim1.season_sk is   null
) t1  cross  join   (select  coalesce (max(season_sk),0) sk_max from qq_season_dim) t2 ;



--3.处理新增数据
insert into qq_season_dim
select row_number()  over  (order  by  season_id) + sk_max,
       badge,isfinish,title,pub_year,season_id,
       1 version,${cur_date}  effective_date,${max_date}  expiry_date
from
(select  t_ods.*  from   manke_ods.t_ods_qq_anime_season_info t_ods
left join  qq_season_dim  t_dim  on t_ods.season_id=t_dim.season_id
where  t_dim.season_id is null ) t1  cross  join   (select  coalesce (max(season_sk),0) sk_max from qq_season_dim) t2 ;



--youku

--1. 设置删除和isfinish 的过期时间
insert  overwrite  table  youku_season_dim
select  season_sk,t_dim.articulation,t_dim.edition,
        t_dim.badge,t_dim.screen_time,t_dim.isfinish,
        t_dim.title,t_dim.exclusive,t_dim.pub_time,t_dim.season_id,
        t_dim.limit_age_up,t_dim.limit_age_down,
        t_dim.version,t_dim.effective_date,
        case when  t_ods.season_id is null  or  t_dim.isfinish!=t_ods.isfinish then ${cur_date}
             else  t_dim.expiry_date
        end expiry_date
from
(select * from  youku_season_dim  where   effective_date=${max_date}  ) t_dim
left  join  manke_ods.t_ods_youku_anime_season_info  t_ods  on (t_dim.season_id=t_ods.season_id)
union all
select  *  from  youku_season_dim  where  effective_date<${max_date};



--2.更新 scd2
insert  into youku_season_dim
select  row_number()  over  (order  by  t1.season_id) +sk_max,
        t1.*
from (
  select  t_ods.*,
          t_dim.version +1  version,
          ${cur_date}  effective_date,${max_date} expiry_date
 from
 (select version ,season_id from  youku_season_dim  where  effective_date=${cur_date} ) t_dim
 inner  join  manke_ods.t_ods_youku_anime_season_info  t_ods  on (t_dim.season_id=t_ods.season_id)
 left  join  youku_season_dim  t_dim1 on  (t_dim1.season_id=t_dim.season_id)
 where  t_dim1.season_sk is  null

) t1  cross  join   (select  coalesce(max(season_sk),0) sk_max  from  youku_season_dim) t2;

--3.处理新增数据
select  row_number()  over  (order  by  t1.season_id) +sk_max,
         t1.*,
         1 version,${cur_date}  effective_date,${max_date}  expiry_date
from(
    select  t_ods.*  from   manke_ods.t_ods_youku_anime_season_info  t_ods
    left join  youku_season_dim  t_dim  on t_ods.season_id=t_dim.season_id
    where   t_dim.season_id is  null
) t1  cross join   (select  coalesce(max(season_sk),0) sk_max  from  youku_season_dim) t2;