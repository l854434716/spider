use  manke_dw;
--增量装载数据并更新维度表

--维度表sd1 与 sd2 更新

set  hivevar:cur_date=current_date();
set  hivevar:max_date=cast('2030-04-07' as  date);
-- bibi
--1.创建临时表保存ods 与 dim 表 连接的中间结果
DROP TABLE IF EXISTS manke_ods.t_dw_ods_bibi_season_tmp;
create table manke_ods.t_dw_ods_bibi_season_tmp  as
select season_sk,t_dim.isfinish dim_isfinish, version,effective_date,expiry_date,
 t_ods.badge,t_ods.copyright,t_ods.isfinish,t_ods.title,t_ods.price,
 t_ods.pub_time,t_ods.bangumi_title,t_ods.season_title,t_ods.season_id
 from
 manke_ods.t_ods_bibi_anime_season_info  t_ods  left join
 bibi_season_dim   t_dim  on(t_ods.season_id=t_dim.season_id);


--2.当t_dw_ods_bibi_season_tmp 表season_sk 为null的时候为新增数据 season_sk 不为null 并且 dim_isfinish 与 isfinish 不相等则是变化数据

insert  overwrite table bibi_season_dim
select  season_sk,badge,copyright,isfinish,title,price,
                  pub_time,bangumi_title,season_title,season_id,
                  version,effective_date,expiry_date
from (

    select
    row_number() over (order by  t.season_id) + t_dim.sk_max  season_sk,
    badge,copyright,isfinish,title,price,
    pub_time,bangumi_title,season_title,season_id,
    version,effective_date,expiry_date
    from (
        select
            badge,copyright,isfinish,title,price,
            pub_time,bangumi_title,season_title,season_id,
            1 version ,${hivevar:cur_date} effective_date,${hivevar:max_date} expiry_date
        from manke_ods.t_dw_ods_bibi_season_tmp
        where season_sk is null
        union all
        select
            badge,copyright,isfinish,title,price,
            pub_time,bangumi_title,season_title,season_id,
            version+1  version,${hivevar:cur_date}  effective_date,${hivevar:max_date} expiry_date
        from manke_ods.t_dw_ods_bibi_season_tmp
        where season_sk is not  null and dim_isfinish!=isfinish


    ) t  cross join  (select coalesce (max(season_sk),0) sk_max  from  bibi_season_dim) t_dim

    union all

    select
      season_sk,badge,copyright,isfinish,title,price,
      pub_time,bangumi_title,season_title,season_id,
      version,effective_date,${hivevar:cur_date}  expiry_date
    from bibi_season_dim
    where  bibi_season_dim.season_sk in (
     select  season_sk from manke_ods.t_dw_ods_bibi_season_tmp where season_sk is not null and  dim_isfinish!=isfinish
    )

    union all

    select
      season_sk,badge,copyright,isfinish,title,price,
      pub_time,bangumi_title,season_title,season_id,
      version,effective_date,expiry_date
    from bibi_season_dim
    where  bibi_season_dim.season_sk not in (
     select  season_sk from manke_ods.t_dw_ods_bibi_season_tmp where season_sk is not null and  dim_isfinish!=isfinish
    )

) x ;



-- qq
DROP TABLE IF EXISTS manke_ods.t_dw_ods_qq_season_tmp;

create  table  manke_ods.t_dw_ods_qq_season_tmp as
select  season_sk,t_dim.isfinish dim_isfinish, version,effective_date,expiry_date,
t_ods.badge,t_ods.isfinish,t_ods.title,t_ods.pub_year,t_ods.season_id
from manke_ods.t_ods_qq_anime_season_info  t_ods  left join  qq_season_dim  t_dim
on (t_ods.season_id=t_dim.season_id);


insert overwrite table  qq_season_dim

select
  row_number() over (order by t.season_id ) + t_dim.sk_max season_sk,
  badge,isfinish,title,pub_year,season_id,version,effective_date,expiry_date
  from
  (select  badge,isfinish,title,pub_year,season_id,1 version,${hivevar:cur_date} effective_date,${hivevar:max_date} expiry_date
   from manke_ods.t_dw_ods_qq_season_tmp where  season_sk is null
   union all
   select  badge,isfinish,title,pub_year,season_id,version+1  version,${hivevar:cur_date} effective_date,${hivevar:max_date} expiry_date
   from manke_ods.t_dw_ods_qq_season_tmp where  season_sk is not null  and  dim_isfinish!=isfinish
   ) t  cross join (select  coalesce(max(season_sk),0) sk_max  from  qq_season_dim) t_dim
union all
    select season_sk,badge,isfinish,title,pub_year,season_id,version,effective_date,${hivevar:cur_date} expiry_date
    from qq_season_dim where qq_season_dim.season_sk in (
      select season_sk  from manke_ods.t_dw_ods_qq_season_tmp  where  season_sk is not null and  dim_isfinish!=isfinish
    )
union all
    select season_sk,badge,isfinish,title,pub_year,season_id,version,effective_date,expiry_date
    from qq_season_dim where qq_season_dim.season_sk not in (
      select season_sk  from manke_ods.t_dw_ods_qq_season_tmp  where  season_sk is not null and  dim_isfinish!=isfinish
    );

--youku
DROP TABLE IF EXISTS manke_ods.t_dw_ods_youku_season_tmp;

create table  manke_ods.t_dw_ods_youku_season_tmp  as
 select  season_sk,t_dim.isfinish dim_isfinish, version,effective_date,expiry_date,
         t_ods.articulation,t_ods.edition,t_ods.badge,t_ods.screen_time,t_ods.isfinish,
         t_ods.title,t_ods.exclusive,t_ods.pub_time,t_ods.season_id,
         t_ods.limit_age_up,t_ods.limit_age_down
 from manke_ods.t_ods_youku_anime_season_info  t_ods  left join  youku_season_dim  t_dim
 on (t_ods.season_id=t_dim.season_id);

insert overwrite table  youku_season_dim

 select
   row_number() over (order by t.season_id)+ t_dim.sk_max  season_sk,
   articulation,edition,badge,screen_time,isfinish,
   title,exclusive,pub_time,season_id,
   limit_age_up,limit_age_down,
   version,effective_date,expiry_date
   from(

     select  articulation,edition,badge,screen_time,isfinish,
              title,exclusive,pub_time,season_id,
              limit_age_up,limit_age_down,
              1 version ,${hivevar:cur_date} effective_date ,${hivevar:max_date} expiry_date
      from  manke_ods.t_dw_ods_youku_season_tmp where season_sk is null
      union all
      select articulation,edition,badge,screen_time,isfinish,
             title,exclusive,pub_time,season_id,
             limit_age_up,limit_age_down,
             version+1 version,${hivevar:cur_date} effective_date,${hivevar:max_date} expiry_date
       from manke_ods.t_dw_ods_youku_season_tmp where  season_sk is not null  and  dim_isfinish!=isfinish


   ) t  cross join  (select coalesce(max(season_sk),0) sk_max from youku_season_dim)  t_dim
 union all
 select  season_sk,articulation,edition,badge,screen_time,isfinish,
         title,exclusive,pub_time,season_id,
         limit_age_up,limit_age_down,
         version,effective_date,${hivevar:cur_date} expiry_date
  from  youku_season_dim where   youku_season_dim.season_sk in (
   select season_sk  from manke_ods.t_dw_ods_youku_season_tmp  where  season_sk is not null and  dim_isfinish!=isfinish
  )
 union all
 select  season_sk,articulation,edition,badge,screen_time,isfinish,
         title,exclusive,pub_time,season_id,
         limit_age_up,limit_age_down,
         version,effective_date,expiry_date
  from  youku_season_dim where   youku_season_dim.season_sk not  in (
   select season_sk  from manke_ods.t_dw_ods_youku_season_tmp  where  season_sk is not null and  dim_isfinish!=isfinish
  );


