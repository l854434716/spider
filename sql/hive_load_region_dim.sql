use manke_dw;
truncate  table t_anime_region_dim;
insert into  t_anime_region_dim
select  row_number() over (order by  t_ods_anime_region.code) + t2.sk_max,t_ods_anime_region.*
from manke_ods.t_ods_anime_region as t_ods_anime_region
cross join (select coalesce(max(region_sk),0) sk_max from t_anime_region_dim) t2;