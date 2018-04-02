use manke_dw;
truncate  table t_anime_type_dim;
insert into  t_anime_type_dim
select  t_ods_anime_type.*
from manke_ods.t_ods_anime_type as t_ods_anime_type;