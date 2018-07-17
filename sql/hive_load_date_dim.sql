use manke_dw;
truncate table t_date_dim;
insert into  t_date_dim
select  row_number() over (order by  t_ods_date.day_col) + t2.sk_max,t_ods_date.*
from manke_ods.t_ods_date as t_ods_date
cross join (select coalesce(max(date_sk),0) sk_max from t_date_dim) t2;