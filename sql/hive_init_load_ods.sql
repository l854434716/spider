use manke_ods;

load  data local inpath './../data/date' overwrite into table t_ods_date;
load  data local inpath './../data/region' overwrite into table t_ods_anime_region;
load  data local inpath './../data/type' overwrite into table t_ods_anime_type;
load  data local inpath './../data/bibi_anime_season'overwrite into table t_ods_bibi_anime_season_info;
load  data local inpath './../data/qq_anime_season' overwrite into table t_ods_qq_anime_season_info;
load  data local inpath './../data/youku_anime_season' overwrite into table t_ods_youku_anime_season_info;