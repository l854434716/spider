use manke_ods;

load  data local inpath '/home/cloudsmaker/mr-app/date' overwrite into table t_ods_date;
load  data local inpath '/home/cloudsmaker/mr-app/region' overwrite into table t_ods_anime_region;
load  data local inpath '/home/cloudsmaker/mr-app/type' overwrite into table t_ods_anime_type;
load  data local inpath '/home/cloudsmaker/mr-app/bibi_anime_season'overwrite into table t_ods_bibi_anime_season_info;
load  data local inpath '/home/cloudsmaker/mr-app/qq_anime_season' overwrite into table t_ods_qq_anime_season_info;
load  data local inpath '/home/cloudsmaker/mr-app/youku_anime_season' overwrite into table t_ods_youku_anime_season_info;