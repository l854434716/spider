use manke_ods;

load  data local inpath '/tmp/manke/bibi_anime_season/${day}'overwrite into table t_ods_bibi_anime_season_info;
load  data local inpath '/tmp/manke/qq_anime_season/${day}' overwrite into table t_ods_qq_anime_season_info;
load  data local inpath '/tmp/manke/youku_anime_season/${day}' overwrite into table t_ods_youku_anime_season_info;