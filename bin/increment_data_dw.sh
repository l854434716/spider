#!/bin/bash
#导入每日数据至数据仓库

base_dir=$(dirname $0)
day=`date "+%Y-%m-%d"`

hive -hivevar day="${day}" -f  $base_dir/../sql/hive_increment_load_ods.sql

hive -hivevar cur_date="${day}" -f  $base_dir/../sql/hive_increment_dim_etl.sql

hive -hivevar cur_date="${day}" -f  $base_dir/../sql/hive_increment_fact_etl.sql