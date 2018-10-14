#!/bin/bash
#导入每日数据至数据仓库
export HADOOP_VERSION="2.6.0-cdh5.13.0"
export JAVA_HOME="/opt/cloudera-manager/jdk1.8/"
base_dir=$(dirname $0)
day=`date "+%Y-%m-%d"`

hive -hivevar day="${day}" -f  $base_dir/../sql/hive_increment_load_ods.sql

hive -hivevar day="${day}" -f  $base_dir/../sql/hive_increment_dim_etl.sql

hive -hivevar day="${day}" -f  $base_dir/../sql/hive_increment_fact_etl.sql

hive  -f  $base_dir/../sql/hive_increment_load_season_types.sql

_year=`date "+%Y"`
_month=`date "+%m"`
_day=`date "+%d"`
hive -hivevar year="${_year}" -hivevar month="${_month}"  -hivevar day="${_day}"  -f  $base_dir/../sql/hive_increment_daily_fact_etl.sql