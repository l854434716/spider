#!/bin/bash
#导入每日数据至数据仓库

base_dir=$(dirname $0)
day=`date "+%Y-%m-%d"`

hive -hivever day="${day}" -f  $base_dir/../sql/hive_increment_load_ods.sql
hive  -f  $base_dir/../sql/hive_increment_dim_etl.sql

hive -hivever day="${day}" -f  $base_dir/../sql/hive_increment_fact_etl.sql

echo "${day} increment data2dw task run done" >>$base_dir/../log/shell_task.log