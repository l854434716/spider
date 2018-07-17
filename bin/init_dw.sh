#!/bin/bash
#初始化数据仓库
base_dir=$(dirname $0)

hive  -f  $base_dir/../sql/hive_dw_table.sql

#初始化数据导入
hive  -f $base_dir/../sql/hive_init_load_ods.sql
hive  -f $base_dir/../sql/hive_load_date_dim.sql
hive  -f $base_dir/../sql/hive_load_region_dim.sql
hive  -f $base_dir/../sql/hive_load_type_dim.sql
hive  -f $base_dir/../sql/hive_init_etl.sql