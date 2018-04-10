#!/bin/bash
#初始化数据仓库
hive  -f  ../sql/hive_dw_table.sql

#初始化数据导入
hive  -f ../sql/hive_init_load_ods.sql
hive  -f ../sql/hive_load_date_dim.sql
hive  -f ../sql/hive_load_region.sql
hive  -f ../sql/hive_load_type_dim.sql
hive  -f ../sql/hive_init_etl.sql