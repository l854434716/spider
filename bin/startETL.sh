#!/bin/sh
# -----------------------------------------------------------------------------
#

. ./startSpider.sh
day=`date "+%Y-%m-%d"`
mkdir -p /tmp/manke/bibi_anime_season/${day}
mkdir -p /tmp/manke/qq_anime_season/${day}
mkdir -p /tmp/manke/youku_anime_season/${day}
base_dir=$(dirname $0)
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.bibi.MongoBibiSeasonJob"
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.qq.MongoQqSeasonJob"
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.youku.MongoYoukuSeasonJob"


$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.bibi.MongoBibiTypesJob"
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.qq.MongoQqTypesJob"
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.youku.MongoYoukuTypesJob"