#!/bin/sh
# -----------------------------------------------------------------------------
#

. ./startSpider.sh

base_dir=$(dirname $0)
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.bibi.MongoBibiSeasonJob"
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.qq.MongoQqSeasonJob"
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.job.youku.MongoYoukuSeasonJob"
