#!/bin/sh
# -----------------------------------------------------------------------------
#

. ./setEnv.sh
export CLASSPATH=$CLASSPATH
base_dir=$(dirname $0)
$JAVA_HOME/bin/java -Xms256m -Xmx1024m -Duser.timezone=GMT+08 -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml  -Dfile.encoding=UTF-8 -Dspider.home=`pwd` "manke.spider.processor.bibi.BibiAnimeIncrCommentProcessor"
