#!/bin/sh
# -----------------------------------------------------------------------------
#

. ./setEnv.sh
export CLASSPATH=$CLASSPATH
base_dir=$(dirname $0)
$JAVA_HOME/bin/java -Xms256m -Xmx1024m  -Djava.awt.headless=true -Dbase.dir=$base_dir/../ -Dlogback.configurationFile=$base_dir/../conf/logback.xml -Djava.library.path=$base_dir/../lib/sigar-native/ -Dfile.encoding=UTF-8 -Dspider.home=`pwd` $1
