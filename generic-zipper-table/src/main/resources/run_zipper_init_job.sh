#!/bin/sh
cd /opt/flink-1.13.2/bin
nohup ./flink run -t yarn-per-job -d -Dyarn.application.name=$1 -c cn.wr.datastream.InitialZipperTableHuDiJob /opt/zipper_hudi_jar/jars/generic-zipper-table-1.0-SNAPSHOT.jar --conf /opt/generic_zipper_hudi_jar/conf/application_prod.properties --sqlFIle /opt/generic_zipper_hudi_jar/sql/$2