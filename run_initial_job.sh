#!/bin/bash

cd /opt/flink-1.13.2/bin
nohup ./flink run -t yarn-per-job -d -Dyarn.application.queue=root.users.admin -Dyarn.application.name=inital_gc_source_sku_zipper_hudi_job -c cn.wr.zipper.table.datastream.InitialGoodsSkuZipperTableHudiJob /home/wr/gc_source_sku_zipper_hudi_jar/uniondrug-zipper-table-hudi-1.0-SNAPSHOT.jar --conf /home/wr/gc_source_sku_zipper_hudi_jar/application.properties