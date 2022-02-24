# generic-zipper-table
## 部署顺序指南
   1. 增量代码部署需要在初始化代码完成后执行
   2. 何时从kafka开始接入增量数据？
      1. 当初始化脚本开始执行时，kafka 此时需要开始接入数据( t(kafka)<=t(初始化脚本))
   3. 当初始化脚本运行后，查看yarn集群,待结束后,代码会将数据库的一份快照数据同步到hudi,
   4. 此时执行增量实时接入脚本,查看yarn集群运行状态,代码部署完成
   
## 部署执行脚本命令
### 初始化脚本
   1. sh run_zipper_init_job.sh job_name zipper_name.sql
### 增量实时入湖脚本
   1. sh run_zipper_incr_job.sh job_name zipper_name.sql
   
### 数据运行层级结构
   generic_zipper_hudi_jar路径下
   ├── bin
   │ ├── run_zipper_incr_job.sh
   │ ├── run_zipper_init_job.sh
   ├── conf
   │ └── application_prod.properties
   ├── jars
   │ └── generic-zipper-table-1.0-SNAPSHOT.jar
   └── sql
       └── zipper_demo.sql