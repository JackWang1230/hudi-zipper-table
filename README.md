# zipper-table-hudi

bootstrap class:
            initial job: cn.wr.zipper.table.datastream.InitialGoodsSkuZipperTableHudiJob
            incremental job: cn.wr.zipper.table.datastream.IncrGoodsSkuZipperTableHudiJob
            
the path where the jar package needs to be placed:
             /home/wr/gc_source_sku_zipper_hudi_jar
             
# execute code
sh run_initial_job.sh
sh run_incremental_job.sh
            
