Parameters used in the Glue Job:
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer 
--conf spark.sql.hive.convertMetastoreParquet=false
--datalake-formats hudi
--bucket_name <enter_your_bucket_name>
--ip_path <enter_input_raw_files_path>
--op_path <enter_output_silver_files_path>
