mount_point = mount_from_linkedservice(target_linked_service, target_container)

#Get job id to read data with synfs
job_id = mssparkutils.env.getJobId()

#Spark 3.3 !!! 
synfs_path = f"synfs:/{job_id}{mount_point}{target_path}/{target_table_name}"


dataframe.coalesce(1).write.mode("overwrite").partitionBy(*partitionBy).parquet(synfs_path)
