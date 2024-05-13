from collections import defaultdict

# Job id
job_id = mssparkutils.env.getJobId()

# Mount points
mount_point = defaultdict(dict)

linked_service_name = "LINKED_SERVCIE_TO_ADLS_LS"
container = "raw"

# if a mount point has been declared, do not create it again
if not mount_point.get(linked_service_name,{}).get(container,None):
   mount_point[linked_service_name][container] = mount_from_linkedservice(linked_service_name, container)

#spark version 3.3
synfs_path = f"synfs:/{job_id}{mount_point[linked_service_name][container]}/{path}/"

#Write data
dataframe.coalesce(1).write.mode("overwrite").partitionBy(*partitionBy).parquet(synfs_path)
