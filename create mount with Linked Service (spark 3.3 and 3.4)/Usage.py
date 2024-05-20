linked_service_name = "LINKED_SERVICE_NAME_LS"
storage_container_name = "RAW"
table_path_to_load = "taxidata/delta/2024/05/01/"


#Mount ADLS and get mount and synfs path
mount_point, synfs_path = mount_from_linkedservice(linked_service_name,storage_container_name)

# Define synfs path
source_file_base_path = f"{synfs_path}{table_path_to_load}"

#Read data
df = spark.read.parquet(source_file_base_path)
