def extract_storage_account_name(url):
    # Split the URL by '.' and get the first part
    return url.split('.')[0].split('//')[-1]


def mount_from_linkedservice(linked_service_name, storage_container_name):
    endpoint = json.loads(mssparkutils.credentials.getPropertiesAll(linked_service_name))['Endpoint']

    storage_account_name = extract_storage_account_name(endpoint)

    abfss_path = f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net"

    mount_point = "/"+storage_account_name+"/"+storage_container_name+"/"

    #Unmount if exists
    try:
        mssparkutils.fs.unmount(mount_point)
    except Exception as e:
        pass

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Temporarily mount the prod container using to access all the required fields
            mssparkutils.fs.mount(
                f"{abfss_path}", 
                f"{mount_point}", 
                # We use here the linked service credentials
                {"linkedService":f"{linked_service_name}"} 
            )
            print(f"Mounted: {mount_point} for {abfss_path}")
            logger.info(f"Mounted: {mount_point} for {abfss_path}")
            break
        except Exception as e:
            print(f"Mounting error on attempt {attempt+1}. Error: {e}. Retrying...")
            #Unmount if exists
            try:
                mssparkutils.fs.unmount(mount_point)
            except Exception as e:
                pass
            time.sleep(30 ** attempt)  # Exponential backoff

    spark_version = spark.version
    main_version = ".".join(spark_version.split(".")[:2])
    job_id = mssparkutils.env.getJobId()
    # Define synfs path for spark version
    if (main_version=='3.3'):
        source_file_base_path = f"synfs:/{job_id}{mount_point}"
    elif (main_version=='3.4'):
        source_file_base_path = f"synfs:/notebook/{job_id}{mount_point}"
    else:
        source_file_base_path = f"synfs:/notebook/{job_id}{mount_point}"

    # return mount and synfs path
    return mount_point, source_file_base_path
