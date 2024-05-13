#############################################################################################################

def extract_storage_account_name(url):
    # Split the URL by '.' and get the first part
    return url.split('.')[0].split('//')[-1]


#############################################################################################################

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

    try:
        # Temporarily mount the container using to access all the required fields
        mssparkutils.fs.mount(
            f"{abfss_path}", 
            f"{mount_point}", 
            # We use here the linked service credentials
            {"linkedService":f"{linked_service_name}"} 
        )
        print(f"Mounted: {mount_point} for {abfss_path}")
        return mount_point
    except Exception as e:
        # Handle exceptions
        print(f"An error occurred: {e}")
        return False
