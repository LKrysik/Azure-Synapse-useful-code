def getJDBCdataWithLinkedService(linked_service_name, tableName) -> DataFrame:
    """ 
    Retrieves data from a specified table in a SQL database via JDBC using credentials from a linked service in Azure Synapse.

    This function extracts the database endpoint, name, and access token from the specified linked service. 
    It then constructs a JDBC URL and uses it to read the specified table into a DataFrame using Spark's JDBC capabilities.

    Parameters:
        linked_service_name (str): The name of the linked service in Azure Synapse which contains the connection information.
        tableName (str): The name of the table from which to fetch the data. Ex: "dbo.v_table_name"

    Returns:
        DataFrame: A DataFrame containing the data from the specified table in the database.
    """
    db_properties={}  
    linked_service_params = json.loads(mssparkutils.credentials.getPropertiesAll(linked_service_name))
    endpoint = linked_service_params['Endpoint']
    database = linked_service_params['Database']
    db_properties["accessToken"] = mssparkutils.credentials.getConnectionStringOrCreds(linked_service_name)
    db_properties["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    #print(f"Load data from Database: {database}, table: {tableName}")
    df = spark.read.jdbc(f"jdbc:sqlserver://{endpoint};databaseName={database}",tableName,properties=db_properties)

    return df
