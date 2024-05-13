def get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def get_access_token(tenant_id: str, service_principal_id: str, service_principal_secret: str, scope: str) -> str:

    # Authorization URL
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'

    # POST parameters
    data = {
        'grant_type': 'client_credentials',
        'client_id': service_principal_id,
        'client_secret': service_principal_secret,
        'resource': scope
    }

    # Send request
    response = requests.post(url, data=data)

    if response.status_code == 200:
        token = response.json()['access_token']
        print("Token generated correctly")
    else:
        print("Error while generating the token:", response.status_code)

    return token


def get_shipments_exclusion(spark: SparkSession, write_database: str) -> DataFrame:

    dbutils = get_dbutils(spark)

    scope = 'https://database.windows.net/'
    tenant_id = "<tenant_id>"
    service_principal_id = dbutils.secrets.get(scope="<secret_scope>", key="<service_principal_id")
    service_principal_secret = dbutils.secrets.get(scope="secret_scope", key="<service_principal_secret>")


    access_token = get_access_token(tenant_id, service_principal_id, service_principal_secret, scope)

    synapse_sql_url = "jdbc:sqlserver://<Serverless SQL endpoint>"
    database_name = "<database_name>"
    db_table = "<table/view name>"
    encrypt = "true"
    host_name_in_certificate = "*.sql.azuresynapse.net"

    df = spark.read \
        .format("jdbc") \
        .option("url", synapse_sql_url) \
        .option("dbtable", db_table) \
        .option("databaseName", database_name) \
        .option("accessToken", access_token) \
        .option("encrypt", encrypt) \
        .option("hostNameInCertificate", host_name_in_certificate) \
        .load()

    df = df.select(F.col("<column_name>"))
    return df
