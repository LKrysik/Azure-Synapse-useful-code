# Linked Service to serverless sql db(no parameters allowed)

```
{
    "name": "LINKED_SERVICE_SL_SQL_LS",
    "properties": {
        "annotations": [],
        "type": "AzureSqlDatabase",
        "typeProperties": {
            "connectionString": "Integrated Security=False;Encrypt=True;Connection Timeout=30;Data Source=!!!yourserverlesssqldb!!!!-ondemand.sql.azuresynapse.net;Initial Catalog=SERVERLESS_SQL_DB"
        },
        "connectVia": {
            "referenceName": "AutoResolveIntegrationRuntime",
            "type": "IntegrationRuntimeReference"
        }
    }
}
```
