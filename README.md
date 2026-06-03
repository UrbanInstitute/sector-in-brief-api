# nccs-dataexplorer-api
API that uses AWS Athena to query NCCS data archives

```
Query Structure:
{
    "User": {
        "name": name,
        "email": email
    },
    "variables": [
        "column 1", 
        "column 2"
    ],
    "filters": {
        "column 1" : [
            "value 1",
            "value 2"
        ]
    }
}
```

A lambda function:
1. Creates an SQL Query from "variables" and "filters"
2. Executes the SQL Query on a parquet file
3. Outputs the result to an NCCS bucket
4. Sends the email provided with the query an update letting them know their 
data is ready

Architecture: [link](https://app.mural.co/t/nccs5536/m/nccs5536/1727200792498/976ee20b094682081356c7567ddf64101ab522b1?sender=uacf4ce686ecf976eab732586)