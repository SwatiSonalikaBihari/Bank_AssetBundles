import dlt
@dlt.table
def transfermed():
    return spark.range(10)