"""Spark session utility"""

from pyspark.sql import SparkSession


def get_or_create_spark(
    spark: SparkSession | None = None,
    app_name: str = "MDIF",
) -> SparkSession:
    """Return existing SparkSession or create one with ADLS authentication."""
    if spark is not None:
        spark.sparkContext.setLogLevel("WARN")
        
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            
            client_id = dbutils.secrets.get(
                scope="marspcmdifcinkv",
                key="team-sp-client-id"
            )
            
            client_secret = dbutils.secrets.get(
                scope="marspcmdifcinkv",
                key="team-sp-client-secret"
            )
            
            spark.conf.set(
                "fs.azure.account.auth.type.cdapshareddesacin.dfs.core.windows.net",
                "OAuth"
            )
            
            spark.conf.set(
                "fs.azure.account.oauth.provider.type.cdapshareddesacin.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            )
            
            spark.conf.set(
                "fs.azure.account.oauth2.client.id.cdapshareddesacin.dfs.core.windows.net",
                client_id
            )
            
            spark.conf.set(
                "fs.azure.account.oauth2.client.secret.cdapshareddesacin.dfs.core.windows.net",
                client_secret
            )
            
            spark.conf.set(
                "fs.azure.account.oauth2.client.endpoint.cdapshareddesacin.dfs.core.windows.net",
                "https://login.microsoftonline.com/4bf30310-e4f1-4658-9e34-9e8a5a193ed1/oauth2/token"
            )
        except Exception as e:
            print(f"[spark_utils] Could not configure ADLS: {e}")
        
        return spark

    builder = SparkSession.builder.appName(app_name)
    spark = builder.getOrCreate()
    return get_or_create_spark(spark=spark, app_name=app_name)
