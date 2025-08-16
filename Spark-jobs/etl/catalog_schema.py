def create_catalog_and_schema(spark):
    spark.sql("CREATE CATALOG IF NOT EXISTS rh")
    spark.sql("CREATE SCHEMA IF NOT EXISTS rh.rh_dataset")
