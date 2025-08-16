from pyspark.sql import SparkSession

def spark_init(app_name="EmployeeAnalysis"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark
