def load_csv(spark, path="/Volumes/rh/rh_dataset/dataset/"):
    df = spark.read.option("header", True).csv(path)
    return df
