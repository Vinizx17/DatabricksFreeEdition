from etl.spark_init import spark_init
from etl.catalog_schema import create_catalog_and_schema
from etl.load_csv import load_csv
from etl.transform import transform
from etl.delta_table import create_delta_table
from etl.merge_delta import merge_delta
from etl.comments import add_comments

# Inicia Spark
spark = spark_init()

# Cria catálogo e schema
create_catalog_and_schema(spark)

# Carrega CSV
df = load_csv(spark)

# Transforma dados
df_final = transform(df)

# Cria tabela Delta
create_delta_table(spark)

# MERGE com lógica de upsert
merge_delta(df_final, spark)

# Adiciona comentários no Unity Catalog
add_comments(spark)

# Mostra dados
df_final.show(truncate=False)
