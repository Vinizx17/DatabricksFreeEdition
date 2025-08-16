from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

def merge_delta(df_final, spark, table_name="rh.rh_dataset.empregados"):
    delta_table = DeltaTable.forName(spark, table_name)
    match_condition = "target.Id_Funcionario = source.Id_Funcionario"
    compare_cols = [c for c in df_final.columns if c != "Id_Funcionario"]
    update_condition = " OR ".join([f"target.{c} <> source.{c}" for c in compare_cols])

    delta_table.alias("target").merge(
        df_final.alias("source"),
        match_condition
    ).whenMatchedUpdate(
        condition=update_condition,
        set={**{c: f"source.{c}" for c in df_final.columns}, "processed_at": "current_timestamp()"}
    ).whenNotMatchedInsert(
        values={**{c: f"source.{c}" for c in df_final.columns}, "processed_at": "current_timestamp()"}
    ).execute()
