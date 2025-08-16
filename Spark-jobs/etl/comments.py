def add_comments(spark, table_name="rh.rh_dataset.empregados"):
    spark.sql(f"COMMENT ON COLUMN {table_name}.Genero IS '0=Feminino, 1=Masculino'")
    spark.sql(f"COMMENT ON COLUMN {table_name}.Estado_Civil IS '0=Solteiro, 1=Casado'")
    spark.sql(f"COMMENT ON COLUMN {table_name}.Disponivel_Hora_Extra IS '0=Não, 1=Sim'")
    spark.sql(f"COMMENT ON COLUMN {table_name}.Viagem_Frequencia IS '1=Viaja Raramente, 2=Viaja Frequentemente'")
    spark.sql(f"COMMENT ON COLUMN {table_name}.processed_at IS 'Timestamp atualizado apenas se o registro foi modificado'")
