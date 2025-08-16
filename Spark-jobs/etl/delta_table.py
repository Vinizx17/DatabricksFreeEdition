def create_delta_table(spark, table_name="rh.rh_dataset.empregados"):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Id_Funcionario INT,
        Idade INT,
        Genero INT,
        Estado_Civil INT,
        Departamento STRING,
        Funcao STRING,
        Viagem_Frequencia INT,
        Valor_Diaria FLOAT,
        Indice_Envolvimento_Trabalho INT,
        Nivel_Satisfacao_Trabalho INT,
        Salario_Mensal FLOAT,
        Salario_Anual FLOAT,
        Numero_Empresas_Anteriores INT,
        Disponivel_Hora_Extra INT,
        Percentual_Ultimo_Aumento_Salario FLOAT,
        Aval_Performance INT,
        Anos_Experiencia INT,
        Numero_Treinamentos_Ano_Anterior INT,
        Anos_na_Empresa INT,
        Anos_Funcao_Atual INT,
        Anos_Desde_Ultima_Promocao INT,
        Anos_com_Gerente_Atual INT,
        Satisfacao_Ponderada FLOAT,
        processed_at TIMESTAMP
    )
    USING DELTA
    """)
