from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, FloatType

def transform(df):
    # Converter tipos
    df = df.withColumn("Id_Funcionario", col("Id_Funcionario").cast(IntegerType())) \
           .withColumn("Idade", col("Idade").cast(IntegerType())) \
           .withColumn("Valor_Diaria", col("Valor Diaria").cast(FloatType())) \
           .withColumn("Indice_Envolvimento_Trabalho", col("Indice_Envolvimento_Trabalho").cast(IntegerType())) \
           .withColumn("Nivel_Satisfacao_Trabalho", col("Nivel_Satisfacao_Trabalho").cast(IntegerType())) \
           .withColumn("Salario_Mensal", col("Salario_Mensal").cast(FloatType())) \
           .withColumn("Numero_Empresas_Anteriores", col("Numero_Empresas_Anteriores").cast(IntegerType())) \
           .withColumn("Percentual_Ultimo_Aumento_Salario", col("Percentual_Ultimo_Aumento_Salario").cast(FloatType())) \
           .withColumn("Aval_Performance", col("Aval_Performance").cast(IntegerType())) \
           .withColumn("Anos_Experiencia", col("Anos_Experiencia").cast(IntegerType())) \
           .withColumn("Numero_Treinamentos_Ano_Anterior", col("Numero_Treinamentos_Ano_Anterior").cast(IntegerType())) \
           .withColumn("Anos_na_Empresa", col("Anos_na_Empresa").cast(IntegerType())) \
           .withColumn("Anos_Funcao_Atual", col("Anos_Funcao_Atual").cast(IntegerType())) \
           .withColumn("Anos_Desde_Ultima_Promocao", col("Anos_Desde_Ultima_Promocao").cast(IntegerType())) \
           .withColumn("Anos_com_Gerente_Atual", col("Anos_com_Gerente_Atual").cast(IntegerType()))

    # Colunas categóricas
    df = df.withColumn("Genero", when(col("Genero") == "Masculino", 1).otherwise(0)) \
           .withColumn("Estado_Civil", when(col("Estado Civil") == "Casado", 1).otherwise(0)) \
           .withColumn("Viagem_Frequencia", when(col("Viagem") == "Viaja Frequentemente", 2).otherwise(1)) \
           .withColumn("Disponivel_Hora_Extra", when(col("Disponivel_Hora_Extra") == "S", 1).otherwise(0))

    # Colunas derivadas
    df = df.withColumn("Salario_Anual", col("Salario_Mensal") * 12)
    df = df.withColumn("Satisfacao_Ponderada", (col("Nivel_Satisfacao_Trabalho") + col("Indice_Envolvimento_Trabalho")) / 2)

    # Tratar valores nulos
    df = df.fillna({
        "Numero_Empresas_Anteriores": 0,
        "Numero_Treinamentos_Ano_Anterior": 0,
        "Anos_Desde_Ultima_Promocao": 0
    })

    # Selecionar colunas
    columns_to_keep = [
        "Id_Funcionario","Idade","Genero","Estado_Civil","Departamento","Funcao",
        "Viagem_Frequencia","Valor_Diaria","Indice_Envolvimento_Trabalho","Nivel_Satisfacao_Trabalho",
        "Salario_Mensal","Salario_Anual","Numero_Empresas_Anteriores","Disponivel_Hora_Extra",
        "Percentual_Ultimo_Aumento_Salario","Aval_Performance","Anos_Experiencia",
        "Numero_Treinamentos_Ano_Anterior","Anos_na_Empresa","Anos_Funcao_Atual",
        "Anos_Desde_Ultima_Promocao","Anos_com_Gerente_Atual","Satisfacao_Ponderada"
    ]
    return df.select(*columns_to_keep)
