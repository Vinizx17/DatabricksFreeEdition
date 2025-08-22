CREATE OR REPLACE TABLE salario_funcao AS
SELECT
    Funcao,
    CASE Genero
        WHEN 0 THEN 'Feminino'
        WHEN 1 THEN 'Masculino'
    END AS Genero,
    AVG(Salario_Mensal) AS Media_Salario_Mensal,
    AVG(Salario_Anual) AS Media_Salario_Anual
FROM empregados
