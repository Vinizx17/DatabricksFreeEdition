CREATE OR REPLACE TABLE tempo_empresa AS
SELECT
    Funcao,
    AVG(Anos_na_Empresa) AS Media_Anos_Empresa,
    AVG(Anos_Funcao_Atual) AS Media_Anos_Funcao
FROM empregados
GROUP BY Funcao;
