CREATE OR REPLACE TABLE satisfacao AS
SELECT
    Funcao,
    AVG(Nivel_Satisfacao_Trabalho) AS Media_Satisfacao,
    AVG(Indice_Envolvimento_Trabalho) AS Media_Engajamento
FROM empregados
GROUP BY Funcao;
