CREATE OR REPLACE TABLE estado_civil AS
SELECT
    Funcao,
    CASE Estado_Civil
        WHEN 0 THEN 'Solteiro'
        WHEN 1 THEN 'Casado'
    END AS Estado_Civil,
    COUNT(*) AS Quantidade_Funcionarios
FROM empregados
GROUP BY Funcao, Estado_Civil;