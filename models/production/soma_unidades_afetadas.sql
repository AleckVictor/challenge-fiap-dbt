SELECT 
    Bairro,
    [Data],
    SUM(NumUnidadeConsumidora) AS unidades_afetadas
FROM 
    {{ref('interrupcoes_pluviometria')}}
GROUP BY 
    Bairro, 
    [Data]
