SELECT 
    Bairro,
    [Data],
    count(DscConjuntoUnidadeConsumidora) AS unidades_afetadas
FROM 
    {{ref('interrupcoes_pluviometria')}}
GROUP BY 
    Bairro, 
    [Data]