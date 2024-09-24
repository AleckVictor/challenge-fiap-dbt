SELECT 
    Bairro,
    [Data],
    Motivo_Final,
    count(Motivo_Final) AS unidades_afetadas
FROM 
    {{ref('interrupcoes_pluviometria')}}
GROUP BY 
    Bairro, 
    [Data],
    Motivo_Final
