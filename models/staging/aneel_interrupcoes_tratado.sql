WITH NormalizedMotivo AS (
    SELECT
        DscConjuntoUnidadeConsumidora,
        DscTipoInterrupcao,
        DatInicioInterrupcao,
        DatFimInterrupcao, 
        NumUnidadeConsumidora,
        NumAno,
        CASE
            WHEN LOWER(DscFatoGeradorInterrupcao) LIKE '%meio ambiente%' THEN
                LTRIM(RTRIM(
                    REPLACE( 
                        SUBSTRING(
                            DscFatoGeradorInterrupcao,
                            CHARINDEX('meio ambiente', LOWER(DscFatoGeradorInterrupcao)) + LEN('meio ambiente'),
                            LEN(DscFatoGeradorInterrupcao)
                        ),
                        ';', ''  
                    )
                ))
            ELSE 'Indefinido'
        END AS Motivo_Final
    FROM 
        {{ref('aneel_interrupcoes')}}
),
PadronizaMotivo AS (
    SELECT
        DscConjuntoUnidadeConsumidora,
        DscTipoInterrupcao,
        DatInicioInterrupcao,
        DatFimInterrupcao,
        NumUnidadeConsumidora,
        NumAno,
        CASE
            WHEN LEFT(LTRIM(Motivo_Final), 1) = '-' THEN LTRIM(SUBSTRING(Motivo_Final, 2, LEN(Motivo_Final)))
            ELSE Motivo_Final
        END AS Motivo_Limpo
    FROM NormalizedMotivo
),
MappedMotivo AS (
    SELECT
        DscConjuntoUnidadeConsumidora,
        DscTipoInterrupcao,
        DatInicioInterrupcao,
        DatFimInterrupcao,
        NumUnidadeConsumidora,
        NumAno,
        CASE
            WHEN LOWER(Motivo_Limpo) = 'arvore ou vegetacao' THEN 'Arvore ou Vegetação'
            WHEN LOWER(Motivo_Limpo) = 'animais' THEN 'Animais'
            WHEN LOWER(Motivo_Limpo) = 'descarga atmosferica' THEN 'Descarga Atmosférica'
            WHEN LOWER(Motivo_Limpo) = 'erosao' THEN 'Erosão'
            WHEN LOWER(Motivo_Limpo) = 'inundacao' THEN 'Inundação'
            WHEN LOWER(Motivo_Limpo) = 'queima ou incendio' THEN 'Queima ou Incêndio'
            WHEN LOWER(Motivo_Limpo) = 'vento' THEN 'Vento'
            ELSE Motivo_Limpo 
        END AS Motivo_Final_Padronizado
    FROM PadronizaMotivo
),
BairrosFiltrados AS (
    SELECT *
    FROM MappedMotivo
    WHERE
        Motivo_Final_Padronizado != 'Indefinido' 
        AND DscConjuntoUnidadeConsumidora IN (
            'CASA VERDE', 'JAÇANÃ', 'VILA GUILHERME', 'PIRITUBA', 'VILA FORMOSA',
            'GUAIANAZES', 'ITAQUERA', 'ITAIM', 'MOOCA', 'MATEUS', 'TIRADENTES',
            'BUTANTÃ', 'LAPA', 'PINHEIROS', 'SANTO AMARO', 'VILA MARIANA', 'PARELHEIROS'
        )  
),
final AS (
    SELECT
        DscConjuntoUnidadeConsumidora,
        DscTipoInterrupcao,
        DatInicioInterrupcao,
        DatFimInterrupcao,
        NumUnidadeConsumidora,
        NumAno,
        Motivo_Final_Padronizado AS Motivo_Final
    FROM BairrosFiltrados
)
SELECT *
FROM final