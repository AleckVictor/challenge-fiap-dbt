WITH AneelFiltrado AS (
    SELECT
        DscConjuntoUnidadeConsumidora,
        DscTipoInterrupcao,
        CONVERT(DATE, DatInicioInterrupcao) AS DataSemHora, 
        DatInicioInterrupcao,
        DatFimInterrupcao,
        NumUnidadeConsumidora,
        NumAno,
        Motivo_Final
    FROM
        {{ref('aneel_interrupcoes_tratado') }}
),
MergedData AS (
    SELECT
        cgesp.Bairro,
        cgesp.Data,
        cgesp.Pluviometria,
        aneel.DscConjuntoUnidadeConsumidora,
        aneel.DscTipoInterrupcao,
        aneel.DatInicioInterrupcao,
        aneel.DatFimInterrupcao,
        aneel.NumUnidadeConsumidora,
        aneel.NumAno,
        aneel.Motivo_Final
    FROM
        {{ref('cgesp_tratado') }} AS cgesp 
    LEFT JOIN
        AneelFiltrado AS aneel
    ON
        cgesp.Bairro = aneel.DscConjuntoUnidadeConsumidora
        AND cgesp.Data = aneel.DataSemHora
    where cgesp.data >= '2020-01-01'
),
final as (
	SELECT
	    Bairro,
	    Data,
	    Pluviometria,
	    DscConjuntoUnidadeConsumidora,
	    DscTipoInterrupcao,
	    DatInicioInterrupcao,
	    DatFimInterrupcao,
	    NumUnidadeConsumidora,
	    NumAno,
	    Motivo_Final
	FROM
	    MergedData)
select * from final