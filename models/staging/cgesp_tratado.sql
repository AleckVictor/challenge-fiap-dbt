
WITH AdjustedBairros AS (
    SELECT
        *,
        CASE
            WHEN Bairro IS NULL THEN NULL
            WHEN CHARINDEX(' - ', Bairro) > 0 THEN LTRIM(RTRIM(SUBSTRING(Bairro, CHARINDEX(' - ', Bairro) + LEN(' - '), LEN(Bairro))))
            ELSE LTRIM(RTRIM(Bairro))
        END AS Bairro_Ajustado
    FROM {{ref('pluviometria_cgesp')}}
),
final as (
	SELECT
	    CASE
	        WHEN Bairro_Ajustado = 'Casa Verde' THEN 'CASA VERDE'
	        WHEN Bairro_Ajustado = 'Jaçanã / Tremembé' THEN 'JAÇANÃ'
	        WHEN Bairro_Ajustado = 'Vl. Maria / Guilherme' THEN 'VILA GUILHERME'
	        WHEN Bairro_Ajustado = 'Pirituba / Jaraguá' THEN 'PIRITUBA'
	        WHEN Bairro_Ajustado = 'Aricanduva / Vl. Formosa' THEN 'VILA FORMOSA'
	        WHEN Bairro_Ajustado = 'Guaianazes' THEN 'GUAIANAZES'
	        WHEN Bairro_Ajustado = 'Itaquera' THEN 'ITAQUERA'
	        WHEN Bairro_Ajustado = 'Itaim Paulista' THEN 'ITAIM'
	        WHEN Bairro_Ajustado = 'Móoca' THEN 'MOOCA'
	        WHEN Bairro_Ajustado = 'São Mateus' THEN 'MATEUS'
	        WHEN Bairro_Ajustado = 'Cidade Tiradentes' THEN 'TIRADENTES'
	        WHEN Bairro_Ajustado = 'Butantã' THEN 'BUTANTÃ'
	        WHEN Bairro_Ajustado = 'Lapa' THEN 'LAPA'
	        WHEN Bairro_Ajustado = 'Pinheiros' THEN 'PINHEIROS'
	        WHEN Bairro_Ajustado = 'Santo Amaro' THEN 'SANTO AMARO'
	        WHEN Bairro_Ajustado = 'Vila Mariana' THEN 'VILA MARIANA'
	        WHEN Bairro_Ajustado = 'Parelheiros' THEN 'PARELHEIROS' 
	        ELSE Bairro_Ajustado
		END as bairro,
	        dia,
	        pluviometria,
	        data
	FROM AdjustedBairros)
select * from final
where bairro in (
            'CASA VERDE', 'JAÇANÃ', 'VILA GUILHERME', 'PIRITUBA', 'VILA FORMOSA',
            'GUAIANAZES', 'ITAQUERA', 'ITAIM', 'MOOCA', 'MATEUS', 'TIRADENTES',
            'BUTANTÃ', 'LAPA', 'PINHEIROS', 'SANTO AMARO', 'VILA MARIANA', 'PARELHEIROS'
        )